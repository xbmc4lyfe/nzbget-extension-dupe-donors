"""Article availability of NZBs on every news server nzbget knows, checked in parallel.

Each sampled article is asked of all servers at once: STAT, and for BODY_PERCENT of the articles also BODY
with yEnc validation, so "the server says it has it" is backed by real data now and then (servers take turns
on a body, so each is downloaded once unless it comes back bad). The first real hit marks the article found
and every server skips it from then on; an article is missing only after every server said no (a server in
its error pause abstains). Each server walks the article list at its own pace, so a slow server never holds
up the fast ones (a request already in flight for an article that was found meanwhile is left to finish
and its answer ignored: cancelling it would mean dropping and re-opening the connection). NZBs are checked
NZBS_TO_CHECK_CONCURRENTLY at a time, each with NNTP_SERVER_CONNECTION_PER_NZB connections per server, all
within MAX_CONNS_PER_NNTP_SERVER (and the server's own nzbget Connections). The NNTP client is vendored
Appz4Fun/cyclops.
"""
import asyncio
import math
import os
import queue
import random
import re
import sys
import threading
from dataclasses import dataclass

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor"))
from cyclops.verify_nzb import (AsyncNntpConnection, ServerConfig, TransientNntpError,  # noqa: E402
                                normalize_message_id, validate_yenc_body)

MIN_KNOWN = 5               # answered articles needed before judging an NZB
SERVER_GIVE_UP = 3          # consecutive errors after which a server pauses ...
SERVER_RETRY_AFTER = 30.0   # ... for this many seconds before it is asked again
PIPELINE = 16               # most STATs sent per round trip on one connection (RFC 3977 3.5 pipelining);
PIPELINE_START = 4          # a server starts at this many, doubling while a batch takes < 1 s, halving > 2 s


class Server(ServerConfig):
    def __repr__(self):  # never print credentials
        return "Server(%s:%d ssl=%s)" % (self.host, self.port, self.ssl)


def servers_from_nzbget_config(entries, max_conns=20, timeout=15.0):
    """Active ServerN.* entries of nzbget's `config` JSON-RPC result -> server list, each allowed at most
    `max_conns` connections and at most half of its own nzbget Connections (nzbget keeps the rest; several
    nzbget servers can share one provider account)."""
    opts = {e["Name"]: str(e.get("Value", "")) for e in entries}
    numbers = sorted({int(m.group(1)) for m in (re.match(r"Server(\d+)\.Host$", k) for k in opts) if m})
    out = []
    for n in numbers:
        get = lambda key, default="": opts.get("Server%d.%s" % (n, key), default)  # noqa: E731
        if get("Active", "yes").lower() != "yes" or not get("Host"):
            continue
        user = get("Username") or None
        out.append(Server(name="server%d" % n, host=get("Host"), port=int(get("Port") or 119),
                          ssl=get("Encryption").lower() == "yes", username=user,
                          password=get("Password") if user else None,
                          max_connections=max(1, min(max_conns, int(get("Connections") or 2 * max_conns) // 2)),
                          timeout=timeout))
    return out


@dataclass
class Limits:
    nzbs: int = 10     # NZBS_TO_CHECK_CONCURRENTLY
    per_nzb: int = 1   # NNTP_SERVER_CONNECTION_PER_NZB


@dataclass
class Health:
    checked: int
    present: int
    missing: int
    error: int
    body_checked: int = 0  # articles that also got a BODY check
    body_bad: int = 0      # ... for which no server delivered valid yEnc data

    @property
    def answered(self):
        return self.present + self.missing + self.error

    @property
    def alive(self):
        """Present share of all answered articles; None if too few answers (budget) to judge.

        Errors count as not present: a live article still gets a hit from some other server, while a
        server that errors (e.g. a transient 451) would otherwise hide every dead article."""
        return self.present / self.answered if self.answered >= MIN_KNOWN else None

    def __add__(self, other):
        return Health(*(a + b for a, b in zip(vars(self).values(), vars(other).values())))


def dead_probe(h):
    """A probe proves an NZB dead only with nothing found and enough definite misses (not mere errors)."""
    return h.present == 0 and h.missing >= MIN_KNOWN


def sample(ids, percent, minimum=20, maximum=300, seed=0):
    """`percent` of the ids, at least `minimum`, at most `maximum` (deterministic)."""
    ids = sorted(ids)
    k = min(len(ids), max(minimum, min(maximum, math.ceil(len(ids) * percent / 100))))
    return random.Random(seed).sample(ids, k)


def plan(ids, percent, body_percent=20, minimum=20, maximum=300, seed=0, max_body=5):
    """Sampled (message-id, also_body) pairs: about `body_percent`% of them, at most `max_body`, also get a
    BODY check (every server downloads a body article, so bodies are kept to a few per NZB)."""
    rnd, bodies, out = random.Random(seed + 1), 0, []
    for m in sample(ids, percent, minimum, maximum, seed):
        body = bodies < max_body and rnd.random() * 100 < body_percent
        bodies += body
        out.append((m, body))
    return out


async def _quit(conn):
    """Close politely: QUIT, its 205, then wait (briefly) until the server closes its end, so the session is
    really gone on the server before this connection's slot is reused."""
    if conn._writer is not None:
        try:
            await asyncio.wait_for(conn._send_command("QUIT"), 5)
            await asyncio.wait_for(conn._reader.read(), 2)  # EOF once the server has closed the session
        except Exception:
            pass
    await conn.close()


def _drop(conn):
    """Close a connection whose request was cancelled mid-flight (its protocol state is unknown)."""
    writer, conn._writer, conn._reader = conn._writer, None, None
    if writer is not None:
        writer.close()


class _ServerSlots:
    """Open-connection budget for one server, shared by every run in this process (concurrent grabs)."""

    def __init__(self, cap):
        self.cap, self.used, self.waiting, self.lock = cap, 0, 0, threading.Lock()

    def try_take(self):
        with self.lock:
            if self.used < self.cap:
                self.used += 1
                return True
            return False

    def give_back(self):
        with self.lock:
            self.used -= 1


_SLOTS, _SLOTS_LOCK = {}, threading.Lock()  # (host, port, user) -> _ServerSlots


def _slots(server):
    with _SLOTS_LOCK:
        key = (server.host, server.port, server.username)
        if key not in _SLOTS:
            _SLOTS[key] = _ServerSlots(server.max_connections)
        return _SLOTS[key]


class _Pool:
    """Connections to one server for one run. Every open connection holds one of the server's slots, so all
    runs together never exceed its cap; an idle connection is closed when another run waits for a slot."""

    def __init__(self, server):
        self.server, self.idle, self.errors, self.down_until = server, [], 0, 0.0
        self.slots, self.inflight, self.waiting, self.depth = _slots(server), set(), 0, PIPELINE_START

    async def _get(self):
        while True:
            if self.idle:
                return self.idle.pop()
            if self.slots.try_take():
                return AsyncNntpConnection(self.server)
            self.slots.waiting += 1
            self.waiting += 1
            try:
                await asyncio.sleep(0.005)
            finally:
                self.slots.waiting -= 1
                self.waiting -= 1

    async def _put(self, conn):
        if self.slots.waiting > self.waiting:  # another run waits for this server: hand the slot over
            await _quit(conn)
            self.slots.give_back()
        else:                                  # keep it (our own waiters pick it up from idle)
            self.idle.append(conn)

    def paused(self):
        return self.down_until > asyncio.get_running_loop().time()

    async def ask(self, batch):
        """This server's answers for [(message-id, False or _BodyGate)]: 'present', 'missing', 'bodybad' or
        'error' each."""
        wait = self.down_until - asyncio.get_running_loop().time()
        if wait > 0:  # a server that kept failing gets a pause, then another try (found articles don't wait)
            await asyncio.sleep(wait)
        conn = await self._get()
        # Shielded: if the article gets settled (or the NZB ends) meanwhile, the request still finishes and
        # its connection goes back to the pool, so no half-used connection is ever torn down and re-opened.
        task = asyncio.ensure_future(self._serve(conn, batch))
        self.inflight.add(task)
        task.add_done_callback(self.inflight.discard)
        return await asyncio.shield(task)

    async def _serve(self, conn, batch):
        t0 = asyncio.get_running_loop().time()
        try:
            answers = await self._ask_batch(conn, batch)
            took = asyncio.get_running_loop().time() - t0  # latency-bound servers gain from deeper batches,
            if took < 1.0:                                  # busy ones only hold a batch past the budget
                self.depth = min(PIPELINE, self.depth * 2)
            elif took > 2.0:
                self.depth = max(1, self.depth // 2)
        except asyncio.CancelledError:  # only at interpreter/loop shutdown
            _drop(conn)
            self.slots.give_back()
            raise
        except Exception as exc:
            self.errors += 1
            if self.errors >= SERVER_GIVE_UP:
                self.down_until = asyncio.get_running_loop().time() + SERVER_RETRY_AFTER
            await conn.close()
            answers = getattr(exc, "answers", None) or ["error"] * len(batch)  # STATs answered before it broke
        else:
            self.errors = 0
        if self.errors >= SERVER_GIVE_UP:
            self.errors = 0
        await self._put(conn)
        return answers

    @staticmethod
    async def _ask_batch(conn, batch):
        """All STATs of the batch in one write, then their replies in order; then BODY for each article that
        has a _BodyGate (one server at a time downloads it). Any reply line is an answer on a healthy
        connection: 430 = missing; another code (some providers say 451 for a missing article) = 'error'
        without dropping the connection. Only a failed connection or a garbled reply raises (cyclops' own
        stat()/body() would close the connection on a 451)."""
        await conn._connect_once()
        try:
            conn._writer.write("".join("STAT %s\r\n" % normalize_message_id(m) for m, _ in batch).encode("ascii"))
            await asyncio.wait_for(conn._writer.drain(), conn.config.timeout)
        except asyncio.TimeoutError as exc:
            raise TransientNntpError("command timeout") from exc
        codes = [(await conn._read_response())[0] for _ in batch]
        out = ["present" if code == 223 else "missing" if code == 430 else "error" for code in codes]
        for j, ((mid, gate), code) in enumerate(zip(batch, codes)):
            if gate and code == 223:
                try:
                    out[j] = await _Pool._body(conn, mid, gate)
                except Exception as exc:  # the connection broke mid-body: keep the STAT answers already in
                    rest = zip(batch[j + 1:], codes[j + 1:], out[j + 1:])
                    exc.answers = out[:j] + ["error"] + ["error" if g and c == 223 else a for (_, g), c, a in rest]
                    raise
        return out

    @staticmethod
    async def _body(conn, mid, body):
        async with body.lock:
            if body.done():  # another server delivered valid data meanwhile
                return "present"
            code, _ = await conn._send_command("BODY %s" % normalize_message_id(mid))
            if code != 222:
                return "bodybad"
            body.ok = validate_yenc_body(await conn._read_multiline()).ok
            return "present" if body.ok else "bodybad"

    async def close(self):
        await asyncio.gather(*self.inflight, return_exceptions=True)  # let requests in flight finish first
        for conn in self.idle:
            await _quit(conn)
            self.slots.give_back()
        self.idle = []


class _BodyGate:
    """One article's BODY check: servers take turns (a body is ~750 KB), and stop once one delivered."""

    def __init__(self, settled):
        self.lock, self.ok, self.settled = asyncio.Lock(), False, settled

    def done(self):
        return self.ok or self.settled()


async def _check_nzb(pools, items, per_nzb, deadline):
    """Check one NZB's sampled articles on every server at once -> Health."""
    n, loop = len(items), asyncio.get_running_loop()
    final, votes, soft = [None] * n, [dict() for _ in range(n)], [False] * n
    gates = {i: _BodyGate(lambda i=i: final[i] is not None) for i, (_, body) in enumerate(items) if body}
    done, left = asyncio.Event(), [n]

    def settle(i, verdict):
        if final[i] is None:
            final[i] = verdict
            left[0] -= 1
            if not left[0]:
                done.set()

    async def walk(p, pool, cursor):  # one server's pass over the articles, skipping ones already found
        while cursor[0] < n:
            todo = []
            while cursor[0] < n and len(todo) < pool.depth:  # the next articles nobody has found yet
                if final[cursor[0]] is None:
                    todo.append(cursor[0])
                cursor[0] += 1
            if not todo:
                continue
            if pool.paused() and any(not q.paused() for q in pools if q is not pool):
                answers = ["error"] * len(todo)  # a server in its pause abstains while others can answer
            else:
                answers = await pool.ask([(items[i][0], gates.get(i, False)) for i in todo])
            for i, answer in zip(todo, answers):
                votes[i][p] = answer
                soft[i] = soft[i] or answer == "bodybad"
                if answer == "present":
                    settle(i, "present")
                elif len(votes[i]) == len(pools):  # nobody had it: missing if any server said so definitively
                    settle(i, "error" if set(votes[i].values()) == {"error"} else "missing")

    walkers = [asyncio.ensure_future(walk(p, pool, cursor))
               for p, pool in enumerate(pools) for cursor in [[0]] for _ in range(per_nzb)]
    if n:
        try:
            await asyncio.wait_for(done.wait(), max(0.01, deadline - loop.time()))
        except asyncio.TimeoutError:
            pass
    for w in walkers:  # requests still in flight are for articles already settled (or the budget ran out)
        w.cancel()
    await asyncio.gather(*walkers, return_exceptions=True)
    for i in range(n):  # budget over: missing where most servers said so (present ones settle at once, so
        misses = sum(v == "missing" for v in votes[i].values())  # leaving these out would inflate alive)
        if final[i] is None and "present" not in votes[i].values() and 2 * misses >= len(pools):
            final[i] = "missing"
    return Health(n, final.count("present"), final.count("missing"), final.count("error"),
                  sum(body for _, body in items), sum(soft[i] and final[i] != "present" for i in range(n)))


async def _run(servers, groups, percent, probe, budget, full, limits, body_percent, max_body, minimum, maximum,
               emit):
    loop = asyncio.get_running_loop()
    pools, gate = [_Pool(s) for s in servers], asyncio.Semaphore(limits.nzbs)

    async def one(key, ids):
        items = plan(ids, percent, body_percent, minimum, maximum, max_body=max_body)
        first = items[:probe] if len(items) >= probe else plan(ids, 0, body_percent, probe, probe, max_body=max_body)
        async with gate:
            deadline = loop.time() + budget  # each NZB's own budget, counted from when its check starts
            h = await _check_nzb(pools, first, limits.per_nzb, deadline)
            if full and not dead_probe(h):  # nothing found and enough definite misses: done
                probed = {m for m, _ in first}
                h += await _check_nzb(pools, [it for it in items if it[0] not in probed], limits.per_nzb,
                                      deadline)
        emit(key, h)

    try:
        await asyncio.gather(*(one(k, ids) for k, ids in groups.items()))
    finally:
        for pool in pools:
            await pool.close()


def check_iter(servers, groups, percent=2.0, probe=10, budget=120.0, full=True, limits=None, body_percent=20,
               max_body=5, minimum=20, maximum=300):
    """{key: message ids} -> yields (key, Health) as each NZB finishes. Every NZB first gets `probe`
    articles; one with none found anywhere is dead and skips its full `percent` sample (`full=False`:
    probe only). The sample is `percent` of its articles, at least `minimum`, at most `maximum`. Articles of an
    NZB still unanswered `budget` s after its check started are unknown."""
    results, end = queue.Queue(), object()

    def run():
        try:
            asyncio.run(_run(servers, groups, percent, probe, budget, full, limits or Limits(), body_percent,
                             max_body, minimum, maximum, lambda k, h: results.put((k, h))))
        finally:
            results.put(end)

    threading.Thread(target=run, daemon=True).start()
    while True:
        item = results.get()
        if item is end:
            return
        yield item


def check_many(servers, groups, percent=2.0, probe=10, budget=120.0, full=True, limits=None, body_percent=20,
               max_body=5, minimum=20, maximum=300):
    """{key: message ids} -> {key: Health} (see check_iter)."""
    return dict(check_iter(servers, groups, percent, probe, budget, full, limits, body_percent, max_body, minimum,
                           maximum))


def availability(servers, message_ids, percent=2.0):
    """Health of one NZB's articles (see check_iter)."""
    return check_many(servers, {0: message_ids}, percent)[0]
