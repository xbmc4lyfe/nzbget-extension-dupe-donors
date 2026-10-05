#!/usr/bin/env python3
"""nzbget-dupe-proxy: impersonates nzbget toward NZBHydra2 and adds duplicate postings as donors.

Requests are forwarded verbatim to nzbget; JSON-RPC `append` gets a DupeKey, then a background worker finds other
postings of the same release via Hydra and appends them under that DupeKey with lower DupeScores, as donors for
nzbget's DupeArticleFallback. "Same release" is decided by PTT-parsed release names (title, episode, group,
resolution, source, codec, audio, HDR, ...), not by size. Python 3 stdlib + vendored PTT (pure Python)."""
import base64
import functools
import glob
import hashlib
import heapq
import itertools
import json
import logging
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from collections import Counter, namedtuple
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from dataclasses import dataclass, fields
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from xml.etree import ElementTree as ET
from xml.parsers import expat

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "vendor"))
from ptt import parse_title  # noqa: E402  (vendored PTT, MIT)
import donor_health  # noqa: E402  (sampled STAT on all news servers, vendored cyclops)

log = logging.getLogger("nzbget-dupe-proxy")
GROUP_WINDOW = 600        # s: appends of the same release within this window share one DupeKey
STATE_TTL = 30 * 86400   # s: forget groups after this
DEAD_TTL = 3 * 86400     # s: remember postings found dead (taken-down articles do not come back)
FETCH_RETRY_DELAY = 2.0  # s before re-fetching an NZB after an indexer error / non-NZB reply
SEARCH_PAGE, SEARCH_PAGES = 100, 5  # Hydra results per request, and pages read when a query fills them
WATCH_STATUSES = ("QUEUED", "PAUSED", "DOWNLOADING", "FETCHING")  # not yet past download
RELIST_WINDOW = 120.0     # s: listings of one size posted this close together are one posting on several indexers
INDEXER_COOLDOWN = 1800.0  # s an indexer is not asked for NZBs after it refused one (403/429: its grab limit)
Result = namedtuple("Result", "title link size grabs date indexer")  # one Hydra search hit
EXT_RE = re.compile(r"(\.(part\d+\.rar|vol\d+\+\d+\.par2|7z\.\d{3}|r\d{2}|z\d{2}|nzb|mkv|mp4|m4v|avi|ts|"
                    r"rar|par2|7z|zip|nfo|sfv|srr|srt|sub|idx|jpg|png|txt)|(?<![hx])\.\d{3})$", re.I)  # not H.264
JUNK_RE = re.compile(r"([.\-_ ](xpost|postbot|obfuscated|scrambled|asrequested|rp|rakuv\w*|buymore|"
                     r"chamele0n|sample|repost))+$", re.I)
MARKER_RE = re.compile(r"^(s\d{1,3}e\d{1,4}|(19|20)\d\d)$")      # episode / year: must match
RES_RE = re.compile(r"^(\d{3,4}p|4k|uhd)$")


@dataclass
class Config:
    listen_port: int = 6790
    nzbget_url: str = "http://127.0.0.1:6789"
    hydra_url: str = ""
    hydra_apikey: str = ""
    max_donors: int = 0  # <= 0 (0 or -1): unlimited
    size_tolerance: float = 0.0  # > 0: skip Hydra results whose size differs more than this fraction
    state_dir: str = "/var/lib/nzbget-dupe-proxy"
    enabled: bool = True
    dry_run: bool = False
    health_percent: float = 5.0   # STAT this % of each NZB's articles on all news servers (0 = off) ...
    health_min_articles: int = 50   # ... but at least this many articles
    health_max_articles: int = 1000  # ... and at most this many
    donor_min_alive: float = 0.5  # drop donors whose sampled articles are alive on no server below this share
    nzbs_to_check_concurrently: int = 10      # NZBs whose articles are checked at the same time
    nntp_server_connection_per_nzb: int = 1   # connections per news server for each NZB being checked
    max_conns_per_nntp_server: int = 20       # hard cap per server (also <= the server's nzbget Connections)
    body_percent: float = 20.0                # share of sampled articles that also get BODY + yEnc check ...
    body_max_per_nzb: int = 20                # ... but at most this many per NZB (one server downloads each)
    health_budget: float = 120.0  # s per health pass (probe of all donors / full samples of the rest)
    fast_donors: int = 5          # donors appended right after the quick probe; the rest after a full sample
    primary_score: int = 1000000  # DupeScore of a primary the proxy manages; donors sit just under it (nzbget's
    #   failover takes a backup only if backup score >= primary score * health / 1000)
    watch_nzbget: bool = False    # also watch nzbget's queue for picks added without the proxy (nzbdavkodi, uploads)
    watch_interval: float = 15.0  # s between queue polls
    watch_settle: float = 20.0    # s a pick waits in the queue first (its submitter's own backups arrive)
    nzbget_username: str = ""     # nzbget login for the watcher (the Hydra path uses Hydra's own)
    nzbget_password: str = ""
    deadline: float = 60.0  # seconds after the primary append
    timeout: float = 30.0   # per HTTP request to Hydra / indexers

    @classmethod
    def from_env(cls, env):
        """Each field from the env var of the same name in upper case (LISTEN_PORT, ...)."""
        c = cls()
        for f in fields(cls):
            v, conv = str(env.get(f.name.upper(), "")).strip(), type(getattr(c, f.name))
            if v:
                setattr(c, f.name, v.lower() in ("1", "true", "yes", "on") if conv is bool else conv(v))
        c.nzbget_url, c.hydra_url = c.nzbget_url.rstrip("/"), c.hydra_url.rstrip("/")
        return c

    @property
    def donor_cap(self):
        """MAX_DONORS as a number, or infinity when MAX_DONORS <= 0."""
        return self.max_donors if self.max_donors > 0 else float("inf")


def clean_name(name):
    """Release name without extensions / volume suffixes, [tags] and indexer junk ('-xpost')."""
    name, prev = name.strip(), None
    while name != prev:
        prev, name = name, EXT_RE.sub("", name)
    return JUNK_RE.sub("", re.sub(r"\[[^\]]*\]|\{[^}]*\}", " ", name).strip()).strip(" ._-")


def normalize_title(name):
    """Lowercase clean name, separators -> single dots."""
    return re.sub(r"[\s._\-()+,]+", ".", clean_name(name).lower()).strip(".")


COMPAT = ("resolution", "quality", "codec", "bit_depth", "audio", "channels", "network", "edition")
EXACT = ("hdr",)  # a missing HDR tag means SDR, so HDR formats must match exactly


def _tokens(v):
    v = " ".join(map(str, v)) if isinstance(v, list) else str(v or "")
    return frozenset(re.findall(r"[a-z0-9]+", v.lower()))


@functools.lru_cache(maxsize=4096)
def release_attrs(name):
    """PTT attributes of a release/file name, normalized for comparison (lowercase token sets)."""
    try:
        a = parse_title(clean_name(name))
    except Exception:  # PTT on garbage input: treat as unreadable
        a = {}
    out = {k: _tokens(a.get(k)) for k in COMPAT + EXACT}
    out.update(title=re.sub(r"[^a-z0-9]", "", str(a.get("title", "")).lower()), group=str(a.get("group") or "").lower(),
               seasons=tuple(a.get("seasons") or ()), episodes=tuple(a.get("episodes") or ()), year=a.get("year"),
               repack=bool(a.get("repack")), proper=bool(a.get("proper")))
    return out


def readable(name):
    """Does a (file) name carry release info, i.e. is it not obfuscated?"""
    a = release_attrs(name)
    return bool(a["title"] and (a["group"] or a["resolution"]))


def same_release(a_name, b_name):
    """Same title/episode/year, same group, same repack/proper, and no conflicting quality attributes."""
    a, b = release_attrs(a_name), release_attrs(b_name)
    if not a["group"]:  # nothing to anchor on: require the same normalized name
        return normalize_title(a_name) == normalize_title(b_name)
    strict = ("title", "group", "seasons", "episodes", "repack", "proper") + EXACT
    if any(a[k] != b[k] for k in strict):
        return False
    if a["year"] and b["year"] and a["year"] != b["year"]:
        return False
    return all(not a[k] or not b[k] or a[k] <= b[k] or b[k] <= a[k] for k in COMPAT)


def short_query(title):
    """'Lucifer.S02E14.Candy...1080p...' -> 'lucifer s02e14 1080p' (None without episode/year)."""
    toks = normalize_title(title).split(".")
    cut = next((i for i, t in enumerate(toks) if MARKER_RE.match(t)), None)
    if cut is None:
        return None
    res = next((t for t in toks[cut + 1:] if RES_RE.match(t)), None)
    return " ".join(toks[:cut + 1] + ([res] if res else []))


@dataclass(frozen=True)
class NzbInfo:
    files: int
    total_bytes: int
    filenames: frozenset
    poster: str
    message_ids: frozenset
    meta: dict
    main_name: str = ""  # name of the largest file

    @property
    def fingerprint(self):
        return hashlib.sha1("\n".join(sorted(self.message_ids)).encode()).hexdigest()


def safe_xml(data):
    """ElementTree.fromstring that refuses entity declarations (billion laughs / XXE); ValueError."""
    def no_entities(*_):
        raise ValueError("XML declares entities")
    tb, p = ET.TreeBuilder(), expat.ParserCreate(namespace_separator="}")
    p.StartElementHandler, p.EndElementHandler, p.CharacterDataHandler = tb.start, tb.end, tb.data
    p.EntityDeclHandler = no_entities
    try:
        p.Parse(data, True)
        return tb.close()
    except (expat.ExpatError, AssertionError) as e:
        raise ValueError("malformed XML: %s" % e)


def parse_nzb(data):
    """Parse NZB bytes into an NzbInfo; ValueError if malformed or empty."""
    files, sizes, ids, poster, meta = 0, {}, set(), "", {}
    for el in safe_xml(data).iter():
        tag = el.tag.rsplit("}", 1)[-1]
        if tag == "meta" and el.get("type"):
            meta[el.get("type").lower()] = (el.text or "").strip()
        elif tag == "file":
            files += 1
            poster = poster or el.get("poster", "")
            m = re.search(r'"([^"]+)"', el.get("subject", ""))
            name = (m.group(1) if m else el.get("subject", "")).strip()
            for seg in el.iter():
                if seg.tag.rsplit("}", 1)[-1] == "segment":
                    sizes[name] = sizes.get(name, 0) + int(seg.get("bytes") or 0)
                    ids.add((seg.text or "").strip())
    if not files or not ids:
        raise ValueError("NZB has no files/segments")
    data_files = {n: b for n, b in sizes.items() if not re.search(r"\.par2$|\.vol\d+[+-]\d+", n, re.I)} or sizes
    return NzbInfo(files, sum(sizes.values()), frozenset(n.lower() for n in sizes), poster, frozenset(ids), meta,
                   max(data_files, key=data_files.get))


def same_posting(a, b):
    """Any real article-ID overlap (> 1%) -> same posting: donors must point at different articles.
    (Indexers re-list one posting with a re-uploaded segment; distinct postings share 0%.)"""
    return len(a & b) > 0.01 * min(len(a), len(b))


SKETCH_K = 64  # message-ID hashes kept per posting sketch


def sketch(ids, k=SKETCH_K):
    """Bottom-k sketch of a posting: the k smallest CRC32s of its message-IDs. Postings that share most of
    their articles (one posting re-listed with a re-uploaded segment) share most of their sketch; distinct
    postings share about none. Compact enough to keep for every kept or dead NZB."""
    return tuple(heapq.nsmallest(k, {zlib.crc32(i.encode()) for i in ids}))


def same_sketch(a, b):
    k = min(len(a), len(b))
    return bool(k) and len(set(a) & set(b)) >= max(1, k // 8)


_KEPT = {}  # (path, mtime, size) -> sketch of an NZB file nzbget keeps


def kept_sketch(path):
    st = os.stat(path)
    key = (path, st.st_mtime, st.st_size)
    if key not in _KEPT:
        if len(_KEPT) > 4096:
            _KEPT.clear()
        with open(path, "rb") as f:
            _KEPT[key] = sketch(parse_nzb(f.read()).message_ids)
    return _KEPT[key]


def candidate_ok(primary_title, primary_bytes, r, tol):
    """Hydra result worth fetching: same release by name; size only matters if a tolerance is set."""
    if tol and primary_bytes and abs(r.size - primary_bytes) > tol * primary_bytes:
        return False
    return same_release(primary_title, r.title)


def group_listings(results):
    """Hydra results -> lists of listings of one posting: same size, posted within RELIST_WINDOW (indexers list
    a posting with its own usenetdate, a few seconds apart). A repost of the same files has the same size but
    another posting time. Listings without a size or date stay alone."""
    groups, last = [], None
    for r in sorted(results, key=lambda r: (r.size, r.date)):
        if last is not None and r.size and r.date and last[0].date and r.size == last[0].size and \
                r.date - last[0].date <= RELIST_WINDOW:
            last.append(r)
        else:
            last = [r]
            groups.append(last)
    return groups


def listing_mismatch(r, info):
    """The indexer served an NZB more than 2% off the size it lists (often another indexer's NZB)."""
    return bool(r.size) and abs(info.total_bytes - r.size) > 0.02 * r.size


def mask(text):
    """Hide apikeys and user:password pairs in URLs/paths before logging."""
    text = re.sub(r"(?i)(apikey=)[^&\s]+", r"\1***", str(text))
    text = re.sub(r"//[^/@\s]+:[^/@\s]+@", "//***@", text)
    return re.sub(r"/[^/:\s]+:[^/\s]+/(json|xml)rpc", r"/***/\1rpc", text)


class State:
    """JSON file {key: {t, title, fps: {message-id fingerprint: nzbid}},
    "_dead": {t, fps: {fingerprint: {t, s: sketch}}}}."""

    def __init__(self, state_dir):
        self.path = os.path.join(state_dir, "state.json")
        self.lock = threading.RLock()
        try:
            with open(self.path) as f:
                self.data = json.load(f)
        except (OSError, ValueError):
            self.data = {}

    def save(self):
        now = time.time()
        self.data = {k: v for k, v in self.data.items() if now - v["t"] < STATE_TTL}
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path + ".tmp", "w") as f:
                json.dump(self.data, f)
            os.replace(self.path + ".tmp", self.path)
        except OSError as e:
            log.error("cannot write state %s: %s", self.path, e)

    def group_for(self, title, now):
        """Key of a recent group whose primary is the same release as `title`."""
        for key, g in sorted(self.data.items(), key=lambda kv: -kv[1]["t"]):
            if now - g["t"] < GROUP_WINDOW and g.get("title") and same_release(g["title"], title):
                return key
        return None

    def sent(self, key, fp):
        return self.data.get(key, {}).get("fps", {}).get(fp)

    def nzbids_for(self, fp):
        """NZBIDs this posting was recorded under (as a primary or a donor), in any group."""
        return {g["fps"][fp] for k, g in self.data.items()
                if k != "_dead" and isinstance(g, dict) and fp in g.get("fps", {})}

    def mark_dead(self, fp, sk):
        with self.lock:
            dead = self.data.setdefault("_dead", {"t": time.time(), "fps": {}})
            dead["t"] = time.time()
            dead["fps"] = {f: e for f, e in dead["fps"].items()
                           if isinstance(e, dict) and time.time() - e["t"] < DEAD_TTL}
            dead["fps"][fp] = {"t": time.time(), "s": list(sk)}
            self.save()

    def is_dead(self, sk):
        """Was this posting (or a near-identical re-listing of it) found dead recently?"""
        entries = self.data.get("_dead", {}).get("fps", {}).values()
        return any(isinstance(e, dict) and time.time() - e["t"] < DEAD_TTL and same_sketch(sk, e["s"])
                   for e in entries)

    def record(self, key, fp, nzbid, title=None, touch=False):
        g = self.data.setdefault(key, {"t": time.time(), "title": title, "fps": {}})
        if touch:
            g.update(t=time.time(), title=title)
        g["fps"][fp] = nzbid
        self.save()


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def score_base(primary):
    """Offset of every DupeScore sent under a primary scored `primary`: donors base+2..base+90, dead base+1."""
    return max(0, primary - 1000)


def target_score(alive, twin=False):
    """DupeScore for a donor: the most whole first. nzbget tries dupes by highest DupeScore, so 9 + 80 * alive
    share (unknown = 1.0): 100% alive = 89, 50% = 49; a byte-identical twin of the primary (same files and bytes:
    article borrowing and whole-file recreation work best with it) gets +1, so it wins a tie. Always below the
    primary's 100; 1 is reserved for dead postings."""
    return min(90, 9 + round(80 * (1.0 if alive is None else alive)) + bool(twin))


class Ranks:
    """Unique donor DupeScores (target_score; equal values count down: 90, 89, ...)."""

    def __init__(self):
        self.used = set()

    def take(self, alive, twin=False):
        score = target_score(alive, twin)
        while score in self.used:
            score -= 1
        self.used.add(score)
        return score

    def release(self, score):
        self.used.discard(score)


@dataclass
class Placed:
    """A donor appended to nzbget, as last scored."""
    id: int
    score: int
    alive: object  # share, or None if unknown
    twin: bool
    v: tuple       # (Result, nzb bytes, NzbInfo)


def alive_of(health, v):
    h = health.get(v[2].fingerprint)
    return h.alive if h else None


def pct(share):
    return "?" if share is None else "%d%%" % round(100 * share)


def rpc_result(status, body):
    """Integer `result` of an nzbget JSON-RPC reply (NZBID for append), 0 on any error."""
    try:
        return _int(json.loads(body).get("result")) if status == 200 else 0
    except (ValueError, AttributeError):
        return 0


class Handler(BaseHTTPRequestHandler):
    proxy = None  # set by Proxy.start
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        log.debug(mask(fmt % args))

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        intercept = self.proxy.cfg.enabled and self.command == "POST" and self.path.endswith("/jsonrpc")
        reply = self.proxy.handle_append(self.path, body, self.headers) if intercept else None
        status, body, ctype = reply or self.proxy.forward(self.path, body, self.headers, self.command)
        self.send_response(status)
        self.send_header("Content-Type", ctype or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST


class Proxy:
    def __init__(self, cfg):
        self.cfg, self.state = cfg, State(cfg.state_dir)
        self.workers, self.server, self.workers_lock = [], None, threading.Lock()
        self.indexer_locks = {}  # indexer -> one fetch at a time
        self.refused = dict(self.state.data.get("_refused", {}).get("until", {}))  # indexer -> refused until
        self.ctx = threading.local()             # per discovery: .base, added to every DupeScore sent
        self.watched, self.first_seen = set(), {}  # watcher: NZBIDs handled; NZBID -> first seen in the queue

    def _indexer_lock(self, indexer):
        with self.workers_lock:
            return self.indexer_locks.setdefault(indexer, threading.Lock())

    def forward(self, path, body, headers, method="POST"):
        """Send a request to nzbget unchanged; returns (status, body, content-type)."""
        h = {k: headers[k] for k in ("Authorization", "Content-Type", "User-Agent") if headers.get(k)}
        req = urllib.request.Request(self.cfg.nzbget_url + path, data=body if method == "POST" else None,
                                     headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return r.status, r.read(), r.headers.get("Content-Type")
        except urllib.error.HTTPError as e:
            return e.code, e.read(), e.headers.get("Content-Type")
        except OSError as e:
            log.error("nzbget unreachable for %s: %s", mask(path), e)
            return 502, b"nzbget unreachable", "text/plain"

    def handle_append(self, path, body, headers):
        """Rewrite + forward a positional JSON-RPC append; None means 'not ours, pass through'."""
        try:
            req = json.loads(body)
            params = req["params"]
            if req.get("method") != "append" or not isinstance(params, list) or len(params) != 10:
                return None
        except (ValueError, TypeError, KeyError, AttributeError):
            return None
        t0, title, info = time.time(), re.sub(r"(?i)\.nzb$", "", str(params[0])), None
        try:
            info = parse_nzb(base64.b64decode(params[1], validate=True))
        except (ValueError, TypeError) as e:
            log.info("append %s: content is not an NZB (%s); no donor discovery", title, e)
        with self.state.lock:
            key = params[6] or self.state.group_for(title, t0) or "dupes:" + normalize_title(title)
            g = self.state.data.get(key)
            fresh = bool(g) and t0 - g["t"] < GROUP_WINDOW
            old = info and fresh and self.state.sent(key, info.fingerprint)
            if old:
                log.info("append key=%s nzbid=%s title=%s: posting already sent, not re-adding", key, old, title)
                reply = {"version": "1.1", "id": req.get("id"), "result": old}
                return 200, json.dumps(reply).encode(), "application/json"
            params[6:9] = [key, self.cfg.primary_score, "SCORE"]
            status, rbody, ctype = self.forward(path, json.dumps(req).encode(), headers)
            nzbid = rpc_result(status, rbody)
            if info and nzbid > 0:
                self.state.record(key, info.fingerprint, nzbid, title, touch=not fresh)
        if nzbid <= 0 or not info or fresh or not self.cfg.hydra_url:
            why = "joined existing group" if fresh else "no discovery"
            log.info("append key=%s nzbid=%s title=%s: %s", key, nzbid, title, why)
        else:
            t = threading.Thread(target=self._discover_safe, daemon=True,
                                 args=(key, title, info, params[2], path, headers.get("Authorization"), nzbid, t0,
                                       score_base(self.cfg.primary_score)))
            with self.workers_lock:
                self.workers = [w for w in self.workers if w.is_alive()] + [t]
            t.start()
        return status, rbody, ctype

    def _discover_safe(self, *args):
        try:
            self.discover(*args)
        except Exception:  # never let a donor problem escape the worker
            log.exception("donor discovery crashed for key=%s", args[0])

    def hydra_search(self, params):
        """All results of one newznab query, reading further pages while a page comes back full."""
        out = []
        for page in range(SEARCH_PAGES):
            got = self._hydra_page(dict(params, limit=SEARCH_PAGE, offset=page * SEARCH_PAGE))
            out += got
            if len(got) < SEARCH_PAGE:
                break
        return out

    def _hydra_page(self, params):
        query = urllib.parse.urlencode(dict(params, apikey=self.cfg.hydra_apikey))
        with urllib.request.urlopen(self.cfg.hydra_url + "/api?" + query, timeout=self.cfg.timeout) as r:
            root = safe_xml(r.read())
        if root.tag == "error":
            raise OSError("hydra error %s: %s" % (root.get("code"), root.get("description")))
        out = []
        for it in root.iter("item"):
            a = {x.get("name"): x.get("value") for x in it if x.tag.endswith("}attr")}
            try:
                date = parsedate_to_datetime(a["usenetdate"]).timestamp()
            except (KeyError, TypeError, ValueError):
                date = 0.0
            out.append(Result((it.findtext("title") or "").strip(), (it.findtext("link") or "").strip(),
                              _int(a.get("size") or it.findtext("size")), _int(a.get("grabs")), date,
                              a.get("hydraIndexerName", "")))
        return out

    def queries(self, title, info):
        norm, short, group = normalize_title(title), short_query(title), release_attrs(title)["group"]
        qs = [{"t": "search", "q": norm.replace(".", " ")}]
        if short and short != qs[0]["q"]:
            qs.append({"t": "search", "q": short})
        if short and group:  # other indexers often rename a release; the group narrows to its postings
            qs.append({"t": "search", "q": "%s %s" % (short, group)})
        imdb, tvdb = (re.sub(r"\D", "", info.meta.get(k, "")) for k in ("imdb", "tvdb"))
        if imdb:
            qs.append({"t": "movie", "imdbid": imdb})
        m = re.search(r"(?:^|\.)s(\d+)e(\d+)(?:\.|$)", norm)
        if tvdb and m:
            qs.append({"t": "tvsearch", "tvdbid": tvdb, "season": int(m.group(1)), "ep": int(m.group(2))})
        return qs

    def fetch(self, r, deadline, retries=1):
        """-> (reason, nzb bytes, NzbInfo); reason 'ok', 'fetch', 'parse' or 'refused'. Indexers answer rate limits
        with 403/429 or an error body, sometimes only for a moment, so a failed fetch is retried once; an indexer
        still refusing (403/429) is not asked again for INDEXER_COOLDOWN. One fetch per indexer at a time, so
        its refusal is known before the next grab."""
        with self._indexer_lock(r.indexer):
            if self.refused.get(r.indexer, 0) > time.time():
                return "refused", None, None
            reason, data, info, status = self._fetch(r, deadline, retries)
            if status in (403, 429) and r.indexer:
                self.refused[r.indexer] = time.time() + INDEXER_COOLDOWN
                with self.state.lock:  # kept on disk: the next process (nzbget extension) honours it too
                    self.state.data["_refused"] = {"t": time.time(), "until": {
                        k: v for k, v in self.refused.items() if v > time.time()}}
                    self.state.save()
                log.info("indexer %s refused NZB downloads (HTTP %d): skipping it for %d min", r.indexer, status,
                         INDEXER_COOLDOWN // 60)
            return reason, data, info

    def _fetch(self, r, deadline, retries):
        status = None
        for attempt in range(retries + 1):
            timeout = max(1.0, min(self.cfg.timeout, deadline - time.time()))
            try:
                with urllib.request.urlopen(r.link, timeout=timeout) as resp:
                    data = resp.read()
            except OSError as e:
                reason, why, status = "fetch", mask(e), getattr(e, "code", None)
            else:
                status = None
                try:
                    return "ok", data, parse_nzb(data), None
                except ValueError:
                    reason, why = "parse", "not an NZB: %r" % mask(data[:160].decode("utf-8", "replace"))
            log.info("donor %s failed %s (%s), attempt %d: %s", reason, r.title, r.indexer, attempt + 1, why)
            if attempt < retries and time.time() + FETCH_RETRY_DELAY < deadline:
                time.sleep(FETCH_RETRY_DELAY)
        return reason, None, None, status

    def fetch_posting(self, listings, deadline):
        """One NZB of a posting listed by several indexers: its listings in turn until one serves an NZB of its
        listed size (an indexer may serve another indexer's NZB for a listing; that one is kept only if no
        listing serves the posting itself). -> (Result, reason, data, NzbInfo, Counter of the attempts)."""
        stats, last, other = Counter(), (listings[0], "fetch", None, None), None
        for j, r in enumerate(listings):
            reason, data, ci = self.fetch(r, deadline)
            if reason == "ok" and not listing_mismatch(r, ci):
                if j + 1 < len(listings):
                    stats["relisted"] += len(listings) - j - 1  # never fetched: the same posting
                return r, reason, data, ci, stats
            if reason == "ok":
                other = other or (r, reason, data, ci)
            else:
                stats[reason] += 1
                last = (r, reason, None, None)
        return (other or last) + (stats,)

    def discover(self, key, title, info, category, path, auth, nzbid, t0, base=0):
        """`base` lifts every DupeScore sent (donors base+2..base+90, dead base+1): 0 under the proxy's own
        primary at 100, pick - 1000 under a pick that a submitter scored higher (and its own backups)."""
        self.ctx.base = base
        cfg, stats, deadline, results = self.cfg, Counter(), t0 + self.cfg.deadline, {}
        nzbget_config = self.rpc_call(path, auth, "config", []) or []
        servers = self.news_servers(nzbget_config, title) if cfg.health_percent else []
        # the primary's own probe starts now, beside the searches: a dead primary is demoted within seconds,
        # before nzbget downloads (or parks) it
        primary_check = threading.Thread(target=self.check_primary, daemon=True,
                                         args=(servers, path, auth, nzbid, title, info, base))
        primary_check.start()
        dist = lambda size: abs(size - info.total_bytes)  # noqa: E731  closest size = most likely byte-identical
        pool = ThreadPoolExecutor(4)
        try:
            for f in [pool.submit(self.hydra_search, q) for q in self.queries(title, info)]:
                try:
                    for r in f.result(timeout=max(0.0, deadline - time.time())):
                        results.setdefault(r.link, r)
                except FutureTimeout:
                    stats["deadline"] += 1
                except Exception as e:
                    log.warning("hydra search failed for %s: %s", title, mask(e))
            cands = sorted((r for r in results.values() if r.link and candidate_ok(
                title, info.total_bytes, r, cfg.size_tolerance)), key=lambda r: (dist(r.size), -r.grabs, -r.date))
            rank = {id(r): i for i, r in enumerate(cands)}
            # postings (each listed by one or more indexers; most grabbed listing first), best listing first,
            # then one posting of each distinct size before the rest
            order = sorted((sorted(g, key=lambda r: rank[id(r)]) for g in group_listings(cands)),
                           key=lambda g: rank[id(g[0])])
            first_of_size = {g[0].size: g for g in reversed(order)}
            order = sorted(order, key=lambda g: first_of_size[g[0].size] is not g)
            if cfg.max_donors > 0:  # each fetch costs an indexer grab
                order = order[:3 * cfg.max_donors]
            known_f = pool.submit(self.known_postings, path, auth, nzbget_config, key, title)  # beside the fetches
            verified, postings = [], [info.message_ids]
            for i in range(0, len(order), 4):  # fetch the whole (capped) order: health may drop some later
                if time.time() > deadline:
                    stats["deadline"] += len(order) - i
                    break
                chunk = order[i:i + 4]
                for r, reason, data, ci, tried in pool.map(self.fetch_posting, chunk, [deadline] * len(chunk)):
                    stats.update(tried)
                    if reason != "ok":
                        continue
                    if listing_mismatch(r, ci):
                        stats["listing-mismatch"] += 1  # the indexer served some other NZB than it lists
                        log.info("%s served a different NZB than it lists for %s: %d bytes / %d files, "
                                 "listed %d bytes", r.indexer, r.title, ci.total_bytes, ci.files, r.size)
                    known, sk = known_f.result(), sketch(ci.message_ids)
                    if readable(ci.main_name) and not same_release(title, ci.main_name):
                        stats["other-release"] += 1
                    elif any(same_sketch(sk, k) for k in known):
                        stats["in-nzbget"] += 1
                    elif self.state.is_dead(sk):
                        stats["known-dead"] += 1  # found dead on an earlier grab: no new health check
                    elif any(same_posting(ci.message_ids, ids) for ids in postings):
                        stats["same-posting"] += 1
                    else:
                        postings.append(ci.message_ids)
                        verified.append((r, data, ci))
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
        verified.sort(key=lambda v: (dist(v[2].total_bytes), -v[0].grabs, -v[0].date))
        servers = servers if verified else []
        probe = self.health(servers, title, {ci.fingerprint: ci.message_ids for _, _, ci in verified}, full=False)
        twin = lambda ci: ci.files == info.files and ci.total_bytes == info.total_bytes  # noqa: E731
        live = [v for v in verified if not self.dead(v, probe, stats, "probe")]
        n_fast = int(min(cfg.fast_donors, cfg.donor_cap))
        ranks, added, placed = Ranks(), 0, {}  # placed: fingerprint -> Placed, donors now in nzbget
        for v in live[:n_fast]:  # quick: probe-alive donors go to nzbget right away
            score = ranks.take(alive_of(probe, v), twin(v[2]))
            donor_id = self.add_donor(key, category, path, auth, v, score, probe, "fast", stats)
            added += donor_id != 0
            if donor_id > 0:
                placed[v[2].fingerprint] = Placed(donor_id, score, alive_of(probe, v), twin(v[2]), v)
        # then full samples of every donor at once, handled as each finishes: refine fast donors, add the rest
        todo = {v[2].fingerprint: (v, True) for v in live[:n_fast] if servers}
        todo.update({v[2].fingerprint: (v, False) for v in live[n_fast:]})
        groups = {fp: v[2].message_ids for fp, (v, _) in todo.items()}
        groups["primary"] = info.message_ids
        checked = self.health_iter(servers, title, groups, full=True) if servers else iter(())
        unchecked = ((fp, None) for fp in list(todo))  # whatever the check did not report (no servers, failure)
        for fp, h in itertools.chain(checked, unchecked):
            if fp == "primary":
                log.info("health %s: primary alive=%s on %d server(s)", title, pct(h.alive), len(servers))
                continue
            entry = todo.pop(fp, None)
            if entry is None:  # already handled from the check's own result
                continue
            v, was_fast = entry
            health = {fp: h} if h else {}
            if was_fast:
                if fp in placed:
                    self.rescore(path, auth, placed[fp], health, ranks, stats)
            elif added >= cfg.donor_cap:
                stats["over-cap"] += 1
            elif not self.dead(v, health, stats, "full sample"):
                score = ranks.take(alive_of(health, v), twin(v[2]))
                donor_id = self.add_donor(key, category, path, auth, v, score, health, "checked", stats)
                added += donor_id != 0
                if donor_id > 0:
                    placed[fp] = Placed(donor_id, score, alive_of(health, v), twin(v[2]), v)
        self.rerank(path, auth, placed.values(), dist, stats)
        primary_check.join()
        log.info("append key=%s nzbid=%s title=%s results=%d candidates=%d verified=%d added=%d%s rejected=%s "
                 "time=%.1fs", key, nzbid, title, len(results), len(cands), len(verified),
                 0 if cfg.dry_run else added, " dry_run would_add=%d" % added if cfg.dry_run else "",
                 dict(stats), time.time() - t0)

    def known_postings(self, path, auth, nzbget_config, key, title):
        """Sketches of the NZBs nzbget already holds for this release: queue and history items with this
        DupeKey or the same release name, read (cached) from the copies nzbget keeps in its NzbDir."""
        opts = {e.get("Name"): str(e.get("Value", "")) for e in nzbget_config}
        nzbdir = opts.get("NzbDir", "").replace("${MainDir}", opts.get("MainDir", ""))
        if not nzbdir:
            return []
        items = (self.rpc_call(path, auth, "history", [True]) or []) + (self.rpc_call(path, auth, "listgroups", [0]) or [])
        names = {x["NZBFilename"] for x in items if x.get("NZBFilename") and (
            x.get("DupeKey") == key or same_release(title, x.get("NZBName") or ""))}
        out = []
        for name in names:
            for f in glob.glob(glob.escape(os.path.join(nzbdir, name)) + "*.queued"):
                try:
                    out.append(kept_sketch(f))
                except (OSError, ValueError):
                    continue
        return out

    def news_servers(self, nzbget_config, title):
        try:
            servers = donor_health.servers_from_nzbget_config(nzbget_config, self.cfg.max_conns_per_nntp_server,
                                                              self.cfg.timeout)
        except (ValueError, TypeError, KeyError) as e:
            log.warning("health check skipped for %s: unreadable news server config (%s)", title, type(e).__name__)
            return []
        if not servers:
            log.info("health check skipped for %s: no active news servers in nzbget config", title)
        return servers

    def health_iter(self, servers, title, groups, full):
        """Yields (key, Health) per NZB as its parallel check finishes; nothing when unchecked (advisory)."""
        if not servers:
            return
        limits = donor_health.Limits(self.cfg.nzbs_to_check_concurrently, self.cfg.nntp_server_connection_per_nzb)
        try:
            yield from donor_health.check_iter(servers, groups, self.cfg.health_percent, budget=self.cfg.health_budget,
                                               full=full, limits=limits, body_percent=self.cfg.body_percent,
                                               max_body=self.cfg.body_max_per_nzb,
                                               minimum=self.cfg.health_min_articles,
                                               maximum=self.cfg.health_max_articles)
        except Exception as e:
            log.warning("health check failed for %s: %s", title, type(e).__name__)

    def health(self, servers, title, groups, full):
        """{key: Health} for all `groups` (see health_iter)."""
        return dict(self.health_iter(servers, title, groups, full))

    def dead(self, v, health, stats, phase):
        """Probe: dead only if nothing was found (10 articles are too few to judge a share). Full sample:
        dead below DONOR_MIN_ALIVE."""
        h = health.get(v[2].fingerprint)
        if h is None or h.alive is None or h.missing < donor_health.MIN_KNOWN:  # errors alone prove nothing
            return False
        if not donor_health.dead_probe(h) if phase == "probe" else (h.alive >= self.cfg.donor_min_alive):
            return False
        stats["dead"] += 1
        self.state.mark_dead(v[2].fingerprint, sketch(v[2].message_ids))
        log.info("dropping dead donor %s [%s] after %s: alive=%s (%d of %d answered articles on no server, "
                 "%d errors)", v[0].title, v[0].indexer, phase, pct(h.alive), h.answered - h.present, h.answered,
                 h.error)
        return True

    def check_primary(self, servers, path, auth, nzbid, title, info, base=0):
        """Probe the primary; one with nothing on any server is demoted to DupeScore 1, so nzbget swaps in
        the healthiest donor as soon as one arrives. Never a partly alive one: the swap deletes what it
        downloaded, which is exactly what nzbget's repair from duplicates needs."""
        h = self.health(servers, title, {"primary": info.message_ids}, full=False).get("primary")
        if h is None or not donor_health.dead_probe(h) or nzbid <= 0:
            return
        self.state.mark_dead(info.fingerprint, sketch(info.message_ids))
        what = "%s: primary is dead (%d of %d probe articles on no server)" % (title, h.missing, h.answered)
        if self.cfg.dry_run:
            log.info("%s; DRY-RUN: not demoted", what)
        elif self.rpc_call(path, auth, "editqueue", ["GroupSetDupeScore", str(base + 1), [nzbid]]):
            log.info("%s: DupeScore -> %d, the healthiest donor takes over", what, base + 1)
        else:
            log.info("%s but already left the queue (nzbget parked or finished it); donors take over from history",
                     what)

    def set_score(self, path, auth, donor_id, score, param=None):
        """DupeScore (and a parameter) of a donor in history or, once nzbget moved it back, in the queue."""
        for kind in ("History", "Group"):  # donors normally sit in history as dupe backups
            cmd = [kind + "SetDupeScore", str(score + self.base()), [donor_id]]
            if self.rpc_call(path, auth, "editqueue", cmd):
                if param:
                    self.rpc_call(path, auth, "editqueue", [kind + "SetParameter", param, [donor_id]])
                return True
        return False

    def rerank(self, path, auth, placed, dist, stats):
        """Once every check is in: DupeScores in wholeness order (whole percent; then twin, closest size, grabs),
        strictly falling, each at most its target_score. Donors were added in the order their checks finished,
        so a wholer one can sit below a worse one until now."""
        live = sorted((p for p in placed if p.score > 1), key=lambda p: (
            -round(100 * (1.0 if p.alive is None else p.alive)), not p.twin, dist(p.v[2].total_bytes),
            -p.v[0].grabs))
        prev = 100
        for p in live:
            want = max(2, min(target_score(p.alive, p.twin), prev - 1))
            prev = want
            if want == p.score:
                continue
            if self.set_score(path, auth, p.id, want):
                log.info("reranked donor nzbid=%d %s [%s]: score %d -> %d, alive=%s", p.id, p.v[0].title,
                         p.v[0].indexer, p.score, want, pct(p.alive))
                p.score = want
            else:
                stats["rescore"] += 1

    def rescore(self, path, auth, p, health, ranks, stats):
        """After its full sample, move a fast donor's DupeScore (and DupeAlive) to its real health."""
        donor_id, old, is_twin, v = p.id, p.score, p.twin, p.v
        h = health.get(v[2].fingerprint)
        if h is None or h.alive is None:
            return
        dead = h.alive < self.cfg.donor_min_alive and h.missing >= donor_health.MIN_KNOWN
        ranks.release(old)
        new = 1 if dead else ranks.take(h.alive, is_twin)
        if not self.set_score(path, auth, donor_id, new, "DupeAlive=%d" % round(100 * h.alive)):
            ranks.release(new)
            ranks.used.add(old)
            stats["rescore"] += 1
            log.warning("could not rescore donor nzbid=%d %s [%s]: keeps score %d, alive=%s", donor_id, v[0].title,
                        v[0].indexer, old, pct(h.alive))
            return
        p.score, p.alive = new, h.alive
        if dead:
            stats["dead"] += 1
            self.state.mark_dead(v[2].fingerprint, sketch(v[2].message_ids))
        log.info("rescored donor nzbid=%d %s [%s]: score %d -> %d, alive=%s (full sample%s)", donor_id, v[0].title,
                 v[0].indexer, old, new, pct(h.alive), ", dead" if dead else "")

    def add_donor(self, key, category, path, auth, v, score, health, how, stats):
        """Append one donor (or log it in dry run); returns its NZBID, -1 in dry run, 0 if not added."""
        r, data, ci = v
        h = health.get(ci.fingerprint)
        desc = "score=%d %s [%s, %d files, %d bytes, grabs=%d, alive=%s] (%s)" % (
            score + self.base(), r.title, r.indexer, ci.files, ci.total_bytes, r.grabs, pct(h.alive if h else None), how)
        if self.cfg.dry_run:
            log.info("DRY-RUN would add donor %s", desc)
            return -1
        with self.state.lock:
            if self.state.sent(key, ci.fingerprint):
                stats["already-sent"] += 1
                return 0
            name = r.title if r.title.lower().endswith(".nzb") else r.title + ".nzb"
            pp = [{"Name": "DupeAlive", "Value": str(round(100 * h.alive))}] if h and h.alive is not None else []
            params = [name, base64.b64encode(data).decode(), category, 0, False, False, key, score + self.base(),
                      "SCORE", pp]
            donor_id = self.rpc(path, auth, "append", params)
            if donor_id <= 0:
                stats["append"] += 1
                return 0
            self.state.record(key, ci.fingerprint, donor_id)
        log.info("added donor nzbid=%d %s", donor_id, desc)
        return donor_id

    def base(self):
        return getattr(self.ctx, "base", 0)

    def watch_auth(self):
        if not self.cfg.nzbget_username:
            return ""
        login = "%s:%s" % (self.cfg.nzbget_username, self.cfg.nzbget_password)
        return "Basic " + base64.b64encode(login.encode()).decode()

    def watch_once(self):
        """One look at nzbget's queue: each new pick (the top DupeScore of its DupeKey, not yet past download,
        not appended by this proxy) gets a donor discovery, as if it had come through the proxy. Backups and
        nzbget's failover promotions rank below their pick and are left alone."""
        path, auth, now = "/jsonrpc", self.watch_auth(), time.time()
        queue = self.rpc_call(path, auth, "listgroups", [0]) or []
        history = None
        for g in queue:
            nzbid = _int(g.get("NZBID"))
            if g.get("Status") not in WATCH_STATUSES or nzbid <= 0 or nzbid in self.watched:
                continue
            if now - self.first_seen.setdefault(nzbid, now) < self.cfg.watch_settle:
                continue
            self.watched.add(nzbid)
            if g.get("DupeKey") and history is None:
                history = self.rpc_call(path, auth, "history", [True]) or []
            job = self.pick_job(path, auth, g, queue, history or [])
            if job is None:
                continue
            t = threading.Thread(target=self._discover_safe, daemon=True, args=job)
            with self.workers_lock:
                self.workers = [w for w in self.workers if w.is_alive()] + [t]
            t.start()

    def pick_job(self, path, auth, g, queue, history):
        """discover() arguments for queue item `g` if it is a new pick (the top DupeScore of its DupeKey, not
        appended by this proxy), after giving it a DupeKey and primary_score when it lacks them; else None.
        Backups and nzbget's failover promotions rank below their pick and are left alone."""
        nzbid = _int(g.get("NZBID"))
        key, score, title = g.get("DupeKey") or "", _int(g.get("DupeScore")), g.get("NZBName") or ""
        if key and any(x is not g and _int(x.get("NZBID")) != nzbid and
                       str(x.get("DupeKey", "")).lower() == key.lower() and _int(x.get("DupeScore")) > score
                       for x in queue + history):
            return None  # a backup or promoted duplicate: its pick was (or is) handled
        info, ambiguous = self.queued_nzb(path, auth, g)
        if info is None:
            return None
        if nzbid in self.state.nzbids_for(info.fingerprint):
            log.info("watch: nzbid=%d %s was handled already (appended or searched by this proxy)", nzbid, title)
            return None  # the same queue item again; a re-submission of the same NZB gets a new NZBID
        if not key or score < self.cfg.primary_score:  # managed here from now on: lift it to primary_score
            key = key or "dupes:" + normalize_title(title)
            for cmd, arg in (("GroupSetDupeKey", key), ("GroupSetDupeScore", str(self.cfg.primary_score)), ("GroupSetDupeMode", "SCORE")):
                self.rpc_call(path, auth, "editqueue", [cmd, arg, [nzbid]])
            score = self.cfg.primary_score
        with self.state.lock:
            self.state.record(key, info.fingerprint, nzbid, title, touch=True)
        log.info("watch: new pick nzbid=%d %s (key=%s score=%d): discovering donors%s", nzbid, title, key, score,
                 "; its NZB is one of several postings of the same size, so it is never demoted" if ambiguous
                 else "")
        return (key, title, info, g.get("Category") or "", path, auth, -nzbid if ambiguous else nzbid, time.time(),
                score_base(score))

    def queued_nzb(self, path, auth, g):
        """(NzbInfo, ambiguous) of a queue item from the copy nzbget keeps in NzbDir (name[.N].queued: the one
        whose size matches), or (None, False). Ambiguous: another posting of exactly that size shares the name
        (byte-identical reposts), so which one is the item's own can't be told."""
        opts = {e.get("Name"): str(e.get("Value", "")) for e in self.rpc_call(path, auth, "config", []) or []}
        nzbdir = opts.get("NzbDir", "").replace("${MainDir}", opts.get("MainDir", ""))
        size = (_int(g.get("FileSizeHi")) << 32) + _int(g.get("FileSizeLo"))
        infos = []
        for f in sorted(glob.glob(glob.escape(os.path.join(nzbdir, g.get("NZBFilename") or "?")) + "*.queued")):
            try:
                with open(f, "rb") as fh:
                    infos.append(parse_nzb(fh.read()))
            except (OSError, ValueError):
                continue
        if not infos:
            log.info("watch: no readable NZB for nzbid=%s %s in %s", g.get("NZBID"), g.get("NZBName"), nzbdir)
            return None, False
        best = min(infos, key=lambda i: abs(i.total_bytes - size))
        rivals = {i.fingerprint for i in infos if i.total_bytes == best.total_bytes}
        return best, len(rivals) > 1

    def watch_forever(self):
        while True:
            try:
                self.watch_once()
            except Exception:
                log.exception("watch: queue poll failed")
            time.sleep(self.cfg.watch_interval)

    def rpc_call(self, path, auth, method, params):
        """JSON-RPC call to nzbget with Hydra's own path + credentials; raw result or None."""
        body = json.dumps({"jsonrpc": "2.0", "id": "dupe-proxy", "method": method, "params": params}).encode()
        status, rbody, _ = self.forward(path, body, {"Authorization": auth, "Content-Type": "application/json"})
        try:
            return json.loads(rbody).get("result") if status == 200 else None
        except (ValueError, AttributeError):
            return None

    def rpc(self, path, auth, method, params):
        """Integer result of an nzbget JSON-RPC call (NZBID for append), 0 on error."""
        return _int(self.rpc_call(path, auth, method, params))

    def start(self, host="0.0.0.0"):
        handler = type("BoundHandler", (Handler,), {"proxy": self})
        self.server = ThreadingHTTPServer((host, self.cfg.listen_port), handler)  # daemon_threads by default
        self.url = "http://%s:%d" % (host, self.server.server_address[1])
        threading.Thread(target=self.server.serve_forever, args=(0.1,), daemon=True).start()
        if self.cfg.watch_nzbget and self.cfg.enabled:
            threading.Thread(target=self.watch_forever, daemon=True).start()
        return self.server.server_address[1]

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    def wait_idle(self, timeout):
        for t in list(self.workers):
            t.join(timeout)


def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(levelname)s %(message)s")
    cfg = Config.from_env(os.environ)
    cfg.dry_run = cfg.dry_run or "--dry-run" in sys.argv
    port = Proxy(cfg).start()
    log.info("listening on :%d -> %s (enabled=%s dry_run=%s hydra=%s max_donors=%s watch_nzbget=%s)", port,
             mask(cfg.nzbget_url), cfg.enabled, cfg.dry_run, mask(cfg.hydra_url), cfg.max_donors if cfg.max_donors > 0
             else "unlimited", cfg.watch_nzbget)
    threading.Event().wait()


if __name__ == "__main__":
    main()
