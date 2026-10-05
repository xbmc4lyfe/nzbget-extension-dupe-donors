#!/usr/bin/env python3
"""DupeDonors: nzbget QUEUE extension that gives every new download health-checked duplicate donors.

On NZB_ADDED it starts a detached worker and exits at once (nzbget runs one queue extension at a time). The
worker waits SettleSeconds, then, if the item is a new pick (the top DupeScore of its DupeKey and not added
by DupeDonors), finds the other postings of the release through a newznab indexer (NZBHydra2), checks their
articles on nzbget's news servers and appends the live ones under the pick's DupeKey, most whole first.
Workers run one at a time (a lock in the state directory), so the news-server connection cap holds. The
code is the nzbget-dupe-proxy service's (nzbget_dupe_proxy.py, donor_health.py, vendor/), unchanged.
"""
import fcntl
import logging
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import nzbget_dupe_proxy as ndp  # noqa: E402

COMMAND_SUCCESS, COMMAND_ERROR = 93, 94
OPTIONS = {  # extension option -> service setting
    "HydraUrl": "hydra_url", "HydraApiKey": "hydra_apikey", "MaxDonors": "max_donors",
    "SizeTolerance": "size_tolerance", "HealthPercent": "health_percent",
    "HealthMinArticles": "health_min_articles", "HealthMaxArticles": "health_max_articles",
    "BodyPercent": "body_percent", "BodyMaxPerNzb": "body_max_per_nzb", "DonorMinAlive": "donor_min_alive",
    "HealthBudget": "health_budget", "FastDonors": "fast_donors",
    "NzbsToCheckConcurrently": "nzbs_to_check_concurrently",
    "NntpServerConnectionPerNzb": "nntp_server_connection_per_nzb",
    "MaxConnsPerNntpServer": "max_conns_per_nntp_server", "PrimaryScore": "primary_score",
    "SettleSeconds": "watch_settle", "DryRun": "dry_run", "StateDir": "state_dir",
}
log = logging.getLogger("nzbget-dupe-proxy")


def option(env, name):
    """An extension option from nzbget's env (NZBPO_Name, or NZBPO_NAME in upper case), or None."""
    v = env.get("NZBPO_" + name)
    return env.get("NZBPO_" + name.upper()) if v is None else v


def config(env):
    """The service Config from the extension options and nzbget's own control settings."""
    host = env.get("NZBOP_CONTROLIP") or "127.0.0.1"
    host = "127.0.0.1" if host in ("0.0.0.0", "::") else host
    values = {"NZBGET_URL": "http://%s:%s" % (host, env.get("NZBOP_CONTROLPORT") or "6789"),
              "NZBGET_USERNAME": env.get("NZBOP_CONTROLUSERNAME", ""),
              "NZBGET_PASSWORD": env.get("NZBOP_CONTROLPASSWORD", ""),
              "STATE_DIR": os.path.join(env.get("NZBOP_MAINDIR") or HERE, "dupe-donors")}
    for name, field in OPTIONS.items():
        v = option(env, name)
        if v not in (None, ""):
            values[field.upper()] = v
    return ndp.Config.from_env(values)


class NzbgetLog(logging.Handler):
    """Sends log lines to nzbget's Messages (JSON-RPC writelog), apikeys and passwords masked."""

    def __init__(self, proxy):
        super().__init__(logging.INFO)
        self.proxy, self.busy = proxy, False

    def emit(self, record):
        if self.busy:  # a failing writelog must not log about itself
            return
        self.busy = True
        try:
            kind = "ERROR" if record.levelno >= logging.ERROR else "WARNING" if record.levelno >= logging.WARNING \
                else "INFO"
            self.proxy.rpc_call("/jsonrpc", self.proxy.watch_auth(), "writelog",
                                [kind, "DupeDonors: " + ndp.mask(record.getMessage())])
        finally:
            self.busy = False


def worker(env, nzbid):
    cfg = config(env)
    os.makedirs(cfg.state_dir, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        filename=os.path.join(cfg.state_dir, "dupe-donors.log"))
    time.sleep(cfg.watch_settle)  # the submitter's own backups land first
    with open(os.path.join(cfg.state_dir, "worker.lock"), "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)  # one worker at a time: one owner of the news-server connection cap
        proxy = ndp.Proxy(cfg)
        handler = NzbgetLog(proxy)
        log.setLevel(logging.INFO)
        log.addHandler(handler)
        try:
            handle(proxy, nzbid)
        finally:
            log.removeHandler(handler)
    return 0


def handle(proxy, nzbid):
    path, auth = "/jsonrpc", proxy.watch_auth()
    queue = proxy.rpc_call(path, auth, "listgroups", [0]) or []
    g = next((x for x in queue if ndp._int(x.get("NZBID")) == nzbid), None)
    if g is None or g.get("Status") not in ndp.WATCH_STATUSES:
        return  # gone, or already past download
    job = proxy.pick_job(path, auth, g, queue, proxy.rpc_call(path, auth, "history", [True]) or [])
    if job is not None:
        proxy.discover(*job)


def connection_test(env):
    """Settings-page button: can the indexer and nzbget be reached with these options?"""
    missing = [n for n in ("HydraUrl", "HydraApiKey") if not option(env, n)]
    if missing:
        print("[ERROR] %s is empty: enter it, click Save, then test again (the test uses saved settings)"
              % " and ".join(missing))
        return COMMAND_ERROR
    cfg = config(env)
    try:
        query = urllib.parse.urlencode({"t": "caps", "apikey": cfg.hydra_apikey})
        with urllib.request.urlopen(cfg.hydra_url + "/api?" + query, timeout=cfg.timeout) as r:
            root = ndp.safe_xml(r.read())
        if root.tag == "error":
            raise OSError("indexer error %s: %s" % (root.get("code"), root.get("description")))
        print("[INFO] Hydra/newznab at %s answers" % ndp.mask(cfg.hydra_url))
    except Exception as e:
        print("[ERROR] Hydra/newznab at %s: %s" % (ndp.mask(cfg.hydra_url), ndp.mask(e)))
        return COMMAND_ERROR
    proxy = ndp.Proxy(cfg)
    servers = proxy.news_servers(proxy.rpc_call("/jsonrpc", proxy.watch_auth(), "config", []) or [], "connection test")
    print("[INFO] %d active news server(s) for health checks" % len(servers))
    return COMMAND_SUCCESS


def main(env, argv):
    if env.get("NZBCP_COMMAND"):
        return connection_test(env) if env["NZBCP_COMMAND"] == "ConnectionTest" else COMMAND_ERROR
    if "--worker" in argv:
        return worker(env, int(argv[argv.index("--worker") + 1]))
    if env.get("NZBNA_EVENT") != "NZB_ADDED":
        return 0
    missing = [n for n in ("HydraUrl", "HydraApiKey") if not option(env, n)]
    if missing:
        print("[ERROR] DupeDonors: set %s in the extension settings and save them" % ", ".join(missing))
        return 0
    state_dir = config(env).state_dir
    os.makedirs(state_dir, exist_ok=True)
    out = open(os.path.join(state_dir, "dupe-donors.log"), "a")
    subprocess.Popen([sys.executable, os.path.abspath(__file__), "--worker", env.get("NZBNA_NZBID", "0")],
                     env=dict(env), stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                     start_new_session=True, close_fds=True)
    print("[INFO] DupeDonors: searching for other postings of %s in the background" % env.get("NZBNA_NZBNAME", ""))
    return 0


if __name__ == "__main__":
    sys.exit(main(os.environ, sys.argv[1:]))
