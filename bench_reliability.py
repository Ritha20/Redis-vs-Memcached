"""Experiments E4-E7: memory, crash durability, failover/HA, concurrent session updates.

E4 memory    memory footprint of 20,000 sessions per size class
E5 crash     acknowledged sessions surviving kill -9 + restart, per persistence mode
E6 failover  Redis (replica + 3 Sentinels) vs 3-node Memcached ring: downtime and lost sessions
E7 cart      concurrent cart updates on one session: lost updates and retries
"""
import json
import multiprocessing as mp
import os
import random
import signal
import subprocess
import sys
import time

import numpy as np
import redis
from pymemcache.client.base import Client as MCClient
from pymemcache.client.hash import HashClient
from redis.sentinel import Sentinel

from common import RUN, TTL, Store, load_sessions, start_memcached, start_redis, stop, wait_port

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
REPS = int(os.environ.get("REPS", 3))


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS"):
                return int(line.split()[1])


# ---------------------------------------------------------------- E4 memory
def e4_memory():
    rows = []
    for size in ("small", "medium", "large"):
        keys, vals = load_sessions(size)
        raw = sum(len(k) + len(v) for k, v in zip(keys, vals))
        for kind in ("redis", "memcached"):
            port = 6391 if kind == "redis" else 11291
            srv = start_redis(port) if kind == "redis" else start_memcached(port)
            time.sleep(0.3)
            base = rss_kb(srv.pid)
            st = Store(kind, port)
            st.preload(keys, vals)
            time.sleep(0.5)
            rss = rss_kb(srv.pid) - base
            if kind == "redis":
                info = st.c.info("memory")
                logical = info["used_memory"] - info["used_memory_startup"]
            else:
                s = st.c.stats()
                logical = int(s[b"bytes"])
                slab = st.c.stats("slabs")
                logical_alloc = int(slab.get(b"total_malloced", 0))
            row = {"store": kind, "size": size, "n": len(keys), "raw_bytes": raw,
                   "rss_delta_bytes": rss * 1024, "logical_bytes": logical,
                   "rss_per_session": rss * 1024 / len(keys),
                   "overhead_ratio": rss * 1024 / raw}
            if kind == "memcached":
                row["slab_malloced_bytes"] = logical_alloc
            rows.append(row)
            print("[E4]", row, flush=True)
            stop(srv)
    return rows


# ---------------------------------------------------------------- E5 crash durability
def e5_crash():
    """Preload 20k sessions, then create new sessions at ~RATE/s for RUN_S seconds,
    kill -9 the server, restart it on the same data directory and count survivors."""
    RATE, RUN_S = 1000, 75
    keys, vals = load_sessions("medium")
    rows = []
    configs = ["memcached", "none", "rdb", "aof_everysec", "aof_always"]
    for cfg in configs:
        for rep in range(int(os.environ.get("CRASH_REPS", 2))):
            kind = "memcached" if cfg == "memcached" else "redis"
            port = 6392 if kind == "redis" else 11292
            workdir = os.path.join(RUN, f"crash_{cfg}")
            srv = start_redis(port, cfg, workdir=workdir) if kind == "redis" else start_memcached(port)
            st = Store(kind, port)
            st.preload(keys, vals)
            acked = list(keys)
            rng = random.Random(rep)
            t0 = time.time()
            i = 0
            while time.time() - t0 < RUN_S:
                k = f"sess:new:{rep}:{i}"
                st.set(k, vals[rng.randrange(len(vals))])
                acked.append(k)
                i += 1
                # pace to RATE writes/s
                lag = t0 + i / RATE - time.time()
                if lag > 0:
                    time.sleep(lag)
            t_kill = time.time()
            srv.send_signal(signal.SIGKILL)
            srv.wait()
            # restart on the same directory (no wipe) and time recovery
            t_restart = time.time()
            if kind == "redis":
                args = ["redis-server", "--port", str(port), "--dir", workdir, "--protected-mode", "no",
                        "--loglevel", "warning", "--logfile", os.path.join(workdir, "log2")]
                if cfg == "rdb":
                    args += ["--save", "3600 1 300 100 60 10000", "--appendonly", "no"]
                elif cfg.startswith("aof"):
                    args += ["--save", "", "--appendonly", "yes",
                             "--appendfsync", "everysec" if cfg == "aof_everysec" else "always"]
                else:
                    args += ["--save", "", "--appendonly", "no"]
                srv = subprocess.Popen(args)
                wait_port(port, 60)
                c = redis.Redis(port=port)
                while True:
                    try:
                        if c.info("persistence")["loading"] == 0:
                            break
                    except redis.exceptions.BusyLoadingError:
                        pass
                    time.sleep(0.01)
            else:
                srv = start_memcached(port)
            t_ready = time.time() - t_restart
            st = Store(kind, port)
            survived = 0
            for j in range(0, len(acked), 500):
                chunk = acked[j:j + 500]
                if kind == "redis":
                    survived += sum(v is not None for v in st.c.mget(chunk))
                else:
                    survived += len(st.c.get_many(chunk))
            new_written = i
            new_lost = len(acked) - survived  # preloaded are counted too
            row = {"config": cfg, "rep": rep, "acked_total": len(acked), "survived": survived,
                   "lost": new_lost, "survival_pct": 100 * survived / len(acked),
                   "lost_seconds_of_writes": new_lost / RATE if new_lost <= new_written else None,
                   "recovery_s": t_ready, "new_sessions_written": new_written}
            rows.append(row)
            print("[E5]", row, flush=True)
            stop(srv)
    return rows


# ---------------------------------------------------------------- E6 failover
def _write_loop(write_fn, duration, kill_at, kill_fn, rate=1000):
    """Write unique sessions at `rate`/s; kill a node at `kill_at`. Returns timeline."""
    acked, events = [], []
    t0 = time.time()
    i = 0
    killed = False
    val = b"x" * 2048
    lat = []
    while time.time() - t0 < duration:
        now = time.time() - t0
        if not killed and now >= kill_at:
            kill_fn()
            killed = True
            events.append(("kill", now))
        k = f"sess:ha:{i}"
        ts = time.perf_counter()
        ok = write_fn(k, val)
        lat.append((time.perf_counter() - ts) * 1e6)
        events.append(("ok" if ok else "err", time.time() - t0))
        if ok:
            acked.append(k)
        i += 1
        lag = t0 + i / rate - time.time()
        if lag > 0:
            time.sleep(lag)
    return acked, events, lat


def _downtime(events):
    kill_t = next(t for e, t in events if e == "kill")
    last_ok_before = max([t for e, t in events if e == "ok" and t <= kill_t], default=kill_t)
    first_ok_after = min([t for e, t in events if e == "ok" and t > kill_t], default=None)
    errs = sum(1 for e, t in events if e == "err")
    return (first_ok_after - last_ok_before) if first_ok_after else None, errs


def e6_redis(rep, wait_replicas=0):
    base = os.path.join(RUN, f"ha_{rep}")
    subprocess.run(["rm", "-rf", base])
    os.makedirs(base)
    p_primary = start_redis(6400, "none", workdir=os.path.join(base, "p"))
    p_replica = start_redis(6401, "none", extra=["--replicaof", "127.0.0.1", "6400"],
                            workdir=os.path.join(base, "r"))
    sentinels = []
    for sp in (26400, 26401, 26402):
        conf = os.path.join(base, f"s{sp}.conf")
        with open(conf, "w") as f:
            f.write(f"port {sp}\nsentinel monitor mymaster 127.0.0.1 6400 2\n"
                    "sentinel down-after-milliseconds mymaster 1000\n"
                    "sentinel failover-timeout mymaster 5000\nsentinel parallel-syncs mymaster 1\n")
        sentinels.append(subprocess.Popen(["redis-server", conf, "--sentinel", "--loglevel", "warning",
                                           "--logfile", os.path.join(base, f"s{sp}.log")]))
        wait_port(sp)
    time.sleep(3)  # sentinels discover replica + each other
    sen = Sentinel([("127.0.0.1", p) for p in (26400, 26401, 26402)], socket_timeout=0.2)
    keys, vals = load_sessions("medium")
    loader = sen.master_for("mymaster", socket_timeout=10)
    for j in range(0, len(keys), 1000):
        pipe = loader.pipeline(transaction=False)
        for k, v in zip(keys[j:j + 1000], vals[j:j + 1000]):
            pipe.set(k, v, ex=TTL)
        pipe.execute()
    loader.wait(1, 5000)
    m = sen.master_for("mymaster", socket_timeout=0.2)

    def write(k, v):
        try:
            m.set(k, v, ex=TTL)
            if wait_replicas:
                # synchronous-ish replication: block until 1 replica acks (100 ms cap)
                if m.wait(1, 100) < 1:
                    return False
            return True
        except Exception:
            return False

    acked, events, lat = _write_loop(write, 15, 5, lambda: p_primary.send_signal(signal.SIGKILL))
    p_primary.wait()
    downtime, errs = _downtime(events)
    time.sleep(1)
    m2 = sen.master_for("mymaster", socket_timeout=10)
    survived_new = sum(sum(v is not None for v in m2.mget(acked[j:j + 1000])) for j in range(0, len(acked), 1000))
    survived_pre = sum(sum(v is not None for v in m2.mget(keys[j:j + 1000])) for j in range(0, len(keys), 1000))
    row = {"system": "redis_sentinel" + ("_wait1" if wait_replicas else ""), "rep": rep,
           "downtime_s": downtime, "write_errors": errs, "acked_writes": len(acked),
           "lost_acked_writes": len(acked) - survived_new,
           "preloaded_sessions_lost_pct": 100 * (len(keys) - survived_pre) / len(keys),
           "write_p50_us": float(np.percentile(lat, 50)), "write_p99_us": float(np.percentile(lat, 99))}
    for p in sentinels + [p_replica]:
        stop(p)
    return row


def e6_memcached(rep):
    ports = [11300, 11301, 11302]
    procs = [start_memcached(p, threads=2, mem_mb=1024) for p in ports]
    hc = HashClient([("127.0.0.1", p) for p in ports], use_pooling=False, ignore_exc=False,
                    retry_attempts=2, retry_timeout=1, dead_timeout=60, connect_timeout=0.2, timeout=0.2,
                    no_delay=True)
    keys, vals = load_sessions("medium")
    for j in range(0, len(keys), 500):
        hc.set_many(dict(zip(keys[j:j + 500], vals[j:j + 500])), expire=TTL)
    victim = procs[0]

    def write(k, v):
        try:
            return bool(hc.set(k, v, expire=TTL, noreply=False))
        except Exception:
            return False

    acked, events, lat = _write_loop(write, 15, 5, lambda: victim.send_signal(signal.SIGKILL))
    victim.wait()
    downtime, errs = _downtime(events)
    survived_new = sum(1 for k in acked if _safe_get(hc, k) is not None)
    survived_pre = sum(1 for k in keys if _safe_get(hc, k) is not None)
    row = {"system": "memcached_3node", "rep": rep, "downtime_s": downtime, "write_errors": errs,
           "acked_writes": len(acked), "lost_acked_writes": len(acked) - survived_new,
           "preloaded_sessions_lost_pct": 100 * (len(keys) - survived_pre) / len(keys),
           "write_p50_us": float(np.percentile(lat, 50)), "write_p99_us": float(np.percentile(lat, 99))}
    for p in procs[1:]:
        stop(p)
    return row


def _safe_get(hc, k):
    try:
        return hc.get(k)
    except Exception:
        return None


def e6_failover():
    rows = []
    for rep in range(REPS):
        for fn in (lambda r: e6_redis(r), lambda r: e6_redis(r, wait_replicas=1), e6_memcached):
            row = fn(rep)
            rows.append(row)
            print("[E6]", row, flush=True)
    return rows


# ---------------------------------------------------------------- E7 concurrent cart updates
N_WORKERS, ADDS = 8, 250


def _cart_worker(args):
    mode, wid = args
    rng = random.Random(wid)
    retries = 0
    lat = []
    if mode.startswith("redis"):
        c = redis.Redis(port=6393)
    else:
        c = MCClient(("127.0.0.1", 11293), no_delay=True)
    for n in range(ADDS):
        item = {"sku": f"{wid}-{n}", "qty": 1}
        t0 = time.perf_counter()
        if mode == "redis_atomic":
            c.rpush("cart:s1", json.dumps(item))  # native list append, atomic server-side
        elif mode in ("redis_naive", "memcached_naive"):
            raw = c.get("sess:s1")
            s = json.loads(raw)
            s["cart"].append(item)
            c.set("sess:s1", json.dumps(s))
        elif mode == "memcached_cas":
            while True:
                raw, token = c.gets("sess:s1")
                s = json.loads(raw)
                s["cart"].append(item)
                if c.cas("sess:s1", json.dumps(s), token):
                    break
                retries += 1
        lat.append((time.perf_counter() - t0) * 1e6)
    return retries, lat


def e7_cart():
    rows = []
    for mode in ("redis_naive", "redis_atomic", "memcached_naive", "memcached_cas"):
        for rep in range(REPS):
            if mode.startswith("redis"):
                srv = start_redis(6393)
                c = redis.Redis(port=6393)
                c.set("sess:s1", json.dumps({"user": "u1", "cart": []}))
            else:
                srv = start_memcached(11293)
                c = MCClient(("127.0.0.1", 11293))
                c.set("sess:s1", json.dumps({"user": "u1", "cart": []}), noreply=False)
            t0 = time.time()
            with mp.get_context("fork").Pool(N_WORKERS) as pool:
                res = pool.map(_cart_worker, [(mode, w) for w in range(N_WORKERS)])
            elapsed = time.time() - t0
            if mode == "redis_atomic":
                final = c.llen("cart:s1")
            else:
                final = len(json.loads(c.get("sess:s1"))["cart"])
            lat = [x for r in res for x in r[1]]
            expected = N_WORKERS * ADDS
            row = {"mode": mode, "rep": rep, "expected_items": expected, "final_items": final,
                   "lost_updates": expected - final, "lost_pct": 100 * (expected - final) / expected,
                   "cas_retries": sum(r[0] for r in res), "elapsed_s": elapsed,
                   "mean_us": float(np.mean(lat)), "p99_us": float(np.percentile(lat, 99))}
            rows.append(row)
            print("[E7]", row, flush=True)
            stop(srv)
    return rows


if __name__ == "__main__":
    which = sys.argv[1]
    os.makedirs(OUT, exist_ok=True)
    rows = {"memory": e4_memory, "crash": e5_crash, "failover": e6_failover, "cart": e7_cart}[which]()
    with open(os.path.join(OUT, f"{which}.json"), "w") as f:
        json.dump(rows, f, indent=1)
