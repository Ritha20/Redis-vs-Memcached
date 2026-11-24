"""Experiments E1-E3: latency/throughput sweeps and persistence overhead.

E1  concurrency x read/write mix, 2 KB sessions, both stores
E2  session size sweep (0.57/2/8 KB), 95/5 mix, 16 clients
E3  Redis persistence modes, 50/50 mix, 16 clients, 2 KB (Memcached as reference)
Each configuration is repeated REPS times on a freshly started, preloaded server.
"""
import json
import multiprocessing as mp
import os
import random
import sys
import time

import numpy as np

from common import Store, hot_index, load_sessions, start_memcached, start_redis, stop

DURATION = float(os.environ.get("DURATION", 6))
WARMUP = 1.0
REPS = int(os.environ.get("REPS", 3))
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")

KEYS, VALS = [], []


def worker(args):
    kind, port, write_ratio, seed, t_start, t_end = args
    rng = random.Random(seed)
    st = Store(kind, port)
    n = len(KEYS)
    lat_r, lat_w = [], []
    misses = errors = 0
    while time.time() < t_start:
        time.sleep(0.001)
    t_warm = t_start + WARMUP
    while True:
        now = time.time()
        if now >= t_end:
            break
        i = hot_index(n, rng)
        is_write = rng.random() < write_ratio
        t0 = time.perf_counter_ns()
        try:
            if is_write:
                st.set(KEYS[i], VALS[rng.randrange(n)])
            else:
                if st.get(KEYS[i]) is None:
                    misses += 1
        except Exception:
            errors += 1
            continue
        dt = (time.perf_counter_ns() - t0) / 1000.0  # microseconds
        if now >= t_warm:
            (lat_w if is_write else lat_r).append(dt)
    return lat_r, lat_w, misses, errors


def server_cpu(pid):
    """Total user+system CPU seconds consumed by the server process (all threads)."""
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    tck = os.sysconf("SC_CLK_TCK")
    return (int(fields[11]) + int(fields[12])) / tck


def summarize(lat):
    if not lat:
        return {}
    a = np.asarray(lat)
    return {"n": int(a.size), "mean": float(a.mean()), "p50": float(np.percentile(a, 50)),
            "p95": float(np.percentile(a, 95)), "p99": float(np.percentile(a, 99)),
            "p999": float(np.percentile(a, 99.9))}


def run_one(kind, write_ratio, clients, persistence="none", port=None):
    port = port or (6390 if kind == "redis" else 11290)
    srv = start_redis(port, persistence) if kind == "redis" else start_memcached(port)
    try:
        Store(kind, port).preload(KEYS, VALS)
        time.sleep(0.5)
        t_start = time.time() + 1.0
        t_end = t_start + WARMUP + DURATION
        ctx = mp.get_context("fork")
        with ctx.Pool(clients) as pool:
            async_res = pool.map_async(worker, [(kind, port, write_ratio, 1000 + c, t_start, t_end)
                                                for c in range(clients)])
            # sample server CPU time over the measured window only
            time.sleep(max(0, t_start + WARMUP - time.time()))
            cpu0 = server_cpu(srv.pid)
            time.sleep(max(0, t_end - time.time()))
            cpu1 = server_cpu(srv.pid)
            res = async_res.get()
    finally:
        stop(srv)
    lr = [x for r in res for x in r[0]]
    lw = [x for r in res for x in r[1]]
    ops = len(lr) + len(lw)
    return {"store": kind, "persistence": persistence if kind == "redis" else "n/a",
            "write_ratio": write_ratio, "clients": clients,
            "server_cpu_us_per_op": (cpu1 - cpu0) * 1e6 / max(ops, 1),
            "server_cpu_util": (cpu1 - cpu0) / DURATION,
            "throughput": ops / DURATION, "all": summarize(lr + lw), "read": summarize(lr),
            "write": summarize(lw), "misses": sum(r[2] for r in res), "errors": sum(r[3] for r in res)}


def sweep(name, configs, size):
    global KEYS, VALS
    KEYS, VALS = load_sessions(size)
    os.makedirs(OUT, exist_ok=True)
    rows = []
    for cfg in configs:
        for rep in range(REPS):
            r = run_one(**cfg)
            r.update({"size": size, "rep": rep})
            rows.append(r)
            print(f"[{name}] {size} {cfg} rep{rep}: {r['throughput']:.0f} ops/s "
                  f"p50={r['all']['p50']:.0f}us p99={r['all']['p99']:.0f}us err={r['errors']}", flush=True)
    return rows


def main(which):
    if which == "e1":
        cfgs = [dict(kind=k, write_ratio=w, clients=c)
                for w in (0.05, 0.30, 0.50) for c in (1, 4, 16, 32) for k in ("redis", "memcached")]
        rows = sweep("E1", cfgs, "medium")
    elif which == "e2":
        rows = []
        for size in ("small", "medium", "large"):
            rows += sweep("E2", [dict(kind=k, write_ratio=0.05, clients=16) for k in ("redis", "memcached")], size)
    elif which == "e3":
        cfgs = [dict(kind="redis", write_ratio=0.5, clients=16, persistence=p)
                for p in ("none", "rdb", "aof_everysec", "aof_always")]
        cfgs.append(dict(kind="memcached", write_ratio=0.5, clients=16))
        rows = sweep("E3", cfgs, "medium")
    with open(os.path.join(OUT, f"{which}.json"), "w") as f:
        json.dump(rows, f, indent=1)


if __name__ == "__main__":
    main(sys.argv[1])
