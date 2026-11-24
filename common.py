"""Shared helpers: start/stop servers, load sessions, build clients."""
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import time

import redis
from pymemcache.client.base import Client as MCClient

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
RUN = os.path.join(HERE, "run")
TTL = 1800  # 30-minute session TTL


def load_sessions(size="medium", n=None):
    keys, vals = [], []
    with open(os.path.join(DATA, f"sessions_{size}.jsonl")) as f:
        for i, line in enumerate(f):
            if n is not None and i >= n:
                break
            sid = json.loads(line)["session_id"]
            keys.append("sess:" + sid)
            vals.append(line.rstrip("\n").encode())
    return keys, vals


def wait_port(port, timeout=10):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open")


def start_redis(port=6379, persistence="none", extra=None, workdir=None):
    workdir = workdir or os.path.join(RUN, f"redis{port}")
    shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir)
    args = ["redis-server", "--port", str(port), "--dir", workdir, "--daemonize", "no",
            "--protected-mode", "no", "--loglevel", "warning", "--logfile", os.path.join(workdir, "log")]
    if persistence == "none":
        args += ["--save", "", "--appendonly", "no"]
    elif persistence == "rdb":
        # Redis 7 default snapshot policy
        args += ["--save", "3600 1 300 100 60 10000", "--appendonly", "no"]
    elif persistence == "aof_everysec":
        args += ["--save", "", "--appendonly", "yes", "--appendfsync", "everysec"]
    elif persistence == "aof_always":
        args += ["--save", "", "--appendonly", "yes", "--appendfsync", "always"]
    else:
        raise ValueError(persistence)
    args += extra or []
    p = subprocess.Popen(args)
    wait_port(port)
    return p


def start_memcached(port=11211, threads=4, mem_mb=4096):
    p = subprocess.Popen(["memcached", "-u", "root", "-p", str(port), "-U", "0", "-t", str(threads),
                          "-m", str(mem_mb), "-c", "4096"])
    wait_port(port)
    return p


def stop(p, sig=signal.SIGTERM):
    if p and p.poll() is None:
        p.send_signal(sig)
        try:
            p.wait(10)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


class Store:
    """Uniform session-store interface over Redis and Memcached."""

    def __init__(self, kind, port):
        self.kind = kind
        if kind == "redis":
            self.c = redis.Redis(host="127.0.0.1", port=port)
        else:
            self.c = MCClient(("127.0.0.1", port), no_delay=True)

    def get(self, k):
        return self.c.get(k)

    def set(self, k, v):
        if self.kind == "redis":
            self.c.set(k, v, ex=TTL)
        else:
            self.c.set(k, v, expire=TTL, noreply=False)

    def preload(self, keys, vals):
        if self.kind == "redis":
            pipe = self.c.pipeline(transaction=False)
            for i, (k, v) in enumerate(zip(keys, vals)):
                pipe.set(k, v, ex=TTL)
                if i % 1000 == 999:
                    pipe.execute()
            pipe.execute()
        else:
            for i in range(0, len(keys), 500):
                self.c.set_many(dict(zip(keys[i:i + 500], vals[i:i + 500])), expire=TTL, noreply=False)


def hot_index(n, rng):
    """80/20 hotspot: 80% of requests hit the most active 20% of sessions."""
    if rng.random() < 0.8:
        return rng.randrange(0, n // 5)
    return rng.randrange(n // 5, n)
