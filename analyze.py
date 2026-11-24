"""Aggregate results/*.json into summary tables (results/summary.json) and 300-dpi figures (figures/)."""
import glob
import json
import os
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(HERE, "results")
F = os.path.join(HERE, "figures")
os.makedirs(F, exist_ok=True)

C = {"redis": "#2a78d6", "memcached": "#eb6834", "memcached_t1": "#1baf7a"}
M = {"redis": "o", "memcached": "s", "memcached_t1": "^"}
LS = {"redis": "-", "memcached": "--", "memcached_t1": ":"}
LBL = {"redis": "Redis 7.0", "memcached": "Memcached 1.6 (4 threads)", "memcached_t1": "Memcached 1.6 (1 thread)"}
plt.rcParams.update({"font.family": "DejaVu Serif", "font.size": 9, "axes.spines.top": False,
                     "axes.spines.right": False, "axes.grid": True, "grid.color": "#e3e3e3",
                     "grid.linewidth": 0.6, "axes.edgecolor": "#888888", "legend.frameon": False,
                     "lines.linewidth": 2, "lines.markersize": 6, "figure.dpi": 300})
summary = {}


def load(name):
    p = os.path.join(R, f"{name}.json")
    return json.load(open(p)) if os.path.exists(p) else None


def ms(vals):
    a = np.asarray(vals, dtype=float)
    return float(a.mean()), float(a.std(ddof=1)) if a.size > 1 else 0.0


# ---------------- E1 application level
e1 = load("e1")
if e1:
    g = defaultdict(list)
    for r in e1:
        g[(r["store"], r["write_ratio"], r["clients"])].append(r)
    tab = []
    for (s, w, c), rs in sorted(g.items()):
        tab.append({"store": s, "write_ratio": w, "clients": c,
                    "thr": ms([r["throughput"] for r in rs]),
                    "p50": ms([r["all"]["p50"] for r in rs]), "p95": ms([r["all"]["p95"] for r in rs]),
                    "p99": ms([r["all"]["p99"] for r in rs]), "p999": ms([r["all"]["p999"] for r in rs]),
                    "read_p50": ms([r["read"]["p50"] for r in rs]),
                    "write_p50": ms([r["write"]["p50"] for r in rs]),
                    "read_p99": ms([r["read"]["p99"] for r in rs]),
                    "write_p99": ms([r["write"]["p99"] for r in rs]),
                    "cpu_op": ms([r["server_cpu_us_per_op"] for r in rs]),
                    "cpu_util": ms([r["server_cpu_util"] for r in rs]),
                    "errors": sum(r["errors"] for r in rs), "misses": sum(r["misses"] for r in rs)})
    summary["e1"] = tab
    fig, axes = plt.subplots(2, 3, figsize=(7.2, 4.6), sharex=True)
    for j, w in enumerate((0.05, 0.30, 0.50)):
        for s in ("redis", "memcached"):
            rows = [t for t in tab if t["store"] == s and t["write_ratio"] == w]
            x = [t["clients"] for t in rows]
            axes[0, j].errorbar(x, [t["thr"][0] / 1000 for t in rows], yerr=[t["thr"][1] / 1000 for t in rows],
                                color=C[s], marker=M[s], ls=LS[s], capsize=2, label=LBL[s])
            axes[1, j].errorbar(x, [t["p99"][0] / 1000 for t in rows], yerr=[t["p99"][1] / 1000 for t in rows],
                                color=C[s], marker=M[s], ls=LS[s], capsize=2)
        axes[0, j].set_title(f"{int(round((1 - w) * 100))}% reads / {int(round(w * 100))}% writes")
        axes[1, j].set_xscale("log", base=2)
        axes[1, j].set_xticks([1, 4, 16, 32]); axes[1, j].set_xticklabels(["1", "4", "16", "32"])
        axes[1, j].set_xlabel("Concurrent clients")
        axes[0, j].set_ylim(bottom=0); axes[1, j].set_ylim(bottom=0)
    axes[0, 0].set_ylabel("Throughput (k ops/s)")
    axes[1, 0].set_ylabel("p99 latency (ms)")
    axes[0, 0].legend(loc="upper left", fontsize=7.5)
    fig.tight_layout()
    fig.savefig(os.path.join(F, "fig1_e1_app.png")); plt.close(fig)

# ---------------- E1b memtier server level
mt = glob.glob(os.path.join(R, "memtier", "*.json"))
if mt:
    g = defaultdict(list)
    for p in mt:
        b = os.path.basename(p)[:-5]
        store, rest = b.split("_r", 1)
        ratio, conns, rep = rest.split("_")
        tot = json.load(open(p))["ALL STATS"]["Totals"]
        pl = tot["Percentile Latencies"]
        g[(store, ratio, int(conns[1:]))].append((tot["Ops/sec"], pl["p50.00"], pl["p99.00"], pl.get("p99.90", np.nan)))
    tab = []
    for (s, ratio, c), v in sorted(g.items()):
        a = np.array(v)
        tab.append({"store": s, "ratio": ratio, "conns": c, "thr": ms(a[:, 0]), "p50": ms(a[:, 1]),
                    "p99": ms(a[:, 2]), "p999": ms(a[:, 3])})
    summary["e1b"] = tab
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.6), sharey=True)
    titles = {"1-19": "95% reads / 5% writes", "3-7": "70% reads / 30% writes", "1-1": "50% reads / 50% writes"}
    for j, ratio in enumerate(("1-19", "3-7", "1-1")):
        for s in ("redis", "memcached", "memcached_t1"):
            rows = sorted([t for t in tab if t["store"] == s and t["ratio"] == ratio], key=lambda t: t["conns"])
            axes[j].errorbar([t["conns"] for t in rows], [t["thr"][0] / 1000 for t in rows],
                             yerr=[t["thr"][1] / 1000 for t in rows], color=C[s], marker=M[s], ls=LS[s],
                             capsize=2, label=LBL[s])
        axes[j].set_title(titles[ratio]); axes[j].set_xscale("log", base=2)
        axes[j].set_xticks([1, 4, 16, 50]); axes[j].set_xticklabels(["1", "4", "16", "50"])
        axes[j].set_xlabel("Connections"); axes[j].set_ylim(bottom=0)
    axes[0].set_ylabel("Throughput (k ops/s)")
    axes[0].legend(loc="lower right", fontsize=7)
    fig.tight_layout(); fig.savefig(os.path.join(F, "fig2_e1b_server.png")); plt.close(fig)

# ---------------- E2 size
e2 = load("e2")
if e2:
    g = defaultdict(list)
    for r in e2:
        g[(r["store"], r["size"])].append(r)
    tab = [{"store": s, "size": z, "thr": ms([r["throughput"] for r in rs]),
            "read_p50": ms([r["read"]["p50"] for r in rs]), "read_p99": ms([r["read"]["p99"] for r in rs]),
            "write_p50": ms([r["write"]["p50"] for r in rs]), "write_p99": ms([r["write"]["p99"] for r in rs]),
            "cpu_op": ms([r["server_cpu_us_per_op"] for r in rs])} for (s, z), rs in g.items()]
    summary["e2"] = tab

# ---------------- E3 persistence
e3 = load("e3")
if e3:
    g = defaultdict(list)
    for r in e3:
        key = "memcached" if r["store"] == "memcached" else r["persistence"]
        g[key].append(r)
    order = ["memcached", "none", "rdb", "aof_everysec", "aof_always"]
    tab = [{"config": k, "thr": ms([r["throughput"] for r in g[k]]),
            "write_p50": ms([r["write"]["p50"] for r in g[k]]), "write_p99": ms([r["write"]["p99"] for r in g[k]]),
            "read_p50": ms([r["read"]["p50"] for r in g[k]]), "read_p99": ms([r["read"]["p99"] for r in g[k]]),
            "write_p999": ms([r["write"]["p999"] for r in g[k]]),
            "cpu_op": ms([r["server_cpu_us_per_op"] for r in g[k]])} for k in order if k in g]
    summary["e3"] = tab
    names = {"memcached": "Memcached", "none": "Redis\nno pers.", "rdb": "Redis\nRDB",
             "aof_everysec": "Redis\nAOF\neverysec", "aof_always": "Redis\nAOF\nalways"}
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9))
    x = np.arange(len(tab))
    cols = [C["memcached"] if t["config"] == "memcached" else C["redis"] for t in tab]
    axes[0].bar(x, [t["thr"][0] / 1000 for t in tab], yerr=[t["thr"][1] / 1000 for t in tab], color=cols,
                width=0.6, capsize=2, edgecolor="white", linewidth=2)
    axes[0].set_ylabel("Throughput (k ops/s)")
    w = 0.36
    axes[1].bar(x - w / 2, [t["write_p50"][0] / 1000 for t in tab], w, color=cols, alpha=0.55,
                edgecolor="white", linewidth=1.5)
    axes[1].bar(x + w / 2, [t["write_p99"][0] / 1000 for t in tab], w, color=cols,
                edgecolor="white", linewidth=1.5, hatch="///")
    from matplotlib.patches import Patch
    axes[1].legend(handles=[Patch(facecolor="#9a9a9a", alpha=0.55, label="write p50 (light)"),
                            Patch(facecolor="#6b6b6b", hatch="///", edgecolor="white", label="write p99 (hatched)")],
                   fontsize=7, loc="upper left")
    axes[1].set_ylabel("Write latency (ms)")
    for ax in axes:
        ax.set_xticks(x); ax.set_xticklabels([names[t["config"]] for t in tab], fontsize=7)
        ax.grid(axis="x", visible=False)
    fig.tight_layout(); fig.savefig(os.path.join(F, "fig3_e3_persistence.png")); plt.close(fig)

# ---------------- E4-E7 pass-through + aggregates
e4 = load("memory")
if e4:
    summary["e4"] = e4
    fig, ax = plt.subplots(figsize=(3.6, 2.5))
    sizes = ["small", "medium", "large"]
    x = np.arange(3); w = 0.36
    for k, s in enumerate(("redis", "memcached")):
        vals = [next(r for r in e4 if r["store"] == s and r["size"] == z)["rss_per_session"] for z in sizes]
        ax.bar(x + (k - 0.5) * w, vals, w, color=C[s], label=LBL[s].split(" (")[0], edgecolor="white", linewidth=1.5)
    raw = [np.mean([r["raw_bytes"] / r["n"] for r in e4 if r["size"] == z]) for z in sizes]
    ax.scatter(x, raw, marker="_", s=300, color="#333333", zorder=3, label="Raw key+value size")
    ax.set_xticks(x); ax.set_xticklabels(["0.57 KB", "2.1 KB", "8.2 KB"]); ax.set_xlabel("Session size class")
    ax.set_ylabel("Resident memory per session (bytes)"); ax.legend(fontsize=7); ax.grid(axis="x", visible=False)
    fig.tight_layout(); fig.savefig(os.path.join(F, "fig4_e4_memory.png")); plt.close(fig)

for name, key, gk in (("crash", "e5", "config"), ("failover", "e6", "system"), ("cart", "e7", "mode")):
    d = load(name)
    if not d:
        continue
    g = defaultdict(list)
    for r in d:
        g[r[gk]].append(r)
    agg = []
    for k, rs in g.items():
        row = {gk: k, "reps": len(rs)}
        for f, v in rs[0].items():
            if isinstance(v, (int, float)) and f not in ("rep",) and all(r.get(f) is not None for r in rs):
                row[f] = ms([r[f] for r in rs])
        agg.append(row)
    summary[key] = agg
    summary[key + "_raw"] = d

e6 = summary.get("e6")
if e6:
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.5))
    order = [s for s in ("redis_sentinel", "redis_sentinel_wait1", "memcached_3node") if any(r["system"] == s for r in e6)]
    names = {"redis_sentinel": "Redis + Sentinel", "redis_sentinel_wait1": "Redis + Sentinel\n(WAIT 1)",
             "memcached_3node": "Memcached\n3-node ring"}
    rows = [next(r for r in e6 if r["system"] == s) for s in order]
    cols = [C["memcached"] if "memcached" in s else C["redis"] for s in order]
    x = np.arange(len(order))
    for i, r in enumerate(rows):
        if "downtime_s" in r:
            axes[0].bar(i, r["downtime_s"][0], yerr=r["downtime_s"][1], color=cols[i], width=0.55, capsize=2)
            lbl = f"{r['downtime_s'][0]:.1f} s"
            if "memcached" in order[i]:
                lbl = "<0.1 s overall\n(~2 s for keys on\nthe failed node)"
            axes[0].text(i, r["downtime_s"][0] + r["downtime_s"][1] + 0.3, lbl, ha="center", fontsize=7)
        else:  # writes never acknowledged again within the 10 s window
            axes[0].bar(i, 10, color="white", edgecolor=cols[i], hatch="///", width=0.55, linewidth=1.2)
            axes[0].text(i, 10.3, "not restored\n(>10 s)", ha="center", fontsize=7)
    axes[0].set_ylim(0, 12.5)
    axes[0].set_ylabel("Write unavailability (s)")
    axes[1].bar(x, [r["preloaded_sessions_lost_pct"][0] for r in rows],
                yerr=[r["preloaded_sessions_lost_pct"][1] for r in rows], color=cols, width=0.55, capsize=2)
    axes[1].set_ylabel("Existing sessions lost (%)")
    for i, r in enumerate(rows):
        v = r["preloaded_sessions_lost_pct"][0]
        axes[1].text(i, v + 0.8, f"{v:.1f}%", ha="center", fontsize=7)
    axes[1].set_ylim(0, 40)
    for ax in axes:
        ax.set_xticks(x); ax.set_xticklabels([names[s] for s in order], fontsize=7); ax.grid(axis="x", visible=False)
    fig.tight_layout(); fig.savefig(os.path.join(F, "fig5_e6_failover.png")); plt.close(fig)

json.dump(summary, open(os.path.join(R, "summary.json"), "w"), indent=1)
print("summary keys:", list(summary))
