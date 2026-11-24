#!/usr/bin/env bash
# E1b: server-level comparison with a native C load generator (memtier_benchmark).
# Server pinned to CPU 0, load generator pinned to CPU 1 so client overhead does not
# compete with the server. 2,095-byte values (mean medium session), 20,000-key space,
# Gaussian key access, set:get ratios 1:19 (95/5), 3:7 (70/30), 1:1 (50/50).
set -u
cd "$(dirname "$0")"
OUT=results/memtier; mkdir -p "$OUT" run
REPS=${REPS:-3}
for ratio in "1:19" "3:7" "1:1"; do
  for conns in 1 4 16 50; do
    for store in redis memcached memcached_t1; do
      for rep in $(seq 1 "$REPS"); do
        if [ "$store" = redis ]; then
          taskset -c 0 redis-server --port 6395 --save "" --appendonly no --protected-mode no \
             --dir run --loglevel warning --daemonize no & PID=$!; PROTO=redis; PORT=6395
        elif [ "$store" = memcached ]; then
          taskset -c 0 memcached -u root -p 11295 -U 0 -t 4 -m 2048 & PID=$!; PROTO=memcache_text; PORT=11295
        else
          taskset -c 0 memcached -u root -p 11295 -U 0 -t 1 -m 2048 & PID=$!; PROTO=memcache_text; PORT=11295
        fi
        sleep 0.5
        # preload the keyspace
        taskset -c 1 memtier_benchmark -s 127.0.0.1 -p $PORT -P $PROTO --hide-histogram \
          -t 1 -c 10 --ratio 1:0 -d 2095 --key-minimum 1 --key-maximum 20000 --key-pattern P:P \
          --requests allkeys --expiry-range 1800-1800 > /dev/null 2>&1
        f="$OUT/${store}_r${ratio/:/-}_c${conns}_rep${rep}.json"
        taskset -c 1 memtier_benchmark -s 127.0.0.1 -p $PORT -P $PROTO --hide-histogram \
          -t 1 -c $conns --ratio $ratio -d 2095 --key-minimum 1 --key-maximum 20000 \
          --key-pattern G:G --test-time 8 --expiry-range 1800-1800 --json-out-file "$f" > /dev/null 2>&1
        echo "$store ratio=$ratio conns=$conns rep=$rep -> $(python3 -c "import json;d=json.load(open('$f'))['ALL STATS']['Totals'];print(round(d['Ops/sec']),round(d['Percentile Latencies']['p50.00'],3),round(d['Percentile Latencies']['p99.00'],3))" 2>&1)"
        kill $PID; wait $PID 2>/dev/null
      done
    done
  done
done
echo MEMTIER_DONE
