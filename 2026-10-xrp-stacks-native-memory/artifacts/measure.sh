#!/usr/bin/env bash
# One sample of xrp-stacks-v7 native memory: RSS split (heap vs native anon) on the TM,
# plus live ForSt cache/memtables from the JM REST metrics. Appends a TSV line to $1.
set -euo pipefail
OUT=${1:?tsv path}; LABEL=${2:-sample}
K="kubectl --kubeconfig $HOME/.kube/config_hprod -n flink"
TM=$($K get pods --no-headers | awk '/^xrp-stacks-v7-taskmanager/{print $1; exit}')
JM=$($K get pods --no-headers | awk '/^xrp-stacks-v7-[0-9a-f]+-/{print $1; exit}')

# heap/native anon RSS in MiB, from smaps with the G1 heap range taken from jcmd
read -r HEAP NATIVE RSS < <($K exec -i "$TM" -- python3 - <<'PY'
import re, subprocess
hi = subprocess.run(['jcmd', '1', 'GC.heap_info'], capture_output=True, text=True).stdout
m = re.search(r'\[(0x[0-9a-f]+), (0x[0-9a-f]+)\)', hi); lo, h = int(m.group(1), 16), int(m.group(2), 16)
tot = {'heap': 0, 'native': 0}; cur = None
for l in open('/proc/1/smaps'):
    m = re.match(r'^([0-9a-f]+)-([0-9a-f]+) \S+ \S+ \S+ \S+\s*(.*)$', l)
    if m:
        s, e = int(m.group(1), 16), int(m.group(2), 16)
        cur = 'heap' if s >= lo and e <= h else (None if m.group(3).startswith('/') else 'native')
    elif l.startswith('Rss:') and cur:
        tot[cur] += int(l.split()[1])
rss = int([l for l in open('/proc/1/status') if l.startswith('VmRSS')][0].split()[1])
print(tot['heap'] // 1024, tot['native'] // 1024, rss // 1024)
PY
)

# job state, parallelism, and ForSt cache + memtables (MiB, summed over subtasks). With
# PER_CF=1 (unmanaged ForSt memory) every column family has its own cache, so all are summed;
# otherwise they share one per-slot cache and report the same value.
read -r STATE PAR CACHE MEMT < <($K exec -i -c flink-main-container "$JM" -- python3 - "${PER_CF:-0}" <<'PY'
import json, sys, urllib.request
CFS = ('account-head', 'account-change-store', 'block-buffer', 'drained-watermark')
cache_cfs = CFS if sys.argv[1] == '1' else CFS[:1]
get = lambda p: json.load(urllib.request.urlopen('http://localhost:8081' + p))
job = next(j for j in get('/jobs')['jobs'] if j['status'] in ('RUNNING', 'RESTARTING', 'CREATED'))
d = get('/jobs/' + job['id'])
v = next(v for v in d['vertices'] if 'stack' in v['name'])
cache = memt = 0
for s in range(v['parallelism']):
    ids = ','.join([f'Create_stack_changes.{cf}.rocksdb_block-cache-usage' for cf in cache_cfs]
                   + [f'Create_stack_changes.{cf}.rocksdb_cur-size-all-mem-tables' for cf in CFS])
    for m in get(f"/jobs/{job['id']}/vertices/{v['id']}/subtasks/{s}/metrics?get={ids}"):
        val = int(float(m['value'])) // 2**20
        if 'block-cache-usage' in m['id']: cache += val
        else: memt += val
print(d['state'], v['parallelism'], cache, memt)
PY
)

UNEXPLAINED=$((NATIVE - CACHE - MEMT))
[ -s "$OUT" ] || printf 'time\tlabel\tstate\tpar\trss_mib\theap_mib\tnative_mib\tcache_mib\tmemt_mib\tnative_minus_forst_mib\n' > "$OUT"
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -u +%H:%M:%S)" "$LABEL" "$STATE" "$PAR" \
  "$RSS" "$HEAP" "$NATIVE" "$CACHE" "$MEMT" "$UNEXPLAINED" >> "$OUT"
tail -1 "$OUT"
