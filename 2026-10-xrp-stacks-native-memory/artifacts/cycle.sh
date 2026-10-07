#!/usr/bin/env bash
# One in-place restart of xrp-stacks-v7: set every vertex's parallelism to $1 via the
# adaptive scheduler's resource-requirements API, wait until all tasks run, settle, measure.
set -euo pipefail
P=${1:?parallelism}; SETTLE=${2:-600}
D=$(cd "$(dirname "$0")" && pwd)
K="kubectl --kubeconfig $HOME/.kube/config_hprod -n flink"
JM=$($K get pods --no-headers | awk '/^xrp-stacks-v7-[0-9a-f]+-/{print $1; exit}')

$K exec -i -c flink-main-container "$JM" -- python3 - "$P" <<'PY'
import json, sys, urllib.request
p = int(sys.argv[1])
base = 'http://localhost:8081'
job = next(j for j in json.load(urllib.request.urlopen(base + '/jobs'))['jobs'] if j['status'] == 'RUNNING')['id']
req = json.load(urllib.request.urlopen(f'{base}/jobs/{job}/resource-requirements'))
body = {v: {'parallelism': {'lowerBound': 1, 'upperBound': p}} for v in req}
r = urllib.request.Request(f'{base}/jobs/{job}/resource-requirements', data=json.dumps(body).encode(),
                           method='PUT', headers={'Content-Type': 'application/json'})
print('PUT', urllib.request.urlopen(r).status, body)
PY

# wait (up to 15 min) until the stack vertex runs at P with all subtasks RUNNING
for _ in $(seq 90); do
  sleep 10
  ok=$($K exec -i -c flink-main-container "$JM" -- python3 - "$P" <<'PY'
import json, sys, urllib.request
p = int(sys.argv[1]); base = 'http://localhost:8081'
jobs = json.load(urllib.request.urlopen(base + '/jobs'))['jobs']
job = next((j['id'] for j in jobs if j['status'] == 'RUNNING'), None)
if not job: print('no'); sys.exit()
d = json.load(urllib.request.urlopen(f'{base}/jobs/{job}'))
v = next(v for v in d['vertices'] if 'stack' in v['name'])
print('yes' if v['parallelism'] == p and v['tasks'].get('RUNNING', 0) == p else 'no')
PY
)
  [ "$ok" = yes ] && break
done
echo "running at p=$P ($ok), settling ${SETTLE}s"
sleep "$SETTLE"
"$D/measure.sh" "$D/samples.tsv" "after-p$P"
