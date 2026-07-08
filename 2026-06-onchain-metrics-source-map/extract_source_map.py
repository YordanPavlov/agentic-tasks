#!/usr/bin/env python3
"""
Derivation/verification tool for the on-chain metrics source map.

Parses the metric dependency graph straight from the repo specs and computes
the set of metrics that (transitively) depend on the legacy `*_stacks` source.
Use this to RE-DERIVE the map when specs change, and to sanity-check the
hand-encoded edges in build_metrics_map.py.

Run from the clickhouse-tables repo root:
    python3 extract_source_map.py                 # summary + full blue set
    python3 extract_source_map.py mvrv_usd nvt    # print canonical parents of given metrics

Source of truth: daily_metrics/specs.d/metrics/**/*.yaml  ->  spec.dependsOn (+ formula).

CAVEAT (important): spec.dependsOn is NOT complete for source attribution.
Some BASE metrics (graph leaves) are produced by jobs that read a source table
directly without declaring it in dependsOn (e.g. realized_value_usd, realized_profit,
address_profit). Those are coin-age/price metrics and are effectively `*_stacks` too,
but they will show up here as uncolored leaves -- classify leaves by their producing
job's source table, not by dependsOn alone. The pivotal blue leaf, age_distribution_5min_delta,
was verified by hand:
    job_functions/distribution_deltas.py  reads  config 'distribution_deltas_table'
        -> default 'distribution_deltas_5min'  (built from *_stacks: eth_stacks/erc20_stacks)
        -> after the stacks->age-balances migration the same metric is backed by
           job_functions/age_balances_base.py (i.e. it would flip blue -> green).
"""
import glob, re, sys
from collections import defaultdict, deque
import yaml

SPEC_GLOB = 'daily_metrics/specs.d/metrics/**/*.yaml'

# leaves that are produced directly from the legacy coin model (verified by job inspection)
STACKS_LEAVES = {
    'age_distribution_5min_delta',
    'age_distribution_5min_queues_delta',
    'stakers_age_distribution_5min_delta',
    'stack_age_consumed',
    'stack_price_consumed',
}

def strip_ver(n: str) -> str:
    return n.split('/')[0]

WIN = r'(_(1d|7d|14d|30d|60d|90d|180d|365d|730d|1y|2y|3y|5y|7y|8y|9y|10y|20y|1h|4h|8h|24h|48h|5min|10min|1day|1week))+$'
BUCKET = r'_(1|1e_?\d+|all|inf)$'
def canon(n: str) -> str:
    n = re.sub(WIN, '', n)
    n = re.sub(BUCKET, '_B', n)
    return re.sub(WIN, '', n)

def parse():
    deps = defaultdict(set)
    formula = {}
    for f in glob.glob(SPEC_GLOB, recursive=True):
        try:
            docs = list(yaml.safe_load_all(open(f)))
        except Exception as e:
            print(f'YAML error {f}: {e}', file=sys.stderr); continue
        for d in docs:
            if not d or d.get('kind') != 'Metric':
                continue
            name = strip_ver((d.get('metadata', {}) or {}).get('name', '') or '')
            if not name:
                continue
            spec = d.get('spec', {}) or {}
            for x in (spec.get('dependsOn', []) or []):
                if isinstance(x, str):
                    deps[name].add(strip_ver(x))
                elif isinstance(x, dict) and x.get('metric'):
                    deps[name].add(strip_ver(x['metric']))
            deps.setdefault(name, deps[name])
            if spec.get('formula'):
                formula[name] = spec['formula']
    return deps, formula

def blue_set(deps):
    radj = defaultdict(set)
    for m, ds in deps.items():
        for d in ds:
            radj[d].add(m)
    seen, q = set(), deque(STACKS_LEAVES)
    while q:
        for m in radj[q.popleft()]:
            if m not in seen:
                seen.add(m); q.append(m)
    return seen

def main():
    deps, formula = parse()
    if len(sys.argv) > 1:                      # print canonical parents of requested metrics
        cdeps = defaultdict(set)
        for m, ds in deps.items():
            for d in ds:
                if canon(d) != canon(m):
                    cdeps[canon(m)].add(canon(d))
        for t in sys.argv[1:]:
            print(f'{t}  <-  {sorted(cdeps.get(canon(t), [])) or "(leaf / produced directly by a source job)"}')
        return
    nodes = set(deps) | {d for v in deps.values() for d in v}
    bs = blue_set(deps)
    print(f'metric specs: nodes={len(nodes)} composites={sum(1 for n in formula)} '
          f'leaves={sum(1 for n in nodes if not deps.get(n))}')
    print(f'stacks-dependent: raw={len(bs)} canonical={len({canon(b) for b in bs})}')
    for b in sorted({canon(b) for b in bs}):
        print('  blue:', b)

if __name__ == '__main__':
    main()
