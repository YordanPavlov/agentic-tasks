"""Aggregate per-group Tier 2 results: per category key counts, worst float diffs, tolerance violations, mismatches.
Also summarizes tier2_agg-* results.

usage: tier2_summary.py <config> [subdir]    subdir: tier2 (default), tier2_key-old, tier2_agg-<check>
"""
import os, sys, glob, json
from collections import defaultdict
from common import load_cfg, cfg_dir


def num(v):
    try: return float(v)
    except (TypeError, ValueError): return 0.0


if __name__ == '__main__':
    cfg = load_cfg(sys.argv[1]); sub = sys.argv[2] if len(sys.argv) > 2 else 'tier2'
    files = sorted(glob.glob(os.path.join(cfg_dir(cfg['name'], sub), '*.jsonl')))
    print(f'{sub}: {len(files)} groups')
    agg = defaultdict(lambda: defaultdict(float)); groups = defaultdict(list)
    for p in files:
        g = os.path.basename(p)[:-6].replace('__', '|')
        for l in open(p):
            r = json.loads(l); cat = r.get('cat', 'agg'); a = agg[cat]; groups[cat].append(g)
            for k, v in r.items():
                if k in ('cat', 'samples'): continue
                if k.startswith('max'): a[k] = max(a[k], num(v))
                else: a[k] += num(v)
            if cat == 'agg' and num(r.get('viol')): print(f"  {g}: viol {r['viol']} samples {r['samples']}")
    for cat, a in sorted(agg.items()):
        nz = {k: (f'{v:.3g}' if k.startswith('max') or k.startswith('sum_') else f'{int(v):,}') for k, v in a.items() if v}
        gs = groups[cat]
        print(f'{cat}: on {len(gs)} groups ({gs[0]}..{gs[-1]})  {nz}')
