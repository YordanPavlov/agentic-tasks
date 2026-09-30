"""Aggregate Tier 2 per-day results by category: key counts, worst float diffs, tolerance violations, mismatches.

usage: tier2_summary.py <config>
"""
import os, sys, glob, json
from collections import defaultdict
from tier1 import CACHE

if __name__ == '__main__':
    name = sys.argv[1]
    agg = defaultdict(lambda: defaultdict(int)); days = defaultdict(list)
    for p in sorted(glob.glob(os.path.join(CACHE, name, 'tier2', '*.jsonl'))):
        d = os.path.basename(p)[:-6]
        for l in open(p):
            r = json.loads(l); a = agg[r['cat']]; days[r['cat']].append(d)
            for k, v in r.items():
                if k.startswith('maxrel_'): a[k] = max(a[k], float(v))
                elif k == 'keys' or k.startswith(('viol_', 'mism_')): a[k] += int(v)
    for cat, a in sorted(agg.items()):
        nz = {k: v for k, v in a.items() if v and k != 'keys'}
        ds = days[cat]
        print(f'{cat}: {a["keys"]:,} keys on {len(ds)} days ({ds[0]}..{ds[-1]})  {nz}')
