"""Compare cached Tier 1 results: classify each day, print a summary plus failing days as JSON (input to Tier 2).

usage: tier1_compare.py <config>
"""
import os, sys, glob, json
from collections import Counter
from tier1 import CACHE

COLS = ['rows', 'blocks', 'bmin', 'bmax', 'txs', 'keys', 'fp']


def load(name, side):
    days = {}
    for p in glob.glob(os.path.join(CACHE, name, 'tier1', f'{side}_*.tsv')):
        for l in open(p):
            d, *v = l.split('\t'); days[d] = dict(zip(COLS, map(int, v)))
    return days


def classify(o, n):
    if o is None: return ['only_new']
    if n is None: return ['only_old']
    c = []
    if (o['blocks'], o['bmin'], o['bmax']) != (n['blocks'], n['bmin'], n['bmax']): c.append('blocks')
    if o['txs'] != n['txs']: c.append('txs')
    if o['keys'] != n['keys']: c.append('keys')
    if o['fp'] != n['fp']: c.append('content')
    return c


if __name__ == '__main__':
    name = sys.argv[1]
    old, new = load(name, 'old'), load(name, 'new')
    fails, cats = {}, Counter()
    for d in sorted(old.keys() | new.keys()):
        c = classify(old.get(d), new.get(d))
        if c: fails[d] = dict(cats=c, old=old.get(d), new=new.get(d)); cats[' + '.join(c)] += 1
    tot = lambda s, k: sum(v[k] for v in s.values())
    print(f'days: old {len(old)}, new {len(new)}, differing {len(fails)}')
    for side, s in (('old', old), ('new', new)):
        print(f'{side}: rows {tot(s, "rows"):,}  keys {tot(s, "keys"):,}  dups {tot(s, "rows") - tot(s, "keys"):,}  '
              f'blocks {tot(s, "blocks"):,}  txs {tot(s, "txs"):,}')
    for k, v in cats.most_common(): print(f'  {v:6d}  {k}')
    for d, f in list(fails.items())[:30]:
        o, n = f['old'] or {}, f['new'] or {}
        diff = {k: (o.get(k), n.get(k)) for k in COLS[:-1] if o.get(k) != n.get(k) and k != 'rows'}
        print(f'  {d} {",".join(f["cats"])} {diff}')
    json.dump(fails, open(os.path.join(CACHE, name, 'tier1_fails.json'), 'w'), indent=1)
