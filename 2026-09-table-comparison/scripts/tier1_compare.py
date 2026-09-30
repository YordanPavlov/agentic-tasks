"""Compare cached Tier 1 results: classify each group (day [+ split_by]), print totals and failing groups,
write tier1_fails.json (input to Tier 2).

usage: tier1_compare.py <config>
"""
import os, sys, glob, json
from collections import Counter
from common import load_cfg, cfg_dir
from tier1 import COLS


def load(cfg, side):
    groups, nsplit = {}, len(cfg['split_by'])
    for p in glob.glob(os.path.join(cfg_dir(cfg['name'], 'tier1'), f'{side}_*.tsv')):
        for l in open(p):
            f = l.rstrip('\n').split('\t'); g = '|'.join(f[:1 + nsplit])
            if g in groups: print(f'!! {side} {g} appears in more than one cached window: stale cache? ({p})')
            groups[g] = dict(zip(COLS, map(int, f[1 + nsplit:])))
    return groups


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
    cfg = load_cfg(sys.argv[1])
    old, new = load(cfg, 'old'), load(cfg, 'new')
    fails, cats = {}, Counter()
    for g in sorted(old.keys() | new.keys()):
        c = classify(old.get(g), new.get(g))
        if c: fails[g] = dict(cats=c, old=old.get(g), new=new.get(g)); cats[' + '.join(c)] += 1
    tot = lambda s, k: sum(v[k] for v in s.values())
    print(f'groups: old {len(old)}, new {len(new)}, differing {len(fails)}')
    for side, s in (('old', old), ('new', new)):
        print(f'{side}: rows {tot(s, "rows"):,}  keys {tot(s, "keys"):,}  dups {tot(s, "rows") - tot(s, "keys"):,}  '
              f'blocks {tot(s, "blocks"):,}  txs {tot(s, "txs"):,}')
    print(f'key diff new-old: {tot(new, "keys") - tot(old, "keys"):+,}  block diff: {tot(new, "blocks") - tot(old, "blocks"):+,}')
    for k, v in cats.most_common(): print(f'  {v:6d}  {k}')
    for g, f in list(fails.items())[:30]:
        o, n = f['old'] or {}, f['new'] or {}
        diff = {k: (o.get(k), n.get(k)) for k in COLS[:-1] if o.get(k) != n.get(k) and k != 'rows'}
        print(f'  {g} {",".join(f["cats"])} rows {o.get("rows")}/{n.get("rows")} keys {o.get("keys")}/{n.get("keys")} {diff}')
    if len(fails) > 30: print(f'  ... {len(fails) - 30} more in tier1_fails.json')
    json.dump(fails, open(os.path.join(cfg_dir(cfg['name']), 'tier1_fails.json'), 'w'), indent=1)
