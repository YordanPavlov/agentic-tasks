"""Tier 2 aggregate check: for tables where rows may legitimately differ while an aggregate must not
(e.g. stacks split differently but the net change per address and block is the same).
Per group of cfg['agg_checks'][check]['group']: sum of `value` over deduplicated keys per side, compared with
relative tolerance against the sum of absolute values (robust for sums near zero).

usage: tier2_agg.py <config> <check> [--sample N] [group ...]    (default: all groups in tier1_fails.json; --sample as in tier2.py)
"""
import os, sys, json, time
from common import load_cfg, cfg_dir, meta, ch, key_expr, side_where, TOL
from tier2 import group_where, select_groups

SAMPLES = 5


def build(cfg, m, chk):
    g = chk['group']
    sel = ', '.join(f'{c} g_{i}' for i, c in enumerate(g))
    dedup = ', '.join(f'any(g_{i}) g_{i}' for i in range(len(g)))
    gcols = ', '.join(f'g_{i}' for i in range(len(g)))
    side = lambda s, t: f"SELECT '{s}' s, {key_expr(m)} kh, {sel}, toFloat64({chk['value']}) v FROM {cfg[t]} WHERE {{w_{t}}}"
    rel = 'abs(o - n) / greatest(ao, an, 1e-300)'
    return f"""
SELECT count() groups, countIf(ko = 0) only_new_groups, countIf(kn = 0) only_old_groups,
       countIf({rel} > {TOL}) viol, max({rel}) maxrel, sum(o) sum_old, sum(n) sum_new,
       groupArraySampleIf({SAMPLES})(toString(tuple({gcols}, o, n)), {rel} > {TOL}) samples
FROM (
  SELECT {gcols}, sumIf(v, s='o') o, sumIf(v, s='n') n, sumIf(abs(v), s='o') ao, sumIf(abs(v), s='n') an,
         countIf(s='o') ko, countIf(s='n') kn
  FROM (SELECT s, kh, {dedup}, any(v) v
        FROM ({side('o', 'old')}
              UNION ALL
              {side('n', 'new')})
        GROUP BY s, kh)
  GROUP BY {gcols})"""


if __name__ == '__main__':
    cfg = load_cfg(sys.argv[1]); check = sys.argv[2]; m = meta(cfg)
    groups = select_groups(cfg, sys.argv[3:])
    out = cfg_dir(cfg['name'], f'tier2_agg-{check}')
    sql = build(cfg, m, cfg['agg_checks'][check])
    for i, g in enumerate(groups):
        path = os.path.join(out, f"{g.replace('|', '__')}.jsonl")
        if os.path.exists(path): continue
        lo, hi, extra = group_where(cfg, g)
        t0 = time.time()
        res = ch(sql.format(w_old=side_where(cfg, 'old', lo, hi, extra), w_new=side_where(cfg, 'new', lo, hi, extra)),
                 fmt='JSONEachRow')
        open(path + '.tmp', 'w').write(res); os.rename(path + '.tmp', path)
        print(f'{i + 1}/{len(groups)} {g} {time.time() - t0:.1f}s  {res.strip()}', flush=True)
