"""Tier 2: per-key diff of old vs new on Tier 1's failing groups (one query per group, resumable).
Per key: distinct truncated-content hashes per side -> category; for keys whose content differs: max relative
float diff (exact values) and tolerance violations per float column, mismatch count per other column.

Categories: only_old, only_new, new_conflict (new has >1 version), old_conflict_new_matches (old has >1
version, one equals new: benign duplicate), value_diff (single versions that differ).

usage: tier2.py <config> [--key old|new] [--sample N] [group ...]    (default: all groups in tier1_fails.json)
  --sample N: every failing group with a blocks/txs/keys difference plus N content-only groups spread evenly
  over the history (for when nearly every day fails on content).
  --key old re-keys the comparison by the old table's sorting key (see "Key changes" in table-comparison.md);
  results then go to tier2_key-old/.
"""
import os, sys, json, time, datetime as D
from common import load_cfg, cfg_dir, meta, rekey, ch, key_expr, vals_expr, side_where, TOL

SAMPLES = 5


def group_where(cfg, g):
    """(lo, hi, extra filter) for a Tier 1 group id 'YYYY-MM-DD[|split values]'."""
    d, *sv = g.split('|')
    extra = ' AND '.join([f"toString({c}) = '{v}'" for c, v in zip(cfg['split_by'], sv)]) or '1'
    return d, str(D.date.fromisoformat(d) + D.timedelta(days=1)), extra


def select_groups(cfg, args):
    """Groups from the command line, else from tier1_fails.json, optionally sampled (--sample N)."""
    n = None
    if '--sample' in args: i = args.index('--sample'); n = int(args[i + 1]); del args[i:i + 2]
    if args: return args
    fails = json.load(open(os.path.join(cfg_dir(cfg['name']), 'tier1_fails.json')))
    if n is None: return sorted(fails)
    hard = sorted(g for g, f in fails.items() if f['cats'] != ['content'])
    soft = sorted(g for g, f in fails.items() if f['cats'] == ['content'])
    step = max(1, len(soft) / n) if n else 0
    return hard + ([soft[int(i * step)] for i in range(min(n, len(soft)))] if n else [])


def build(cfg, m):
    floats = [c for c in m['vals'] if 'Float' in m['new']['cols'][c]]
    others = [c for c in m['vals'] if c not in floats]
    # prefixed aliases: shadowing real column names would change the hash expression
    sel = ', '.join((f'toFloat64(ifNull({c}, 0)) v_{c}' if c in floats else f"ifNull(toString({c}), '<NULL>') v_{c}")
                    for c in m['vals']) or '1 v__'
    per_key = ', '.join([f"anyIf(v_{c}, s='n') n_{c}, minIf(v_{c}, s='o') omin_{c}, maxIf(v_{c}, s='o') omax_{c}" for c in floats] +
                        [f"anyIf(v_{c}, s='n') n_{c}, groupUniqArrayIf(v_{c}, s='o') o_{c}" for c in others]) or '1 x__'
    # distance of new to the nearer old extreme; for single-version old keys that is the exact diff
    rel = lambda c: f"least(abs(n_{c} - omin_{c}), abs(n_{c} - omax_{c})) / greatest(abs(n_{c}), abs(omin_{c}), abs(omax_{c}), 1e-300)"
    stats = ', '.join([f"max({rel(c)}) maxrel_{c}, countIf({rel(c)} > {TOL}) viol_{c}" for c in floats] +
                      [f"countIf(NOT has(o_{c}, n_{c})) mism_{c}" for c in others]) or '0 x__'
    cat = """multiIf(empty(nv), 'only_old', empty(ov), 'only_new', length(nv) > 1, 'new_conflict',
                   has(ov, nv[1]), 'old_conflict_new_matches', 'value_diff')"""
    # float stats are only meaningful for keys present on both sides; for only_* categories they are 0/NaN
    side = lambda s, t: (f"SELECT '{s}' s, {key_expr(m)} kh, cityHash64(kh, {vals_expr(m) or '0'}) vh, {sel} "
                         f"FROM {cfg[t]} WHERE {{w_{t}}}")
    return f"""
SELECT {cat} cat, count() keys, {stats}, groupArraySample({SAMPLES})(kh) samples
FROM (
  SELECT kh, groupUniqArrayIf(vh, s='o') ov, groupUniqArrayIf(vh, s='n') nv, {per_key}
  FROM ({side('o', 'old')}
        UNION ALL
        {side('n', 'new')})
  GROUP BY kh HAVING arraySort(ov) != arraySort(nv))
GROUP BY cat ORDER BY cat"""


if __name__ == '__main__':
    args = sys.argv[2:]; key = None
    if '--key' in args: i = args.index('--key'); key = args[i + 1]; del args[i:i + 2]
    cfg = load_cfg(sys.argv[1]); m = meta(cfg)
    if key: m = rekey(m, key)
    groups = select_groups(cfg, args)
    out = cfg_dir(cfg['name'], f'tier2_key-{key}' if key else 'tier2')
    sql = build(cfg, m)
    for i, g in enumerate(groups):
        path = os.path.join(out, f"{g.replace('|', '__')}.jsonl")
        if os.path.exists(path): continue
        lo, hi, extra = group_where(cfg, g)
        t0 = time.time()
        res = ch(sql.format(w_old=side_where(cfg, 'old', lo, hi, extra), w_new=side_where(cfg, 'new', lo, hi, extra)),
                 fmt='JSONEachRow')
        open(path + '.tmp', 'w').write(res); os.rename(path + '.tmp', path)
        print(f'{i + 1}/{len(groups)} {g} {time.time() - t0:.1f}s', flush=True)
        for l in res.splitlines(): print('   ', l)
