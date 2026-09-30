"""Tier 2: per-key diff of old vs new for Tier 1's failing days.
Per key: distinct truncated-content hashes per side, then a category; for value diffs, max relative
float diff (exact values) and non-float column mismatch counts. Resumable per day.

usage: tier2.py <config> [day ...]    (default: all days in tier1_fails.json)
"""
import os, sys, json, time, datetime as D
from tier1 import CONFIGS, CACHE, ch, build_sql, shard_table

TOL = 1e-9
SAMPLES = 5


def columns(cfg):
    db, t = shard_table(cfg['new'])
    key = ch(f"SELECT sorting_key FROM system.tables WHERE database='{db}' AND name='{t}'").strip()
    kcols = [c.strip() for c in key.split(',')]
    cols = [l.split('\t') for l in ch(f"SELECT name, type FROM system.columns WHERE database='{db}' AND table='{t}'").splitlines()]
    vals = [(c, ty) for c, ty in cols if c not in kcols]
    return key, vals


def build(cfg):
    key, vals = columns(cfg)
    vh = build_sql(cfg).split('cityHash64(kh, ', 1)[1].split(') vh', 1)[0]
    floats = [c for c, ty in vals if 'Float' in ty]
    others = [c for c, ty in vals if 'Float' not in ty]
    # prefixed aliases: shadowing real column names would change the vh expression
    sel = ', '.join((f'toFloat64(ifNull({c}, 0)) v_{c}' if c in floats else f"ifNull(toString({c}), '<NULL>') v_{c}")
                    for c, _ in vals)
    per_key = ', '.join([f"anyIf(v_{c}, s='n') n_{c}, minIf(v_{c}, s='o') omin_{c}, maxIf(v_{c}, s='o') omax_{c}" for c in floats] +
                        [f"anyIf(v_{c}, s='n') n_{c}, groupUniqArrayIf(v_{c}, s='o') o_{c}" for c in others])
    # distance of new to the nearer old extreme; for single-version old keys that is the exact diff
    rel = lambda c: (f"least(abs(n_{c} - omin_{c}), abs(n_{c} - omax_{c})) / greatest(abs(n_{c}), abs(omin_{c}), abs(omax_{c}), 1e-300)")
    stats = ', '.join([f"max({rel(c)}) maxrel_{c}, countIf({rel(c)} > {TOL}) viol_{c}" for c in floats] +
                      [f"countIf(NOT has(o_{c}, n_{c})) mism_{c}" for c in others])
    cat = """multiIf(empty(nv), 'only_old', empty(ov), 'only_new', length(nv) > 1, 'new_conflict',
                   has(ov, nv[1]), 'old_conflict_new_matches', 'value_diff')"""
    return f"""
SELECT {cat} cat, count() keys, {stats}, groupArraySample({SAMPLES})(kh) samples
FROM (
  SELECT kh, groupUniqArrayIf(vh, s='o') ov, groupUniqArrayIf(vh, s='n') nv, {per_key}
  FROM (SELECT 'o' s, cityHash64({key}) kh, cityHash64(kh, {vh}) vh, {sel} FROM {cfg['old']} WHERE {{w}}
        UNION ALL
        SELECT 'n' s, cityHash64({key}) kh, cityHash64(kh, {vh}) vh, {sel} FROM {cfg['new']} WHERE {{w}})
  GROUP BY kh HAVING arraySort(ov) != arraySort(nv))
GROUP BY cat ORDER BY cat"""


if __name__ == '__main__':
    name = sys.argv[1]; cfg = CONFIGS[name]
    days = sys.argv[2:] or sorted(json.load(open(os.path.join(CACHE, name, 'tier1_fails.json'))))
    out = os.path.join(CACHE, name, 'tier2'); os.makedirs(out, exist_ok=True)
    sql = build(cfg)
    for i, d in enumerate(days):
        path = os.path.join(out, f'{d}.jsonl')
        if os.path.exists(path): continue
        nd = D.date.fromisoformat(d) + D.timedelta(days=1)
        t0 = time.time()
        res = ch(sql.format(w=f"{cfg['dt']} >= '{d}' AND {cfg['dt']} < '{nd}'"), fmt='JSONEachRow')
        open(path + '.tmp', 'w').write(res); os.rename(path + '.tmp', path)
        print(f'{i + 1}/{len(days)} {d} {time.time() - t0:.1f}s', flush=True)
        for l in res.splitlines(): print('   ', l)
