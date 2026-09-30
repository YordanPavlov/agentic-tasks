"""Tier 1: per-day, dedup-invariant guardrails of old vs new table over the full history.
Key = shard table's sorting key; value columns = all other columns (floats truncated to ~2e-10 rel).
One worker per side (2 queries in flight). Resumable: one cached TSV per (side, window).

usage: tier1.py <config> [lo hi]      e.g. tier1.py xrp_balances 2023-06-01 2023-07-01
"""
import subprocess, os, sys, math, json, time, datetime as D
from concurrent.futures import ThreadPoolExecutor

CONFIGS = {
    'xrp_balances': dict(old='default.xrp_balances', new='test.xrp_balances_test',
                         dt='dt', block='blockNumber', tx='transactionHash',
                         start='2013-01-01', cutoff='2026-09-26'),
}
CACHE = os.environ.get('TIER1_CACHE', os.path.expanduser('~/.cache/rerun-cmp'))
TARGET_ROWS = 60_000_000
FLOAT_SHIFT = 20  # keeps ~32 mantissa bits: ULP noise lands in the same bucket
LOG = json.dumps({'job': 'rerun-compare-tier1', 'owner': 'yordan.p@santiment.net', 'team': 'bigdata',
                  'repo': 'clickhouse-tables', 'dag': 'manual-tests'})


def ch(sql, fmt='TSV'):
    for attempt in range(3):
        r = subprocess.run(['clickhouse-client', '-h', 'clickhouse.production.san', '--port', '30900', '-u', 'readonly',
                            f'--log_comment={LOG}', '--max_execution_time=1800', f'--format={fmt}', '--query', sql],
                           capture_output=True, text=True)
        if r.returncode == 0: return r.stdout
        time.sleep(5)
    raise RuntimeError(f'{r.stderr[:500]}\n{sql[:300]}')


def split(t): return t.split('.', 1)


def shard_table(dist):
    db, t = split(dist)
    eng = ch(f"SELECT engine_full FROM system.tables WHERE database='{db}' AND name='{t}'").strip()
    args = [a.strip(" '\\") for a in eng[eng.index('(') + 1:eng.rindex(')')].split(',')]
    return args[1], args[2]


def hash_expr(col, typ):
    """Canonical, NULL-safe hash argument(s); cityHash64 returns NULL if any argument is NULL."""
    nullable = typ.startswith('Nullable(')
    base = typ[9:-1] if nullable else typ
    v = f'ifNull({col}, {"0" if base.startswith(("Float", "Int", "UInt")) else "defaultValueOfTypeName(%r)" % base})' if nullable else col
    if base.startswith('Float'):
        v = f'bitShiftRight(reinterpretAsUInt64(toFloat64({v})), {FLOAT_SHIFT})'
    return f'isNull({col}), {v}' if nullable else v


def build_sql(cfg):
    db, t = shard_table(cfg['new'])
    key = ch(f"SELECT sorting_key FROM system.tables WHERE database='{db}' AND name='{t}'").strip()
    kcols = [c.strip() for c in key.split(',')]
    cols = [l.split('\t') for l in ch(f"SELECT name, type FROM system.columns WHERE database='{db}' AND table='{t}'").splitlines()]
    vals = ', '.join(hash_expr(c, ty) for c, ty in cols if c not in kcols)
    return f"""
SELECT toDate({cfg['dt']}) d, count() rows, uniqExact({cfg['block']}) blocks, min({cfg['block']}) bmin, max({cfg['block']}) bmax,
       uniqExact(cityHash64({cfg['tx']})) txs, uniqExact(kh) keys, sumDistinct(vh) fp
FROM (SELECT {cfg['dt']}, {cfg['block']}, {cfg['tx']}, cityHash64({key}) kh, cityHash64(kh, {vals}) vh
      FROM {{table}} WHERE {cfg['dt']} >= '{{lo}}' AND {cfg['dt']} < '{{hi}}')
GROUP BY d ORDER BY d FORMAT TSV"""


def tier0(cfg):
    """Rows per month from system.parts of the local shard (x3 shards), max over sides."""
    rows = {}
    for side in ('old', 'new'):
        db, t = shard_table(cfg[side])
        for l in ch(f"SELECT toStartOfMonth(min(min_time)), sum(rows) FROM system.parts WHERE database='{db}' AND table='{t}' "
                    f"AND active GROUP BY partition").splitlines():
            m, n = l.split('\t'); rows[m] = max(rows.get(m, 0), int(n) * 3)
    return rows


def windows(cfg, lo, hi):
    mr = tier0(cfg); d = lo
    while d < hi:
        nm = (d.replace(day=28) + D.timedelta(days=4)).replace(day=1)
        end = min(nm, hi); days = (nm - d.replace(day=1)).days
        step = max(1, math.floor(days * TARGET_ROWS / max(mr.get(str(d.replace(day=1)), 1), 1)))
        while d < end:
            e = min(d + D.timedelta(days=step), end); yield d, e; d = e


def run_side(name, cfg, side, wins, sql):
    out = os.path.join(CACHE, name, 'tier1'); os.makedirs(out, exist_ok=True)
    t_start = time.time()
    for i, (lo, hi) in enumerate(wins):
        path = os.path.join(out, f'{side}_{lo}_{hi}.tsv')
        if os.path.exists(path): continue
        t0 = time.time()
        res = ch(sql.format(table=cfg[side], lo=lo, hi=hi))
        open(path + '.tmp', 'w').write(res); os.rename(path + '.tmp', path)
        print(f'{side} {i + 1}/{len(wins)} {lo}..{hi} {time.time() - t0:.1f}s (total {time.time() - t_start:.0f}s)', flush=True)
    return time.time() - t_start


if __name__ == '__main__':
    name = sys.argv[1]; cfg = CONFIGS[name]
    lo = D.date.fromisoformat(sys.argv[2] if len(sys.argv) > 2 else cfg['start'])
    hi = D.date.fromisoformat(sys.argv[3] if len(sys.argv) > 3 else cfg['cutoff'])
    sql = build_sql(cfg); wins = list(windows(cfg, lo, hi))
    print(f'{len(wins)} windows per side', flush=True)
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {s: ex.submit(run_side, name, cfg, s, wins, sql) for s in ('old', 'new')}
        for s, f in futs.items(): print(f'{s} done in {f.result():.0f}s', flush=True)
