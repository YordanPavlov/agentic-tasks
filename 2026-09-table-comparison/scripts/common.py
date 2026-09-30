"""Shared helpers for old-vs-new table comparison: ClickHouse access, table metadata, hash expressions,
window planning. Every tier imports from here; per-table settings live in configs.py.
"""
import subprocess, os, json, time, math, datetime as D
from configs import CONFIGS

CACHE = os.environ.get('TCMP_CACHE', os.path.expanduser('~/.cache/table-cmp'))
HOST = os.environ.get('TCMP_HOST', 'clickhouse.production.san')
TARGET_ROWS = 60_000_000  # rows per Tier 1 query: large enough to amortize overhead, small enough for ~30 s
FLOAT_SHIFT = 20          # keeps ~32 mantissa bits: ULP noise lands in the same bucket, >4.7e-10 rel changes do not
TOL = 1e-9
LOG = json.dumps({'job': 'table-compare', 'owner': 'yordan.p@santiment.net', 'team': 'bigdata',
                  'repo': 'clickhouse-tables', 'dag': 'manual-tests'})


def ch(sql, fmt='TSV', timeout=1800):
    # --format overrides a FORMAT clause in the query, so the format is always passed here
    for attempt in range(3):
        r = subprocess.run(['clickhouse-client', '-h', HOST, '--port', '30900', '-u', 'readonly',
                            f'--log_comment={LOG}', f'--max_execution_time={timeout}', f'--format={fmt}', '--query', sql],
                           capture_output=True, text=True)
        if r.returncode == 0: return r.stdout
        if 'Code: 62' in r.stderr or 'Code: 47' in r.stderr or 'Code: 43' in r.stderr: break  # syntax/identifier/type: no retry
        time.sleep(5)
    raise RuntimeError(f'{r.stderr[:800]}\n--- query ---\n{sql[:1500]}')


def cfg_dir(name, *sub):
    p = os.path.join(CACHE, name, *sub); os.makedirs(p, exist_ok=True); return p


def load_cfg(name):
    c = dict(block=None, tx=None, key='new', exclude=[], split_by=[], where='1', where_old='1', where_new='1',
             agg_checks={}, start=None, cutoff=None)
    c.update(CONFIGS[name]); c['name'] = name
    return c


# ---------- metadata ----------

def shard_table(dist):
    """(db, table) of the local table behind a Distributed table; the table itself if not Distributed."""
    db, t = dist.split('.', 1)
    eng = ch(f"SELECT engine_full FROM system.tables WHERE database='{db}' AND name='{t}'").strip()
    if not eng: raise RuntimeError(f'table {dist} not found')
    if not eng.startswith('Distributed('): return db, t
    args = [a.strip(" '\\") for a in eng[eng.index('(') + 1:eng.rindex(')')].split(',')]
    return args[1], args[2]


def cluster_shards(dist):
    db, t = dist.split('.', 1)
    eng = ch(f"SELECT engine_full FROM system.tables WHERE database='{db}' AND name='{t}'").strip()
    if not eng.startswith('Distributed('): return 1
    cl = eng[eng.index('(') + 1:].split(',')[0].strip(" '\\")
    return int(ch(f"SELECT uniqExact(shard_num) FROM system.clusters WHERE cluster='{cl}'").strip())


def table_meta(dist):
    db, t = shard_table(dist)
    eng, key = ch(f"SELECT engine, sorting_key FROM system.tables WHERE database='{db}' AND name='{t}'").strip().split('\t')
    cols = {}
    for l in ch(f"SELECT name, type, default_kind FROM system.columns WHERE database='{db}' AND table='{t}' "
                f"ORDER BY position").splitlines():
        n, ty, kind = (l.split('\t') + [''])[:3]
        if kind != 'ALIAS': cols[n] = ty
    return dict(shard=f'{db}.{t}', engine=eng, key=[k.strip() for k in key.split(',')], cols=cols)


def meta(cfg):
    """Resolved comparison metadata, cached per config: key, value columns, schema differences."""
    path = os.path.join(cfg_dir(cfg['name']), 'meta.json')
    if os.path.exists(path): return json.load(open(path))
    o, n = table_meta(cfg['old']), table_meta(cfg['new'])
    common = [c for c in n['cols'] if c in o['cols']]
    m = dict(old=o, new=n, exclude=cfg['exclude'],
             only_old=[c for c in o['cols'] if c not in n['cols']], only_new=[c for c in n['cols'] if c not in o['cols']],
             type_diff={c: [o['cols'][c], n['cols'][c]] for c in common if o['cols'][c] != n['cols'][c]},
             shards=max(cluster_shards(cfg['old']), cluster_shards(cfg['new'])))
    m = rekey(m, cfg['key'])
    json.dump(m, open(path, 'w'), indent=1)
    return m


def rekey(m, key):
    """Copy of meta with another comparison key ('new' | 'old' | list of expressions); value columns follow."""
    k = {'new': m['new']['key'], 'old': m['old']['key']}[key] if isinstance(key, str) else key
    vals = [c for c in m['new']['cols'] if c in m['old']['cols'] and c not in k and c not in m['exclude']]
    return dict(m, key=k, vals=vals)


def base_type(ty):
    """(base type, nullable) with LowCardinality/Nullable wrappers removed."""
    nullable = False
    while True:
        if ty.startswith('LowCardinality('): ty = ty[15:-1]
        elif ty.startswith('Nullable('): ty = ty[9:-1]; nullable = True
        else: return ty, nullable


# ---------- hash expressions ----------

def canon(col, m, truncate):
    """NULL-safe, cross-side comparable hash argument(s) for one column.
    cityHash64 returns NULL if any argument is NULL, hence isNull()+ifNull(); types that differ between the
    sides are hashed as strings; floats are hashed as Float64 bits, optionally truncated by FLOAT_SHIFT."""
    ty = m['new']['cols'][col]
    base, nullable = base_type(ty)
    if col in m['type_diff'] and 'Float' not in base:
        return f"ifNull(toString({col}), '<NULL>')"
    if nullable:
        dflt = '0' if base.startswith(('Float', 'Int', 'UInt', 'Decimal')) else f"defaultValueOfTypeName('{base}')"
        v = f'ifNull({col}, {dflt})'
    else:
        v = col
    if base.startswith('Float'):
        v = f'reinterpretAsUInt64(toFloat64({v}))'
        if truncate: v = f'bitShiftRight({v}, {FLOAT_SHIFT})'
    return f'isNull({col}), {v}' if nullable else v


def key_expr(m):
    return 'cityHash64(' + ', '.join(canon(c, m, False) if c in m['new']['cols'] else c for c in m['key']) + ')'


def vals_expr(m):
    """Arguments of the content hash: every compared value column, floats truncated."""
    return ', '.join(canon(c, m, True) for c in m['vals'])


def side_where(cfg, side, lo, hi, extra='1'):
    return (f"{cfg['dt']} >= '{lo}' AND {cfg['dt']} < '{hi}' AND ({cfg['where']}) AND ({cfg['where_' + side]}) "
            f"AND ({extra})")


# ---------- window plan ----------

def month_rows(cfg, m):
    """Estimated rows per month per side from system.parts of the local shard x number of shards."""
    rows = {}
    for side in ('old', 'new'):
        db, t = m[side]['shard'].split('.', 1)
        rows[side] = {}
        for l in ch(f"SELECT toStartOfMonth(min_time) mo, sum(rows) FROM system.parts WHERE database='{db}' AND table='{t}' "
                    f"AND active GROUP BY mo ORDER BY mo").splitlines():
            mo, n = l.split('\t'); rows[side][mo] = int(n) * m['shards']
    return rows


def plan_windows(cfg, m, lo, hi):
    """Consecutive-day windows of ~TARGET_ROWS (max over sides); small months are merged into one window."""
    mr = month_rows(cfg, m)
    per_day = lambda d: max(mr[s].get(str(d.replace(day=1)), 0) for s in mr) / 30.4
    wins, d, start, acc = [], lo, lo, 0.0
    while d < hi:
        acc += per_day(d); d += D.timedelta(days=1)
        if acc >= TARGET_ROWS or d == hi:
            wins.append((str(start), str(d))); start, acc = d, 0.0
    return wins


def windows(cfg):
    """Window plan, fixed once per config so cached per-window results stay valid across restarts."""
    path = os.path.join(cfg_dir(cfg['name']), 'windows.json')
    if os.path.exists(path): return [tuple(w) for w in json.load(open(path))]
    wins = plan_windows(cfg, meta(cfg), D.date.fromisoformat(cfg['start']), D.date.fromisoformat(cfg['cutoff']))
    json.dump(wins, open(path, 'w'))
    return wins
