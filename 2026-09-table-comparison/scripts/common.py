"""Shared pieces of the old-vs-new table comparison: ClickHouse access, column metadata, SQL generation from
the templates in sql/, and the per-month result cache. README.md describes the method.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import TypedDict

from configs import CONFIGS, Config

HOST = os.environ.get('TCMP_HOST', 'clickhouse.production.san')
PORT = os.environ.get('TCMP_PORT', '30900')
USER = os.environ.get('TCMP_USER', 'readonly')
CACHE = Path(os.environ.get('TCMP_CACHE', '~/.cache/table-cmp')).expanduser()
SQL_DIR = Path(__file__).parent / 'sql'

TOL = 1e-9          # relative tolerance for float columns
MAX_KEYS = 10_000   # key hashes kept per (shard, day, category) for drill-down and the cross-shard check
LOG_COMMENT = json.dumps({'job': 'table-compare', 'owner': 'yordan.p@santiment.net', 'team': 'bigdata',
                          'repo': 'clickhouse-tables', 'dag': 'manual-tests'})

# Categories that mean "no difference"; everything else needs explaining.
OK_CATEGORIES = ('equal', 'float_noise')
# Network errors (209 timeout, 210 connection refused, 32 unexpected EOF) are retried; anything else is final.
RETRYABLE_CODES = {'32', '209', '210'}


# ---------- ClickHouse access ----------

def ch(sql: str, fmt: str = 'TSV', timeout: int = 1800) -> str:
    """Run one query with clickhouse-client and return its output. The format is passed as an option,
    because --format overrides a FORMAT clause anyway."""
    cmd = ['clickhouse-client', '-h', HOST, '--port', PORT, '-u', USER, f'--log_comment={LOG_COMMENT}',
           f'--max_execution_time={timeout}', '--output_format_json_quote_64bit_integers=0',
           f'--format={fmt}', '--query', sql]
    for attempt in range(3):
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode == 0:
            return r.stdout
        code = re.search(r'Code: (\d+)', r.stderr)
        if not code or code.group(1) not in RETRYABLE_CODES:
            break
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f'{r.stderr[:800]}\n--- query ---\n{sql[:2000]}')


def ch_json(sql: str, timeout: int = 1800) -> list[dict[str, object]]:
    return [json.loads(line) for line in ch(sql, 'JSONEachRow', timeout).splitlines()]


# ---------- columns ----------

@dataclass(frozen=True)
class Column:
    """One key or value column, with its type on each side."""
    name: str
    old_type: str
    new_type: str

    @property
    def base(self) -> str:
        return strip_type(self.new_type)[0]

    @property
    def nullable(self) -> bool:
        return strip_type(self.old_type)[1] or strip_type(self.new_type)[1]

    @property
    def is_float(self) -> bool:
        return any(strip_type(t)[0].startswith('Float') for t in (self.old_type, self.new_type))

    @property
    def same_type(self) -> bool:
        return strip_type(self.old_type)[0] == strip_type(self.new_type)[0]


def strip_type(ty: str) -> tuple[str, bool]:
    """(base type, nullable) with the LowCardinality and Nullable wrappers removed."""
    nullable = False
    while True:
        if ty.startswith('LowCardinality('):
            ty = ty[len('LowCardinality('):-1]
        elif ty.startswith('Nullable('):
            ty, nullable = ty[len('Nullable('):-1], True
        else:
            return ty, nullable


def table_types(table: str) -> dict[str, str]:
    db, name = table.split('.', 1)
    out = ch(f"SELECT name, type FROM system.columns WHERE database = '{db}' AND table = '{name}'")
    return dict(line.split('\t') for line in out.splitlines())


@lru_cache(maxsize=None)  # one metadata lookup per config and process
def columns(cfg: Config) -> tuple[list[Column], list[Column]]:
    """(key columns, value columns) with types from both tables; fails if a listed column is missing."""
    old, new = table_types(cfg.old), table_types(cfg.new)
    if not old or not new:
        raise RuntimeError(f'table not found: {cfg.old if not old else cfg.new}')
    missing = [c for c in cfg.keys + cfg.values if c not in old or c not in new]
    if missing:
        raise RuntimeError(f'columns missing on one side: {missing}')
    def col(c: str) -> Column:
        return Column(c, old[c], new[c])
    return [col(c) for c in cfg.keys], [col(c) for c in cfg.values]


# ---------- SQL expressions ----------

def hash_args(c: Column) -> str:
    """Argument(s) that hash the same on both sides. Hash functions return NULL for a NULL argument, hence
    isNull() + ifNull(); a column whose type differs between the sides is hashed as text (floats as Float64)."""
    if c.is_float and not c.same_type:
        v = f'toFloat64({c.name})'
    elif not c.same_type:
        return f"ifNull(toString({c.name}), '<NULL>')"
    else:
        v = c.name
    if c.nullable:
        return f"isNull({c.name}), ifNull({v}, defaultValueOfTypeName('{c.base}'))"
    return v


def float_value(c: Column) -> str:
    """A float column as Float64; NULL becomes 0 here and is told apart by isNull() in the value hash."""
    return f'toFloat64(ifNull({c.name}, 0))' if c.nullable else f'toFloat64({c.name})'


def key_hash(keys: list[Column]) -> str:
    # 128 bits, so collisions are negligible even for billions of keys
    return 'sipHash128(' + ', '.join(hash_args(c) for c in keys) + ')'


def row_exprs(keys: list[Column], values: list[Column], dt_col: str) -> str:
    """Layer 1 of compare.sql: the shape every row of both tables is reduced to."""
    plain = [hash_args(c) for c in values if not c.is_float]
    plain += [f'isNull({c.name})' for c in values if c.is_float and c.nullable]
    floats = [c for c in values if c.is_float]
    exprs = [
        f'{key_hash(keys)} AS cmp_kh',
        f'toDate({dt_col}) AS cmp_day',
        f"cityHash64({', '.join(plain)}) AS cmp_vh" if plain else 'toUInt64(0) AS cmp_vh',
        *[f'{float_value(c)} AS cmp_f_{c.name}' for c in floats],
        'cityHash64(' + ', '.join(['cmp_vh'] + [f'reinterpretAsUInt64(cmp_f_{c.name})' for c in floats])
        + ') AS cmp_ah',
    ]
    return ',\n                   '.join(exprs)


def float_aggs(values: list[Column]) -> str:
    """Layer 2 of compare.sql: each float's value per side (only used once both sides have one version)."""
    return ''.join(f',\n            minIf(cmp_f_{c.name}, cmp_side = 0) AS o_f_{c.name},'
                   f'\n            minIf(cmp_f_{c.name}, cmp_side = 1) AS n_f_{c.name}'
                   for c in values if c.is_float)


def floats_within_tol(values: list[Column]) -> str:
    """Layer 3 of compare.sql: every float equal, both NaN, or within TOL of each other relative to the larger."""
    checks = [f'(o_f_{n} = n_f_{n} OR (isNaN(o_f_{n}) AND isNaN(n_f_{n})) '
              f'OR abs(o_f_{n} - n_f_{n}) <= {TOL} * greatest(abs(o_f_{n}), abs(n_f_{n})))'
              for n in (c.name for c in values if c.is_float)]
    return '\n            AND '.join(checks) if checks else '1'


def window_filter(cfg: Config, w: Window) -> str:
    return f"{cfg.dt} >= '{w.lo}' AND {cfg.dt} < '{w.hi}'"


def template(name: str) -> Template:
    return Template((SQL_DIR / name).read_text())


def on_every_shard(cfg: Config, sql: str) -> str:
    """Wrap a query over local tables so that it runs where the data is. For a sharded table, cluster(view(...))
    sends the whole query to one replica of each shard and the caller gets the shards' results concatenated."""
    if cfg.cluster is None:
        return f'SELECT * FROM (\n{sql}\n)'
    return f"SELECT * FROM cluster('{cfg.cluster}', view(\n{sql}\n))"


def compare_sql(cfg: Config, w: Window) -> str:
    keys, values = columns(cfg)
    inner = template('compare.sql').substitute(
        max_keys=MAX_KEYS, row_exprs=row_exprs(keys, values, cfg.dt), float_aggs=float_aggs(values),
        floats_within_tol=floats_within_tol(values), old=cfg.old, new=cfg.new, window=window_filter(cfg, w),
        where=cfg.where, where_old=cfg.where_old, where_new=cfg.where_new)
    return on_every_shard(cfg, inner)


def rows_sql(cfg: Config, w: Window, hashes: list[str]) -> str:
    keys, values = columns(cfg)
    inner = template('rows.sql').substitute(
        key_hash=key_hash(keys), columns=', '.join(c.name for c in keys + values), old=cfg.old, new=cfg.new,
        window=window_filter(cfg, w), where=cfg.where, where_old=cfg.where_old, where_new=cfg.where_new,
        hashes=', '.join(f"'{h}'" for h in hashes))
    return on_every_shard(cfg, inner) + '\nORDER BY key_hash, side, host'


# ---------- windows and cache ----------

@dataclass(frozen=True)
class Window:
    """[lo, hi) in whole days; one calendar month, cut to the configured start and cutoff."""
    lo: dt.date
    hi: dt.date

    @property
    def name(self) -> str:
        return self.lo.strftime('%Y-%m')


def windows(cfg: Config) -> list[Window]:
    start, cutoff = dt.date.fromisoformat(cfg.start), dt.date.fromisoformat(cfg.cutoff)
    out, lo = [], start
    while lo < cutoff:
        next_month = (lo.replace(day=1) + dt.timedelta(days=32)).replace(day=1)
        out.append(Window(lo, min(next_month, cutoff)))
        lo = next_month
    return out


def window_of(cfg: Config, day: str) -> Window:
    d = dt.date.fromisoformat(day)
    return next(w for w in windows(cfg) if w.lo <= d < w.hi)


def load_cfg(name: str) -> Config:
    if name not in CONFIGS:
        raise SystemExit(f'unknown config {name!r}; known: {", ".join(CONFIGS)}')
    return CONFIGS[name]


def fingerprint(cfg: Config) -> str:
    """Changes whenever the config, the SQL templates or the tolerance change; cached results with another
    fingerprint are stale."""
    h = hashlib.sha256(repr((cfg, TOL, MAX_KEYS)).encode())
    for p in sorted(SQL_DIR.glob('*.sql')):
        h.update(p.read_bytes())
    return h.hexdigest()[:16]


class ResultRow(TypedDict):
    """One row of compare.sql: the keys of one category on one day on one shard."""
    host: str
    day: str
    category: str
    keys: int
    old_rows: int
    new_rows: int
    sample: list[str]


class MonthResult(TypedDict):
    fingerprint: str
    lo: str
    hi: str
    elapsed_s: float
    rows: list[ResultRow]


def result_path(name: str, w: Window) -> Path:
    return CACHE / name / f'{w.name}.json'


def load_result(name: str, cfg: Config, w: Window) -> MonthResult | None:
    """The cached result of a window, or None if it is missing or stale."""
    p = result_path(name, w)
    if not p.exists():
        return None
    r: MonthResult = json.loads(p.read_text())
    return r if r['fingerprint'] == fingerprint(cfg) else None


def save_result(name: str, w: Window, r: MonthResult) -> None:
    p = result_path(name, w)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix('.tmp')
    tmp.write_text(json.dumps(r))
    tmp.replace(p)  # atomic, so an interrupted run never leaves a half-written result
