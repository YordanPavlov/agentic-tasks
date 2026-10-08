"""Shared pieces of the old-vs-new table comparison: ClickHouse access, column metadata, SQL generation from
the templates in sql/, the windows and the result cache. README.md describes the method.
"""
from __future__ import annotations

import datetime
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
CACHE_DIR = Path(os.environ.get('TCMP_CACHE', '~/.cache/table-cmp')).expanduser()
SQL_DIR = Path(__file__).parent / 'sql'

FLOAT_TOLERANCE = 1e-9   # relative
MAX_KEY_HASHES = 10_000  # kept per (window, shard, day, category) for drill-down and the cross-shard check
# Rows per shard and window, both sides as on disk. A query takes ~350 bytes per row (XRP balances), ~7 GiB here.
WINDOW_ROWS = int(os.environ.get('TCMP_WINDOW_ROWS', 20_000_000))
LOG_COMMENT = json.dumps({'job': 'table-compare', 'owner': 'yordan.p@santiment.net', 'team': 'bigdata',
                          'repo': 'clickhouse-tables', 'dag': 'manual-tests'})

# Categories that mean "no difference"; everything else needs explaining.
OK_CATEGORIES = ('equal', 'float_noise', 'soft_diff')
# Network errors (209 timeout, 210 connection refused, 32 unexpected EOF) are retried; anything else is final.
RETRYABLE_ERROR_CODES = {'32', '209', '210'}


# ---------- ClickHouse access ----------

def run_query(sql: str, output_format: str = 'TSV', timeout_s: int = 1800) -> str:
    """Run one query with clickhouse-client and return its output. The format is passed as an option,
    because --format overrides a FORMAT clause anyway."""
    command = ['clickhouse-client', '-h', HOST, '--port', PORT, '-u', USER, f'--log_comment={LOG_COMMENT}',
               f'--max_execution_time={timeout_s}', '--output_format_json_quote_64bit_integers=0',
               f'--format={output_format}',
               # sources select *, which would leave out MATERIALIZED and ALIAS columns
               '--asterisk_include_materialized_columns=1', '--asterisk_include_alias_columns=1',
               '--query', sql]
    for attempt in range(3):
        process = subprocess.run(command, capture_output=True, text=True)
        if process.returncode == 0:
            return process.stdout
        error_code = re.search(r'Code: (\d+)', process.stderr)
        if not error_code or error_code.group(1) not in RETRYABLE_ERROR_CODES:
            break
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f'{process.stderr[:800]}\n--- query ---\n{sql[:2000]}')


def run_query_json(sql: str, timeout_s: int = 1800) -> list[dict[str, object]]:
    return [json.loads(line) for line in run_query(sql, 'JSONEachRow', timeout_s).splitlines()]


# ---------- columns ----------

@dataclass(frozen=True)
class Column:
    """One key or value column, with its type on each side."""
    name: str
    old_type: str
    new_type: str

    @property
    def base_type(self) -> str:
        return strip_type(self.new_type)[0]

    @property
    def nullable(self) -> bool:
        return strip_type(self.old_type)[1] or strip_type(self.new_type)[1]

    @property
    def is_float(self) -> bool:
        return any(strip_type(type_name)[0].startswith('Float') for type_name in (self.old_type, self.new_type))

    @property
    def same_type(self) -> bool:
        return strip_type(self.old_type)[0] == strip_type(self.new_type)[0]


def strip_type(type_name: str) -> tuple[str, bool]:
    """(base type, nullable) with the LowCardinality and Nullable wrappers removed."""
    nullable = False
    while True:
        if type_name.startswith('LowCardinality('):
            type_name = type_name[len('LowCardinality('):-1]
        elif type_name.startswith('Nullable('):
            type_name, nullable = type_name[len('Nullable('):-1], True
        else:
            return type_name, nullable


def source_types(config: Config, side: str) -> dict[str, str]:
    """Column name -> type of what one side's source returns; it runs on the connected host, which needs the
    local tables."""
    sql = f'DESCRIBE (\n{source_sql(config, side, config.start, config.start, "0")}\n)'
    return dict(line.split('\t')[:2] for line in run_query(sql).splitlines())


@lru_cache(maxsize=None)  # one metadata lookup per config and process
def columns(config: Config) -> tuple[list[Column], list[Column], list[Column]]:
    """(key, value, soft-value columns) with types from both sources; fails if a listed column is missing."""
    old_types, new_types = source_types(config, 'old'), source_types(config, 'new')
    names = config.keys + config.values + config.soft_values
    missing = [name for name in names if name not in old_types or name not in new_types]
    if missing:
        raise RuntimeError(f'columns missing on one side: {missing}')

    def column(name: str) -> Column:
        return Column(name, old_types[name], new_types[name])
    return ([column(name) for name in config.keys], [column(name) for name in config.values],
            [column(name) for name in config.soft_values])


# ---------- SQL generation ----------
# The SQL templates in sql/ hold the structure; Python only fills in their $placeholders from the config.

def hash_args(column: Column) -> str:
    """Argument(s) that hash the same on both sides. Hash functions return NULL for a NULL argument, hence
    isNull() + ifNull(); a column whose type differs between the sides is hashed as text (floats as Float64)."""
    if column.is_float and not column.same_type:
        value = f'toFloat64({column.name})'
    elif not column.same_type:
        return f"ifNull(toString({column.name}), '<NULL>')"
    else:
        value = column.name
    if column.nullable:
        return f"isNull({column.name}), ifNull({value}, defaultValueOfTypeName('{column.base_type}'))"
    return value


def float_value(column: Column) -> str:
    """A float column as Float64; NULL becomes 0 here and is told apart by isNull() in the values hash."""
    return f'toFloat64(ifNull({column.name}, 0))' if column.nullable else f'toFloat64({column.name})'


def source_sql(config: Config, side: str, start: str, end: str, key_range: str) -> str:
    """The rows of one side ('old' or 'new') in [start, end) and the key range, from the config's source."""
    table, where = (config.old, config.where_old) if side == 'old' else (config.new, config.where_new)
    return Template((SQL_DIR / 'sources' / config.source).read_text()).substitute(
        table=table, dt=config.dt, start=start, end=end, key_range=key_range,
        where=f'({config.where}) AND ({where})')


def placeholders(config: Config, start: str, end: str, key_range: str) -> dict[str, str]:
    """The $placeholders shared by the templates: the sources, column lists and the partition column."""
    keys, values, soft_values = columns(config)
    floats = [column for column in values if column.is_float]
    exact = [hash_args(column) for column in values if not column.is_float]
    exact += [f'isNull({column.name})' for column in floats if column.nullable]
    return dict(
        old_source=source_sql(config, 'old', start, end, key_range),
        new_source=source_sql(config, 'new', start, end, key_range),
        dt=config.dt,
        key_columns=', '.join(hash_args(column) for column in keys),
        exact_value_columns=', '.join(exact) or '0',
        float_value_columns=', '.join(float_value(column) for column in floats),
        soft_value_columns=', '.join(hash_args(column) for column in soft_values) or '0',
        columns=', '.join(column.name for column in keys + values + soft_values))


def from_template(file_name: str, config: Config, values: dict[str, str]) -> str:
    """A template with its placeholders filled in, wrapped so that it runs where the data is.
    For a sharded table, cluster(view(...)) sends the whole query to one replica of each shard, and the caller
    gets the shards' results concatenated."""
    sql = Template((SQL_DIR / file_name).read_text()).substitute(values)
    if config.cluster is None:
        return f'SELECT * FROM (\n{sql}\n)'
    return f"SELECT * FROM cluster('{config.cluster}', view(\n{sql}\n))"


def compare_sql(config: Config, window: Window) -> str:
    return from_template('compare.sql', config, {
        **placeholders(config, config.start, config.cutoff, window.key_range),
        'tolerance': repr(FLOAT_TOLERANCE), 'max_key_hashes': str(MAX_KEY_HASHES)})


def rows_sql(config: Config, day: str, key_hashes: list[str], key_range: str = '1') -> str:
    """The rows of the given key hashes in the calendar month of day: a key whose day differs between old and new
    usually still has both versions in that month. key_range, the hash's window, keeps a ranking source cheap."""
    month_start = datetime.date.fromisoformat(day).replace(day=1)
    month_end = (month_start + datetime.timedelta(days=32)).replace(day=1)
    start, end = max(str(month_start), config.start), min(str(month_end), config.cutoff)
    quoted_hashes = ', '.join(f"'{key_hash}'" for key_hash in key_hashes)
    sql = from_template('rows.sql', config, {**placeholders(config, start, end, key_range),
                                            'key_hashes': quoted_hashes})
    return sql + '\nORDER BY key_hash, side, host'


# ---------- windows ----------
# The comparison is split into ranges of a sorting-key prefix ("windows"), so each query aggregates a bounded
# number of keys and the primary index limits what it reads. bounds.sql derives them once from the tables' indexes.

def sorting_key(table: str) -> list[str]:
    database, name = table.split('.', 1)
    output = run_query(f"SELECT sorting_key FROM system.tables WHERE database = '{database}' AND name = '{name}'")
    return output.strip().split(', ')


def bound_columns(config: Config) -> list[Column]:
    """The columns windows are cut on: the longest prefix shared by both sorting keys of plain key columns with the
    same non-nullable type on both sides. Rows with the same key then always fall in the same window."""
    keys = {column.name: column for column in columns(config)[0]}
    result = []
    for old_name, new_name in zip(sorting_key(config.old), sorting_key(config.new)):
        column = keys.get(old_name)
        if old_name != new_name or column is None or column.nullable or not column.same_type:
            break
        result.append(column)
    if not result:
        raise RuntimeError('the sorting keys of old and new share no prefix of key columns to cut windows on')
    return result


def literal_sql(column: Column) -> str:
    """An expression that renders the column's value as an SQL literal; strings go through hex so any bytes survive."""
    if column.base_type.startswith(('String', 'FixedString')):
        return f"concat('unhex(''', hex({column.name}), ''')')"
    if column.base_type.startswith(('Int', 'UInt', 'Float', 'Decimal', 'Bool')):
        return f'toString({column.name})'
    return f"concat('''', toString({column.name}), '''')"  # dates, times and the like contain no quotes


class Bound(TypedDict):
    """The first key of a window: one SQL literal per bound column, and a readable label."""
    literals: list[str]
    label: str


class Bounds(TypedDict):
    fingerprint: str
    columns: list[str]
    bounds: list[Bound]


def compute_bounds(config: Config) -> Bounds:
    bound = bound_columns(config)
    names = ', '.join(column.name for column in bound)
    (old_database, old_name), (new_database, new_name) = config.old.split('.', 1), config.new.split('.', 1)
    index_entries = from_template('index.sql', config, dict(
        old_database=old_database, old_name=old_name, new_database=new_database, new_name=new_name,
        bound_columns=names, bound_literals=f"[{', '.join(literal_sql(column) for column in bound)}]",
        start=config.start, end=config.cutoff))
    sql = Template((SQL_DIR / 'bounds.sql').read_text()).substitute(
        index_entries=index_entries, bound_columns=names, window_rows=str(WINDOW_ROWS))
    bounds: list[Bound] = []
    for row in run_query_json(sql, timeout_s=600):
        literals = [str(literal) for literal in row['literals']] if isinstance(row['literals'], list) else []
        if not bounds or bounds[-1]['literals'] != literals:  # several granules can start with the same prefix
            bounds.append(Bound(literals=literals, label=str(row['label'])))
    return Bounds(fingerprint=fingerprint(config), columns=[column.name for column in bound], bounds=bounds)


def key_order(names: list[str], literals: list[str], strict: str, last: str) -> str:
    """The lexicographic comparison (names) <op> (literals), expanded into column comparisons: the primary index
    is not used for a comparison of tuples."""
    if len(names) == 1:
        return f'{names[0]} {last} {literals[0]}'
    rest = key_order(names[1:], literals[1:], strict, last)
    return f'({names[0]} {strict} {literals[0]} OR ({names[0]} = {literals[0]} AND {rest}))'


@dataclass(frozen=True)
class Window:
    """One query of the comparison: the keys from lower (inclusive) to upper (exclusive); None is unbounded."""
    index: int
    count: int
    columns: tuple[str, ...]
    lower: tuple[str, ...] | None
    upper: tuple[str, ...] | None
    label: str  # the lower bound, readable

    @property
    def name(self) -> str:
        return f'{self.index:04d}'

    @property
    def key_range(self) -> str:
        conditions = []
        if self.lower is not None:
            conditions.append(key_order(list(self.columns), list(self.lower), '>', '>='))
        if self.upper is not None:
            conditions.append(key_order(list(self.columns), list(self.upper), '<', '<'))
        return ' AND '.join(conditions) or '1'


def windows(bounds: Bounds) -> list[Window]:
    edges: list[Bound | None] = [None, *bounds['bounds'], None]
    return [Window(index=index, count=len(edges) - 1, columns=tuple(bounds['columns']),
                   lower=None if lower is None else tuple(lower['literals']),
                   upper=None if upper is None else tuple(upper['literals']),
                   label='start' if lower is None else lower['label'])
            for index, (lower, upper) in enumerate(zip(edges, edges[1:]))]


# ---------- cache ----------

def load_config(name: str) -> Config:
    if name not in CONFIGS:
        raise SystemExit(f'unknown config {name!r}; known: {", ".join(CONFIGS)}')
    return CONFIGS[name]


def fingerprint(config: Config) -> str:
    """Changes whenever the config, the SQL templates or the constants change; cached bounds with another
    fingerprint are stale, and so are the results computed with them."""
    digest = hashlib.sha256(repr((config, FLOAT_TOLERANCE, MAX_KEY_HASHES, WINDOW_ROWS)).encode())
    for path in sorted(SQL_DIR.rglob('*.sql')):
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def bounds_fingerprint(bounds: Bounds) -> str:
    """Identifies the bounds a result was computed with; they include the config's fingerprint."""
    return hashlib.sha256(json.dumps(bounds, sort_keys=True).encode()).hexdigest()[:16]


class ResultRow(TypedDict):
    """One row of compare.sql: the keys of one category on one day on one shard."""
    host: str
    day: str
    category: str
    keys: int
    old_rows: int
    new_rows: int
    key_hashes: list[str]


class WindowResult(TypedDict):
    fingerprint: str
    elapsed_s: float
    rows: list[ResultRow]


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value))
    temporary.replace(path)  # atomic, so an interrupted run never leaves a half-written file


def bounds_path(config_name: str) -> Path:
    return CACHE_DIR / config_name / 'bounds.json'


def load_bounds(config_name: str, config: Config) -> Bounds | None:
    """The cached bounds, or None if they are missing or stale."""
    path = bounds_path(config_name)
    if not path.exists():
        return None
    bounds: Bounds = json.loads(path.read_text())
    return bounds if bounds['fingerprint'] == fingerprint(config) else None


def save_bounds(config_name: str, bounds: Bounds) -> None:
    write_json(bounds_path(config_name), bounds)


def result_path(config_name: str, window: Window) -> Path:
    return CACHE_DIR / config_name / f'window-{window.name}.json'


def load_result(config_name: str, bounds: Bounds, window: Window) -> WindowResult | None:
    """The cached result of a window, or None if it is missing or was computed with other bounds."""
    path = result_path(config_name, window)
    if not path.exists():
        return None
    result: WindowResult = json.loads(path.read_text())
    return result if result['fingerprint'] == bounds_fingerprint(bounds) else None


def save_result(config_name: str, window: Window, result: WindowResult) -> None:
    write_json(result_path(config_name, window), result)
