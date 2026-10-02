"""Shared pieces of the old-vs-new table comparison: ClickHouse access, column metadata, SQL generation from
the templates in sql/, and the per-month result cache. README.md describes the method.
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
MAX_KEY_HASHES = 10_000  # kept per (shard, day, category) for drill-down and the cross-shard check
LOG_COMMENT = json.dumps({'job': 'table-compare', 'owner': 'yordan.p@santiment.net', 'team': 'bigdata',
                          'repo': 'clickhouse-tables', 'dag': 'manual-tests'})

# Categories that mean "no difference"; everything else needs explaining.
OK_CATEGORIES = ('equal', 'float_noise')
# Network errors (209 timeout, 210 connection refused, 32 unexpected EOF) are retried; anything else is final.
RETRYABLE_ERROR_CODES = {'32', '209', '210'}


# ---------- ClickHouse access ----------

def run_query(sql: str, output_format: str = 'TSV', timeout_s: int = 1800) -> str:
    """Run one query with clickhouse-client and return its output. The format is passed as an option,
    because --format overrides a FORMAT clause anyway."""
    command = ['clickhouse-client', '-h', HOST, '--port', PORT, '-u', USER, f'--log_comment={LOG_COMMENT}',
               f'--max_execution_time={timeout_s}', '--output_format_json_quote_64bit_integers=0',
               f'--format={output_format}', '--query', sql]
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


def table_types(table: str) -> dict[str, str]:
    """Column name -> type of a 'db.table'."""
    database, name = table.split('.', 1)
    output = run_query(f"SELECT name, type FROM system.columns WHERE database = '{database}' AND table = '{name}'")
    return dict(line.split('\t') for line in output.splitlines())


@lru_cache(maxsize=None)  # one metadata lookup per config and process
def columns(config: Config) -> tuple[list[Column], list[Column]]:
    """(key columns, value columns) with types from both tables; fails if a listed column is missing."""
    old_types, new_types = table_types(config.old), table_types(config.new)
    if not old_types or not new_types:
        raise RuntimeError(f'table not found: {config.old if not old_types else config.new}')
    missing = [name for name in config.keys + config.values if name not in old_types or name not in new_types]
    if missing:
        raise RuntimeError(f'columns missing on one side: {missing}')

    def column(name: str) -> Column:
        return Column(name, old_types[name], new_types[name])
    return [column(name) for name in config.keys], [column(name) for name in config.values]


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


def placeholders(config: Config, window: Window) -> dict[str, str]:
    """The $placeholders shared by the templates: tables, window, column lists and the config's filters."""
    keys, values = columns(config)
    floats = [column for column in values if column.is_float]
    exact = [hash_args(column) for column in values if not column.is_float]
    exact += [f'isNull({column.name})' for column in floats if column.nullable]
    return dict(
        old_table=config.old, new_table=config.new, dt=config.dt, start=str(window.start), end=str(window.end),
        key_columns=', '.join(hash_args(column) for column in keys),
        exact_value_columns=', '.join(exact) or '0',
        float_value_columns=', '.join(float_value(column) for column in floats),
        columns=', '.join(column.name for column in keys + values),
        where=config.where, where_old=config.where_old, where_new=config.where_new)


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
        **placeholders(config, window), 'tolerance': repr(FLOAT_TOLERANCE), 'max_key_hashes': str(MAX_KEY_HASHES)})


def rows_sql(config: Config, window: Window, key_hashes: list[str]) -> str:
    quoted_hashes = ', '.join(f"'{key_hash}'" for key_hash in key_hashes)
    sql = from_template('rows.sql', config, {**placeholders(config, window), 'key_hashes': quoted_hashes})
    return sql + '\nORDER BY key_hash, side, host'


# ---------- windows and cache ----------

@dataclass(frozen=True)
class Window:
    """Days [start, end): one calendar month, cut to the configured start and cutoff."""
    start: datetime.date
    end: datetime.date

    @property
    def name(self) -> str:
        return self.start.strftime('%Y-%m')


def windows(config: Config) -> list[Window]:
    start, cutoff = datetime.date.fromisoformat(config.start), datetime.date.fromisoformat(config.cutoff)
    result, window_start = [], start
    while window_start < cutoff:
        next_month = (window_start.replace(day=1) + datetime.timedelta(days=32)).replace(day=1)
        result.append(Window(window_start, min(next_month, cutoff)))
        window_start = next_month
    return result


def window_of(config: Config, day: str) -> Window:
    date = datetime.date.fromisoformat(day)
    return next(window for window in windows(config) if window.start <= date < window.end)


def load_config(name: str) -> Config:
    if name not in CONFIGS:
        raise SystemExit(f'unknown config {name!r}; known: {", ".join(CONFIGS)}')
    return CONFIGS[name]


def fingerprint(config: Config) -> str:
    """Changes whenever the config, the SQL templates or the constants change; cached results with another
    fingerprint are stale."""
    digest = hashlib.sha256(repr((config, FLOAT_TOLERANCE, MAX_KEY_HASHES)).encode())
    for path in sorted(SQL_DIR.glob('*.sql')):
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


class ResultRow(TypedDict):
    """One row of compare.sql: the keys of one category on one day on one shard."""
    host: str
    day: str
    category: str
    keys: int
    old_rows: int
    new_rows: int
    key_hashes: list[str]


class MonthResult(TypedDict):
    fingerprint: str
    start: str
    end: str
    elapsed_s: float
    rows: list[ResultRow]


def result_path(config_name: str, window: Window) -> Path:
    return CACHE_DIR / config_name / f'{window.name}.json'


def load_result(config_name: str, config: Config, window: Window) -> MonthResult | None:
    """The cached result of a window, or None if it is missing or stale."""
    path = result_path(config_name, window)
    if not path.exists():
        return None
    result: MonthResult = json.loads(path.read_text())
    return result if result['fingerprint'] == fingerprint(config) else None


def save_result(config_name: str, window: Window, result: MonthResult) -> None:
    path = result_path(config_name, window)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(result))
    temporary.replace(path)  # atomic, so an interrupted run never leaves a half-written result
