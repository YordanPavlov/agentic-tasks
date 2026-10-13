"""Shared pieces of the old-vs-new table comparison: ClickHouse access, SQL generation from the templates in sql/,
the partitions and windows, and the result files. README.md describes the method.
"""
from __future__ import annotations

import datetime
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from string import Template
from typing import Any, Iterable, NotRequired, TypedDict

from configs import CONFIGS, Config

HOST = os.environ.get('TCMP_HOST', 'clickhouse.production.san')
PORT = os.environ.get('TCMP_PORT', '30900')
USER = os.environ.get('TCMP_USER', 'readonly')
RESULTS_DIR = Path(os.environ.get('TCMP_RESULTS', Path(__file__).parent / 'results'))
SQL_DIR = Path(__file__).parent / 'sql'
# Read once, so that a run uses one version of the SQL even if the files change while it runs.
TEMPLATES = {path.relative_to(SQL_DIR).as_posix(): Template(path.read_text()) for path in SQL_DIR.rglob('*.sql')}

FLOAT_TOLERANCE = 1e-9  # relative
# Rows per shard and window, both sides together. A window query holds all of its keys in memory.
WINDOW_ROWS = int(os.environ.get('TCMP_WINDOW_ROWS', 20_000_000))
LOG_COMMENT = json.dumps({'job': 'table-compare', 'owner': 'yordan.p@santiment.net', 'team': 'bigdata',
                          'repo': 'clickhouse-tables', 'dag': 'manual-tests'})

CATEGORIES = ('missing_in_new', 'missing_in_old', 'differs', 'equal')
# the classes of 'differs' by size; compare.py's --float-max and --small-max set the bounds
DIFF_CLASSES = ('float', 'small', 'large')
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


def run_query_json(sql: str, timeout_s: int = 1800) -> list[dict[str, Any]]:
    return [json.loads(line) for line in run_query(sql, 'JSONEachRow', timeout_s).splitlines()]


def load_config(name: str) -> Config:
    if name not in CONFIGS:
        raise SystemExit(f'unknown config {name!r}; known: {", ".join(CONFIGS)}')
    return CONFIGS[name]


# ---------- partitions ----------

def next_month(day: str) -> str:
    """The first day of the month after day's."""
    return str((datetime.date.fromisoformat(day).replace(day=1) + datetime.timedelta(days=32)).replace(day=1))


def months(start: str, end: str) -> list[str]:
    """The months 'YYYY-MM' that overlap [start, end)."""
    result = []
    day = f'{start[:7]}-01'
    while day < end:
        result.append(day[:7])
        day = next_month(day)
    return result


@dataclass(frozen=True)
class Partition:
    """A ClickHouse partition of both tables, limited to [start, cutoff): it is cut into windows and compared on
    its own. A month ('YYYY-MM'), or the whole range ('all') for unpartitioned tables."""
    name: str
    start: str
    end: str

    @property
    def months(self) -> list[str]:
        return months(self.start, self.end)


def partitions(config: Config) -> list[Partition]:
    """The partitions of the tables, as their partition keys tell. Other partition keys fail: a time filter that
    doesn't match the partitions reads the same granules once per filter."""
    keys = {table: run_query(f"SELECT partition_key FROM system.tables WHERE database || '.' || name = '{table}'")
            .strip() for table in (config.old, config.new)}
    if all(key == '' for key in keys.values()):
        return [Partition('all', config.start, config.cutoff)]
    if all(re.fullmatch(rf'(toYYYYMM|toStartOfMonth)\({config.dt}\)', key) for key in keys.values()):
        return [Partition(month, max(f'{month}-01', config.start), min(next_month(f'{month}-01'), config.cutoff))
                for month in months(config.start, config.cutoff)]
    raise SystemExit(f'unsupported partition keys: {keys}; supported: none, or monthly on {config.dt}')


# ---------- SQL generation ----------
# The SQL templates in sql/ hold the structure; Python only fills in their $placeholders from the config.

def side_where(config: Config, side: str, common_groups: bool = True) -> str:
    """The filters of one side ('old' or 'new'); with common_groups_only, also its groups present on the other."""
    where, other_table, other_where = ((config.where_old, config.new, config.where_new) if side == 'old'
                                       else (config.where_new, config.old, config.where_old))
    filters = [f'({condition})' for condition in (config.where, where) if condition != '1']
    if config.common_groups_only and common_groups:
        groups = ', '.join(config.group_by)
        other_filters = ''.join(f' AND ({condition})' for condition in (config.where, other_where) if condition != '1')
        filters.append(f'({groups}) IN (SELECT DISTINCT {groups} FROM {other_table}'
                       f" WHERE {config.dt} >= '{config.start}' AND {config.dt} < '{config.cutoff}'{other_filters})")
    return ' AND '.join(filters) or '1'


def from_template(file_name: str, config: Config, values: dict[str, str]) -> str:
    """A template with its placeholders filled in, wrapped so that it runs where the data is.
    For a sharded table, cluster(view(...)) sends the whole query to one replica of each shard, and the caller
    gets the shards' results concatenated."""
    sql = TEMPLATES[file_name].substitute(values)
    if config.cluster is None:
        return f'SELECT * FROM (\n{sql}\n)'
    return f"SELECT * FROM cluster('{config.cluster}', view(\n{sql}\n))"


def table_values(config: Config, start: str, end: str, common_groups: bool = True) -> dict[str, str]:
    """The placeholders of the templates that read the tables."""
    return dict(old_table=config.old, new_table=config.new, dt=config.dt, start=start, end=end,
                old_where=side_where(config, 'old', common_groups),
                new_where=side_where(config, 'new', common_groups))


@dataclass(frozen=True)
class Thresholds:
    """Upper bounds of the 'float' and 'small' classes."""
    float_max: float
    small_max: float


def compare_sql(config: Config, partition: Partition, key_range: str, thresholds: Thresholds) -> str:
    return from_template('compare.sql', config, dict(
        **table_values(config, partition.start, partition.end), key_range=key_range,
        keys=', '.join(config.keys), value=config.value, tolerance=repr(FLOAT_TOLERANCE),
        float_max=repr(thresholds.float_max), small_max=repr(thresholds.small_max),
        group_cells=''.join(f'toString({column}), ' for column in config.group_by)))


# ---------- windows ----------
# Each partition is cut into ranges of the bucket columns ("windows"), so each query holds a bounded number of keys and
# the primary index limits what it reads. cuts.sql cuts them from exact row counts.

@lru_cache(maxsize=None)
def column_types(table: str) -> dict[str, str]:
    """Column name -> type of a local table; it runs on the connected host, which holds the local tables."""
    return dict(line.split('\t')[:2] for line in run_query(f'DESCRIBE TABLE {table}').splitlines())


def literal_sql(column: str, type_name: str) -> str:
    """An expression that renders the column's value as an SQL literal; strings go through hex so any bytes survive."""
    if re.match(r'(LowCardinality\()?(Fixed)?String', type_name):
        return f"concat('unhex(''', hex({column}), ''')')"
    if type_name.startswith(('Int', 'UInt', 'Float', 'Decimal', 'Bool')):
        return f'toString({column})'
    if type_name.startswith('Nullable'):
        raise RuntimeError(f'bucket column {column} is Nullable; it cannot be cut on')
    return f"concat('''', toString({column}), '''')"  # dates, times and the like contain no quotes


@lru_cache(maxsize=None)
def shard_count(config: Config) -> int:
    if config.cluster is None:
        return 1
    return int(run_query(f"SELECT count(DISTINCT shard_num) FROM system.clusters WHERE cluster = '{config.cluster}'"))


def key_order(names: list[str], literals: list[str], strict: str, last: str) -> str:
    """The lexicographic comparison (names) <op> (literals), expanded into column comparisons: the primary index
    is not used for a comparison of tuples."""
    if len(names) == 1:
        return f'{names[0]} {last} {literals[0]}'
    rest = key_order(names[1:], literals[1:], strict, last)
    return f'({names[0]} {strict} {literals[0]} OR ({names[0]} = {literals[0]} AND {rest}))'


@dataclass(frozen=True)
class Windows:
    """A partition cut into windows, and its rows per side as FINAL shows them."""
    old_rows: int
    new_rows: int
    key_ranges: list[str]  # one SQL condition per window


def cut_partition_into_windows(config: Config, partition: Partition) -> Windows:
    buckets = list(config.buckets)
    types = column_types(config.new)
    counts = from_template('counts.sql', config, dict(
        **table_values(config, partition.start, partition.end), buckets=', '.join(buckets),
        bucket_literals=f"[{', '.join(literal_sql(column, types[column]) for column in buckets)}]"))
    sql = TEMPLATES['cuts.sql'].substitute(
        counts=counts, buckets=', '.join(buckets), window_rows=str(WINDOW_ROWS * shard_count(config)))
    row = run_query_json(sql, timeout_s=1800)[0]
    edges: list[list[str] | None] = [None, *row['cuts'], None]
    key_ranges = []
    for lower, upper in zip(edges, edges[1:]):
        conditions = []
        if lower is not None:
            conditions.append(key_order(buckets, lower, '>', '>='))
        if upper is not None:
            conditions.append(key_order(buckets, upper, '<', '<'))
        key_ranges.append(' AND '.join(conditions) or '1')
    return Windows(old_rows=int(row['old_rows']), new_rows=int(row['new_rows']), key_ranges=key_ranges)


def coverage(config: Config) -> dict[str, list[dict[str, Any]]]:
    """The groups present on one side only over the whole range, with their rows."""
    sql = from_template('coverage.sql', config, dict(
        **table_values(config, config.start, config.cutoff, common_groups=False),
        group_by=', '.join(config.group_by)))
    rows: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in run_query_json(sql):  # one row per group, side and shard
        group = tuple(str(row[column]) for column in config.group_by)
        total = rows.setdefault(group, {**{column: row[column] for column in config.group_by},
                                        'old_rows': 0, 'new_rows': 0})
        total['old_rows'] += int(row['old_rows'])
        total['new_rows'] += int(row['new_rows'])
    return {'only_in_old': [row for row in rows.values() if row['new_rows'] == 0],
            'only_in_new': [row for row in rows.values() if row['old_rows'] == 0]}


# ---------- result files ----------

def describe(config: Config, thresholds: Thresholds | None = None) -> dict[str, object]:
    """What a reader of the results needs to know about the comparison, without configs.py."""
    return dict(old=config.old, new=config.new, keys=list(config.keys), value=config.value,
                group_by=list(config.group_by), dt=config.dt, start=config.start, cutoff=config.cutoff,
                tolerance=FLOAT_TOLERANCE,
                **(dict(float_max=thresholds.float_max, small_max=thresholds.small_max) if thresholds else {}),
                common_groups_only=config.common_groups_only,
                where=config.where, where_old=config.where_old, where_new=config.where_new)


class Month(TypedDict):
    """One results/<config>/YYYY/YYYY-MM.json."""
    month: str
    config: dict[str, object]
    partition: dict[str, object]  # the partition the month was compared in: name, windows, started, finished (UTC)
    rows: dict[str, int]       # old, new: as FINAL shows them
    keys: dict[str, int]       # per category
    days: dict[str, Any]       # day -> cell, failing keys only
    groups: NotRequired[dict[str, Any]]  # group_by values -> cell, with group_by only


def failing_keys(keys: dict[str, int]) -> int:
    """The keys of all categories but equal, from a month's keys."""
    return sum(keys.get(category, 0) for category in CATEGORIES if category != 'equal')


def is_cell(node: object) -> bool:
    """Whether a node under days or groups is a cell: category -> stats."""
    return (isinstance(node, dict) and bool(node)
            and all(key in CATEGORIES and isinstance(child, dict) for key, child in node.items()))


def day_ranges(days: Iterable[str]) -> list[str]:
    """ISO days as ranges of consecutive days: 'YYYY-MM-DD..YYYY-MM-DD', or the day alone."""
    ranges: list[list[datetime.date]] = []
    for day in sorted(datetime.date.fromisoformat(day) for day in set(days)):
        if ranges and day - ranges[-1][1] == datetime.timedelta(days=1):
            ranges[-1][1] = day
        else:
            ranges.append([day, day])
    return [str(first) if first == last else f'{first}..{last}' for first, last in ranges]


def to_json(value: object) -> str:
    """Pretty-printed JSON, except that the cells under days and groups stay on one line each."""
    cells: list[object] = []

    def mark(node: object) -> object:
        if is_cell(node):
            cells.append(node)
            return f'<cell {len(cells) - 1}>'
        if isinstance(node, dict):
            return {key: mark(child) for key, child in node.items()}
        return node
    if isinstance(value, dict):
        value = {key: mark(child) if key in ('days', 'groups') else child for key, child in value.items()}
    text = json.dumps(value, indent=2)
    return re.sub(r'"<cell (\d+)>"', lambda match: json.dumps(cells[int(match.group(1))]), text)


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(to_json(value) + '\n')
    temporary.replace(path)  # atomic, so an interrupted run never leaves a half-written file


def month_path(config_name: str, month: str) -> Path:
    return RESULTS_DIR / config_name / month[:4] / f'{month}.json'


def coverage_path(config_name: str) -> Path:
    return RESULTS_DIR / config_name / 'coverage.json'


def load_json(path: Path) -> Any:
    """The content of a result file, or None if it is missing."""
    return json.loads(path.read_text()) if path.exists() else None
