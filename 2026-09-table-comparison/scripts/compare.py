"""Compare old and new partition by partition, in date order. A partition (a month, or the whole range for
unpartitioned tables) is cut into windows of about equal rows; one query per window reads it from each side once
and categorizes every key. Each month's result goes to results/<config>/YYYY/YYYY-MM.json once its partition is
done, so an interrupted run resumes with the first unfinished partition.

usage: compare.py <config> [PARTITION ...] [--parallel N] [--max-failing-keys N] [--float-max X] [--small-max X]
                  [--force] [--print-sql PARTITION WINDOW]
  PARTITION     only these partitions: YYYY-MM, or 'all' for unpartitioned tables (default: all partitions)
  --parallel N  queries in flight at once (default 2, the limit for long queries on prod)
  --max-failing-keys N  stop after the month in which the failing keys of all months pass N (default 100,000):
                the new table needs a closer look first
  --float-max X, --small-max X  the largest relative differences of 'differs' counted as 'float' (default 1e-6)
                and 'small' (default 0.01); larger ones are 'large'. A change needs --force
  --force       re-run partitions that already have results: after the data, the config or the SQL changed
  --print-sql   print the query of one window of one partition and exit
"""
from __future__ import annotations

import argparse
import datetime
import os
import sys
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from common import (CATEGORIES, DIFF_CLASSES, Month, Partition, Thresholds, Windows, compare_sql, coverage,
                    coverage_path, cut_partition_into_windows, day_ranges, describe, failing_keys, is_cell,
                    load_config, load_json, month_path, months, partitions, run_query_json, write_json)
from configs import Config

WindowResult = tuple[list[dict[str, Any]], str]  # compare.sql's rows, the time the query finished


def now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')


def run_window(config: Config, partition: Partition, key_range: str, thresholds: Thresholds) -> WindowResult:
    return run_query_json(compare_sql(config, partition, key_range, thresholds), timeout_s=7200), now()


def add_to_cell(cell: dict[str, Any], row: dict[str, Any]) -> None:
    """Adds one row of compare.sql to the cell."""
    category = row['category']
    if category == 'differs':
        stats = cell.setdefault(category, {'keys': 0, **{name: 0 for name in DIFF_CLASSES}, 'new_gt_old': 0,
                                           'max_diff_pct': 0.0})
        stats[row['diff_class']] += row['keys']
        stats['new_gt_old'] += row['keys'] if row['new_gt_old'] else 0
        if row['max_diff'] is not None:
            stats['max_diff_pct'] = max(stats['max_diff_pct'], float(f"{row['max_diff'] * 100:.3g}"))
    else:
        stats = cell.setdefault(category, {'keys': 0, 'zero': 0})
        stats['zero'] += row['keys'] if row['is_zero'] else 0
    stats['keys'] += row['keys']


def order(key: str) -> tuple[int, int | str]:
    """Sort order of group values and days: numbers by value."""
    return (0, int(key)) if key.isdigit() else (1, key)


def sorted_tree(node: dict[str, Any]) -> dict[str, Any]:
    """Days or nested groups sorted by key, a cell's categories in a fixed order."""
    if is_cell(node):
        return {category: node[category] for category in CATEGORIES if category in node}
    return {key: sorted_tree(node[key]) for key in sorted(node, key=order)}


def build_months(config: Config, thresholds: Thresholds, partition: Partition, started: str, windows: Windows,
                 results: list[WindowResult]) -> list[Month]:
    """The month files of a finished partition. Fails if the windows' rows don't add up to the partition's
    counts."""
    finished = max(finished for _, finished in results)
    months = {month: Month(month=month, config=describe(config, thresholds),
                           partition=dict(name=partition.name, windows=len(results), started=started,
                                          finished=finished),
                           rows={'old': 0, 'new': 0}, keys={}, days={})
              for month in partition.months}
    groups: dict[str, dict[tuple[str, ...], dict[str, Any]]] = {month: {} for month in partition.months}
    group_days: dict[tuple[str, tuple[str, ...], str], set[str]] = {}  # (month, group, category) -> days
    for rows, _ in results:
        for row in rows:
            cell_path = [str(part) for part in row['cell']]  # group values and day, or the month for equal keys
            day = cell_path[-1]
            month = months[day[:7]]
            month['rows']['old'] += row['old_row_count']
            month['rows']['new'] += row['new_row_count']
            month['keys'][row['category']] = month['keys'].get(row['category'], 0) + row['keys']
            if row['category'] == 'equal':
                continue
            add_to_cell(month['days'].setdefault(day, {}), row)
            if config.group_by:
                group = tuple(cell_path[:-1])
                add_to_cell(groups[day[:7]].setdefault(group, {}), row)
                group_days.setdefault((day[:7], group, row['category']), set()).add(day)
    old_rows = sum(month['rows']['old'] for month in months.values())
    new_rows = sum(month['rows']['new'] for month in months.values())
    if (old_rows, new_rows) != (windows.old_rows, windows.new_rows):
        raise RuntimeError(f'rows of the windows (old {old_rows:,}, new {new_rows:,}) differ from the counts '
                           f'(old {windows.old_rows:,}, new {windows.new_rows:,})')
    for name, month in months.items():
        month['keys'] = {category: month['keys'][category] for category in CATEGORIES if category in month['keys']}
        month['days'] = sorted_tree(month['days'])
        if config.group_by:
            nested: dict[str, Any] = {}
            for group, cell in groups[name].items():
                for category, stats in cell.items():
                    stats['days'] = day_ranges(group_days[name, group, category])
                node = nested
                for part in group[:-1]:
                    node = node.setdefault(part, {})
                node[group[-1]] = cell
            month['groups'] = sorted_tree(nested)
    return list(months.values())


def report(month: Month) -> str:
    failing = ', '.join(f'{category}={keys:,}' for category, keys in month['keys'].items() if category != 'equal')
    elapsed = (datetime.datetime.fromisoformat(str(month['partition']['finished']))
               - datetime.datetime.fromisoformat(str(month['partition']['started'])))
    return (f"{month['month']}  {month['partition']['windows']:>3} windows  {elapsed.total_seconds():>6.0f} s  "
            f"{month['rows']['old']:>13,} old rows  {month['rows']['new']:>13,} new rows  {failing or 'equal'}")


@dataclass
class Running:
    """A partition whose windows are queued or running."""
    partition: Partition
    started: str  # before the partition was cut
    windows: Windows
    futures: list[Future[WindowResult]]


def finish(config_name: str, config: Config, thresholds: Thresholds, running: Running, failing_before: int,
           max_failing: int) -> int:
    """Writes the months of a finished partition in date order, until the failing keys of the run exceed
    max_failing; returns the failing keys of the run after it."""
    try:
        months = build_months(config, thresholds, running.partition, running.started, running.windows,
                              [future.result() for future in running.futures])
    except RuntimeError as error:
        # a failed partition writes nothing and is retried on the next run
        print(f'{running.partition.name}  FAILED: {error}', file=sys.stderr, flush=True)
        return failing_before
    failing = failing_before
    for month in months:
        if failing > max_failing:
            break
        write_json(month_path(config_name, month['month']), month)
        print(report(month), flush=True)
        failing += failing_keys(month['keys'])
    return failing


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('partitions', nargs='*')
    parser.add_argument('--parallel', type=int, default=2)
    parser.add_argument('--max-failing-keys', type=int, default=100_000)
    parser.add_argument('--float-max', type=float, default=1e-6)
    parser.add_argument('--small-max', type=float, default=0.01)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--print-sql', nargs=2, metavar=('PARTITION', 'WINDOW'))
    args = parser.parse_args()
    config = load_config(args.config)
    thresholds = Thresholds(args.float_max, args.small_max)
    all_partitions = {partition.name: partition for partition in partitions(config)}

    if args.print_sql:
        partition = all_partitions[args.print_sql[0]]
        key_range = cut_partition_into_windows(config, partition).key_ranges[int(args.print_sql[1])]
        print(compare_sql(config, partition, key_range, thresholds))
        return

    if config.group_by and (args.force or load_json(coverage_path(args.config)) is None):
        started = now()
        groups = coverage(config)
        write_json(coverage_path(args.config), {'config': describe(config), 'started': started, 'finished': now(),
                                                **groups})
        print(f"coverage: {len(groups['only_in_old'])} groups only in old, {len(groups['only_in_new'])} only in new",
              file=sys.stderr)

    todo = [all_partitions[name] for name in args.partitions] if args.partitions else list(all_partitions.values())
    if not args.force:
        todo = [partition for partition in todo
                if any(load_json(month_path(args.config, month)) is None for month in partition.months)]
    todo_months = {month for partition in todo for month in partition.months}
    failing = sum(failing_keys(result['keys']) for month in months(config.start, config.cutoff)
                  if month not in todo_months
                  and (result := load_json(month_path(args.config, month))) is not None)
    print(f'{len(todo)} of {len(all_partitions)} partition(s) to run; {failing:,} failing keys so far', file=sys.stderr)

    # Partitions are cut one after the other, as soon as fewer than --parallel queries are queued, and finished in
    # date order; the workers only run queries.
    pool = ThreadPoolExecutor(max_workers=args.parallel)
    queue: deque[Running] = deque()

    def unfinished() -> list[Future[WindowResult]]:
        return [future for running in queue for future in running.futures if not future.done()]

    def finish_ready() -> None:
        nonlocal failing
        while queue and all(future.done() for future in queue[0].futures) and failing <= args.max_failing_keys:
            failing = finish(args.config, config, thresholds, queue.popleft(), failing, args.max_failing_keys)

    try:
        for partition in todo:
            if failing > args.max_failing_keys:
                break
            while len(unfinished()) >= args.parallel:
                wait(unfinished(), return_when=FIRST_COMPLETED)
                finish_ready()
            started = now()
            try:
                windows = cut_partition_into_windows(config, partition)
            except RuntimeError as error:
                print(f'{partition.name}  FAILED to cut: {error}', file=sys.stderr, flush=True)
                continue
            futures = [pool.submit(run_window, config, partition, key_range, thresholds)
                       for key_range in windows.key_ranges]
            queue.append(Running(partition, started, windows, futures))
            finish_ready()
        while queue and failing <= args.max_failing_keys:
            wait(unfinished(), return_when=FIRST_COMPLETED)
            finish_ready()
    except KeyboardInterrupt:
        # Ctrl+C also reached the running clickhouse-clients; os._exit skips the join on the worker threads
        pool.shutdown(wait=False, cancel_futures=True)
        print('interrupted; finished partitions are written', file=sys.stderr, flush=True)
        os._exit(130)
    if failing > args.max_failing_keys:
        print(f'stopped: {failing:,} failing keys, more than --max-failing-keys {args.max_failing_keys:,}',
              file=sys.stderr)
    pool.shutdown(cancel_futures=True)  # after a stop, waits for the queries already running


if __name__ == '__main__':
    main()
