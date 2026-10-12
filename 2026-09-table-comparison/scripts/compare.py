"""Compare old and new partition by partition, in date order. A partition (a month, or the whole range for
unpartitioned tables) is cut into windows of about equal rows; one query per window reads it from each side once
and categorizes every key. Each month's result goes to results/<config>/YYYY/YYYY-MM.json once its partition is
done, so an interrupted run resumes with the first unfinished partition.

usage: compare.py <config> [PARTITION ...] [--parallel N] [--max-failing-keys N] [--force]
                  [--print-sql PARTITION WINDOW]
  PARTITION     only these partitions: YYYY-MM, or 'all' for unpartitioned tables (default: all partitions)
  --parallel N  queries in flight at once (default 2, the limit for long queries on prod)
  --max-failing-keys N  stop after the month in which the failing keys of all months pass N (default 100,000):
                the new table needs a closer look first
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

from common import (CATEGORIES, Month, Partition, Windows, compare_sql, coverage, coverage_path,
                    cut_partition_into_windows, describe, failing_keys, is_cell, load_config, load_json, month_path,
                    months, partitions, run_query_json, write_json)
from configs import Config

WindowResult = tuple[list[dict[str, Any]], str]  # compare.sql's rows, the time the query finished


def now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')


def run_window(config: Config, partition: Partition, key_range: str) -> WindowResult:
    return run_query_json(compare_sql(config, partition, key_range), timeout_s=7200), now()


def add_to_cell(cell: dict[str, Any], row: dict[str, Any]) -> None:
    """Adds one row of compare.sql (one category of a cell, from one window and shard) to the cell."""
    category = row['category']
    cell[category] = cell.get(category, 0) + row['keys']
    if category == 'differs' and row['max_diff'] is not None:
        cell['max_diff_pct'] = max(cell.get('max_diff_pct', 0.0), float(f"{row['max_diff'] * 100:.3g}"))


def sorted_tree(node: dict[str, Any]) -> dict[str, Any]:
    """The nested deviations sorted by key, numbers by value: groups such as asset ids, then days."""
    def order(key: str) -> tuple[int, int | str]:
        return (0, int(key)) if key.isdigit() else (1, key)
    if is_cell(node):  # categories in a fixed order, then max_diff_pct
        return {field: node[field] for field in (*CATEGORIES, 'max_diff_pct') if field in node}
    return {key: sorted_tree(node[key]) for key in sorted(node, key=order)}


def build_months(config: Config, partition: Partition, started: str, windows: Windows,
                 results: list[WindowResult]) -> list[Month]:
    """The month files of a finished partition. Fails if the windows' rows don't add up to the partition's
    counts."""
    finished = max(finished for _, finished in results)
    months = {month: Month(month=month, config=describe(config),
                           partition=dict(name=partition.name, windows=len(results), started=started,
                                          finished=finished),
                           rows={'old': 0, 'new': 0}, keys={}, deviations={})
              for month in partition.months}
    for rows, _ in results:
        for row in rows:
            cell_path = [str(part) for part in row['cell']]  # group values and day, or the month for equal keys
            month = months[cell_path[-1][:7]]
            month['rows']['old'] += row['old_row_count']
            month['rows']['new'] += row['new_row_count']
            month['keys'][row['category']] = month['keys'].get(row['category'], 0) + row['keys']
            if row['category'] == 'equal':
                continue
            node = month['deviations']
            for part in cell_path:
                node = node.setdefault(part, {})
            add_to_cell(node, row)
    old_rows = sum(month['rows']['old'] for month in months.values())
    new_rows = sum(month['rows']['new'] for month in months.values())
    if (old_rows, new_rows) != (windows.old_rows, windows.new_rows):
        raise RuntimeError(f'rows of the windows (old {old_rows:,}, new {new_rows:,}) differ from the counts '
                           f'(old {windows.old_rows:,}, new {windows.new_rows:,})')
    for month in months.values():
        month['keys'] = {category: month['keys'][category] for category in CATEGORIES if category in month['keys']}
        month['deviations'] = sorted_tree(month['deviations'])
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


def finish(config_name: str, config: Config, running: Running, failing_before: int, max_failing: int) -> int:
    """Writes the months of a finished partition in date order, until the failing keys of the run exceed
    max_failing; returns the failing keys of the run after it."""
    try:
        months = build_months(config, running.partition, running.started, running.windows,
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
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--print-sql', nargs=2, metavar=('PARTITION', 'WINDOW'))
    args = parser.parse_args()
    config = load_config(args.config)
    all_partitions = {partition.name: partition for partition in partitions(config)}

    if args.print_sql:
        partition = all_partitions[args.print_sql[0]]
        key_range = cut_partition_into_windows(config, partition).key_ranges[int(args.print_sql[1])]
        print(compare_sql(config, partition, key_range))
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
            failing = finish(args.config, config, queue.popleft(), failing, args.max_failing_keys)

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
            futures = [pool.submit(run_window, config, partition, key_range) for key_range in windows.key_ranges]
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
