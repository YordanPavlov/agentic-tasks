"""Compare old and new period by period, in date order. A period (a month, or the whole range for a table not
partitioned by month) is cut into windows of about equal rows; one query per window reads it from each side once
and categorizes every key. Each month's result goes to results/<config>/YYYY/YYYY-MM.json once its period is done,
so an interrupted run resumes with the first unfinished period.

usage: compare.py <config> [PERIOD ...] [--parallel N] [--max-failing-keys N] [--force] [--print-sql PERIOD WINDOW]
  PERIOD        only these periods: YYYY-MM, or 'all' for a config with monthly=False (default: all periods)
  --parallel N  queries in flight at once (default 2, the limit for long queries on prod)
  --max-failing-keys N  stop after the month in which the failing keys of all months pass N (default 100,000):
                the new table needs a closer look first
  --force       re-run periods that already have results, e.g. after the data changed
  --print-sql   print the query of one window of one period and exit
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from common import (CATEGORIES, Cuts, Month, Period, compare_sql, coverage, coverage_path, cut, describe,
                    example_names, failing_keys, fingerprint, load_config, load_json, month_path, months, periods,
                    run_query_json, write_json)
from configs import Config

WindowResult = tuple[list[dict[str, Any]], float]  # compare.sql's rows, query time in seconds


def run_window(config: Config, period: Period, key_range: str) -> WindowResult:
    started = time.monotonic()
    rows = run_query_json(compare_sql(config, period, key_range), timeout_s=7200)
    return rows, time.monotonic() - started


def add_to_cell(cell: dict[str, Any], row: dict[str, Any], names: list[str]) -> None:
    """Adds one row of compare.sql (one category of a cell, from one window and shard) to the cell."""
    category = row['category']
    cell[category] = cell.get(category, 0) + row['keys']
    examples = cell.pop('examples', {})  # popped and put back, so that it stays the cell's last field
    category_examples = examples.setdefault(category, [])
    if category == 'differs' and row['max_diff'] is not None:
        max_diff_pct = float(f"{row['max_diff'] * 100:.3g}")
        if max_diff_pct > cell.get('max_diff_pct', -1):
            cell['max_diff_pct'] = max_diff_pct
            category_examples.insert(0, dict(zip(names, row['max_diff_example'])))  # the largest difference first
    for example in row['examples']:
        if dict(zip(names, example)) not in category_examples:
            category_examples.append(dict(zip(names, example)))
    del category_examples[3:]
    cell['examples'] = examples


def sorted_tree(node: dict[str, Any]) -> dict[str, Any]:
    """The nested deviations sorted by key, numbers by value: groups such as asset ids, then days."""
    def order(key: str) -> tuple[int, int | str]:
        return (0, int(key)) if key.isdigit() else (1, key)
    if 'examples' in node:  # a cell
        return node
    return {key: sorted_tree(node[key]) for key in sorted(node, key=order)}


def build_months(config: Config, period: Period, cuts: Cuts, results: list[WindowResult]) -> list[Month]:
    """The month files of a finished period. Fails if the windows' rows don't add up to the period's counts."""
    months = {month: Month(month=month, fingerprint=fingerprint(config), config=describe(config),
                           period=dict(name=period.name, windows=len(results),
                                       query_s=round(sum(elapsed for _, elapsed in results))),
                           rows={'old': 0, 'new': 0}, keys={}, deviations={})
              for month in period.months}
    names = example_names(config)
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
            add_to_cell(node, row, names)
    old_rows = sum(month['rows']['old'] for month in months.values())
    new_rows = sum(month['rows']['new'] for month in months.values())
    if (old_rows, new_rows) != (cuts.old_rows, cuts.new_rows):
        raise RuntimeError(f'rows of the windows (old {old_rows:,}, new {new_rows:,}) differ from the counts '
                           f'(old {cuts.old_rows:,}, new {cuts.new_rows:,})')
    for month in months.values():
        month['keys'] = {category: month['keys'][category] for category in CATEGORIES if category in month['keys']}
        month['deviations'] = sorted_tree(month['deviations'])
    return list(months.values())


def report(month: Month) -> str:
    failing = ', '.join(f'{category}={keys:,}' for category, keys in month['keys'].items() if category != 'equal')
    return (f"{month['month']}  {month['period']['windows']:>3} windows  {month['period']['query_s']:>6} s  "
            f"{month['rows']['old']:>13,} old rows  {month['rows']['new']:>13,} new rows  {failing or 'equal'}")


@dataclass
class Running:
    """A period whose windows are queued or running."""
    period: Period
    cuts: Cuts
    futures: list[Future[WindowResult]]


def finish(config_name: str, config: Config, running: Running, failing_before: int, max_failing: int) -> int:
    """Writes the months of a finished period in date order, until the failing keys of the run exceed
    max_failing; returns the failing keys of the run after it."""
    try:
        months = build_months(config, running.period, running.cuts, [future.result() for future in running.futures])
    except RuntimeError as error:
        # a failed period writes nothing and is retried on the next run
        print(f'{running.period.name}  FAILED: {error}', file=sys.stderr, flush=True)
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
    parser.add_argument('periods', nargs='*')
    parser.add_argument('--parallel', type=int, default=2)
    parser.add_argument('--max-failing-keys', type=int, default=100_000)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--print-sql', nargs=2, metavar=('PERIOD', 'WINDOW'))
    args = parser.parse_args()
    config = load_config(args.config)
    all_periods = {period.name: period for period in periods(config)}

    if args.print_sql:
        period = all_periods[args.print_sql[0]]
        print(compare_sql(config, period, cut(config, period).key_ranges[int(args.print_sql[1])]))
        return

    if config.group_by and (args.force or load_json(coverage_path(args.config), config) is None):
        groups = coverage(config)
        write_json(coverage_path(args.config), {'fingerprint': fingerprint(config), 'config': describe(config),
                                                **groups})
        print(f"coverage: {len(groups['only_in_old'])} groups only in old, {len(groups['only_in_new'])} only in new",
              file=sys.stderr)

    todo = [all_periods[name] for name in args.periods] if args.periods else list(all_periods.values())
    if not args.force:
        todo = [period for period in todo
                if any(load_json(month_path(args.config, month), config) is None for month in period.months)]
    todo_months = {month for period in todo for month in period.months}
    failing = sum(failing_keys(result['keys']) for month in months(config.start, config.cutoff)
                  if month not in todo_months
                  and (result := load_json(month_path(args.config, month), config)) is not None)
    print(f'{len(todo)} of {len(all_periods)} period(s) to run; {failing:,} failing keys so far', file=sys.stderr)

    # Periods are cut one after the other, as soon as fewer than --parallel queries are queued, and finished in
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
        for period in todo:
            if failing > args.max_failing_keys:
                break
            while len(unfinished()) >= args.parallel:
                wait(unfinished(), return_when=FIRST_COMPLETED)
                finish_ready()
            try:
                cuts = cut(config, period)
            except RuntimeError as error:
                print(f'{period.name}  FAILED to cut: {error}', file=sys.stderr, flush=True)
                continue
            futures = [pool.submit(run_window, config, period, key_range) for key_range in cuts.key_ranges]
            queue.append(Running(period, cuts, futures))
            finish_ready()
        while queue and failing <= args.max_failing_keys:
            wait(unfinished(), return_when=FIRST_COMPLETED)
            finish_ready()
    except KeyboardInterrupt:
        # Ctrl+C also reached the running clickhouse-clients; os._exit skips the join on the worker threads
        pool.shutdown(wait=False, cancel_futures=True)
        print('interrupted; finished periods are written', file=sys.stderr, flush=True)
        os._exit(130)
    if failing > args.max_failing_keys:
        print(f'stopped: {failing:,} failing keys, more than --max-failing-keys {args.max_failing_keys:,}',
              file=sys.stderr)
    pool.shutdown(cancel_futures=True)  # after a stop, waits for the queries already running


if __name__ == '__main__':
    main()
