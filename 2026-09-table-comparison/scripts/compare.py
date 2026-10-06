"""Compare old and new window by window: the key space is split into ranges of about equal rows, and one query per
range reads it from each side once and categorizes every key. The bounds and the results are cached, so an
interrupted run resumes where it stopped.

usage: compare.py <config> [N ...] [--parallel N] [--force] [--print-sql]
  N             only these windows, by number (default: all)
  --parallel N  queries in flight at once (default 2, the limit for long queries on prod)
  --force       re-run windows that are already cached, e.g. after the data changed
  --print-sql   print the query of the first selected window and exit
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from common import (OK_CATEGORIES, Bounds, ResultRow, Window, WindowResult, bounds_fingerprint, compare_sql,
                    compute_bounds, load_bounds, load_config, load_result, run_query_json, save_bounds, save_result,
                    windows)
from configs import Config


def run_window(config_name: str, config: Config, bounds: Bounds, window: Window) -> WindowResult:
    started = time.monotonic()
    rows: list[ResultRow] = []
    for row in run_query_json(compare_sql(config, window), timeout_s=7200):
        hashes = row['key_hashes']
        rows.append(ResultRow(
            host=str(row['host']), day=str(row['day']), category=str(row['category']), keys=int(str(row['keys'])),
            old_rows=int(str(row['old_rows'])), new_rows=int(str(row['new_rows'])),
            key_hashes=[str(key_hash) for key_hash in hashes] if isinstance(hashes, list) else []))
    result = WindowResult(fingerprint=bounds_fingerprint(bounds), elapsed_s=round(time.monotonic() - started, 1),
                          rows=rows)
    save_result(config_name, window, result)
    return result


def report(window: Window, result: WindowResult) -> str:
    """One line per window: time, keys and the categories that are not OK."""
    total_keys = sum(row['keys'] for row in result['rows'])
    differing: dict[str, int] = {}
    for row in result['rows']:
        if row['category'] not in OK_CATEGORIES:
            differing[row['category']] = differing.get(row['category'], 0) + row['keys']
    differences = ', '.join(f'{category}={keys:,}' for category, keys in sorted(differing.items()))
    return (f'{window.name}  {result["elapsed_s"]:>7.1f} s  {total_keys:>13,} keys  '
            f'{differences or "no differences"}  (from {window.label})')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('windows', nargs='*', type=int)
    parser.add_argument('--parallel', type=int, default=2)
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--print-sql', action='store_true')
    args = parser.parse_args()
    config = load_config(args.config)

    # computed once and kept, even with --force: windows from other bounds would not add up with cached ones
    bounds = load_bounds(args.config, config)
    if bounds is None:
        started = time.monotonic()
        bounds = compute_bounds(config)
        save_bounds(args.config, bounds)
        print(f'bounds: {len(bounds["bounds"]) + 1} windows on ({", ".join(bounds["columns"])}), '
              f'{time.monotonic() - started:.0f} s', file=sys.stderr)

    todo = [window for window in windows(bounds) if not args.windows or window.index in args.windows]
    if args.print_sql:
        if todo:
            print(compare_sql(config, todo[0]))
        return
    if not args.force:
        todo = [window for window in todo if load_result(args.config, bounds, window) is None]
    print(f'{len(todo)} of {len(windows(bounds))} window(s) to run', file=sys.stderr)

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = [(window, pool.submit(run_window, args.config, config, bounds, window)) for window in todo]
        for window, future in futures:
            try:
                print(report(window, future.result()), flush=True)
            except RuntimeError as error:
                # a failed window is left uncached and retried on the next run
                print(f'{window.name}  FAILED: {error}', file=sys.stderr, flush=True)


if __name__ == '__main__':
    main()
