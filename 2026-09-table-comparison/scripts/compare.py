"""Compare old and new month by month: one query per month reads each side once and categorizes every key.
Results are cached per month, so an interrupted run resumes where it stopped.

usage: compare.py <config> [YYYY-MM ...] [--parallel N] [--force] [--print-sql]
  YYYY-MM       only these months (default: every month from start to cutoff)
  --parallel N  queries in flight at once (default 2, the limit for long queries on prod)
  --force       re-run months that are already cached, e.g. after the data changed
  --print-sql   print the query of the first selected month and exit
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from common import (OK_CATEGORIES, MonthResult, ResultRow, Window, ch_json, compare_sql, fingerprint, load_cfg,
                    load_result, save_result, windows)
from configs import Config


def run_window(name: str, cfg: Config, w: Window) -> MonthResult:
    t0 = time.monotonic()
    rows: list[ResultRow] = [
        ResultRow(host=str(r['host']), day=str(r['day']), category=str(r['category']), keys=int(str(r['keys'])),
                  old_rows=int(str(r['old_rows'])), new_rows=int(str(r['new_rows'])),
                  sample=[str(h) for h in r['sample']] if isinstance(r['sample'], list) else [])
        for r in ch_json(compare_sql(cfg, w), timeout=7200)]
    result = MonthResult(fingerprint=fingerprint(cfg), lo=str(w.lo), hi=str(w.hi),
                         elapsed_s=round(time.monotonic() - t0, 1), rows=rows)
    save_result(name, w, result)
    return result


def report(w: Window, r: MonthResult) -> str:
    """One line per month: time, keys and the categories that are not OK."""
    keys = sum(x['keys'] for x in r['rows'])
    bad: dict[str, int] = {}
    for x in r['rows']:
        if x['category'] not in OK_CATEGORIES:
            bad[x['category']] = bad.get(x['category'], 0) + x['keys']
    diffs = ', '.join(f'{c}={n:,}' for c, n in sorted(bad.items())) or 'no differences'
    return f'{w.name}  {r["elapsed_s"]:>7.1f} s  {keys:>13,} keys  {diffs}'


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('config')
    ap.add_argument('months', nargs='*')
    ap.add_argument('--parallel', type=int, default=2)
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--print-sql', action='store_true')
    a = ap.parse_args()
    cfg = load_cfg(a.config)

    todo = [w for w in windows(cfg) if not a.months or w.name in a.months]
    if a.print_sql:
        if todo:
            print(compare_sql(cfg, todo[0]))
        return
    if not a.force:
        todo = [w for w in todo if load_result(a.config, cfg, w) is None]
    print(f'{len(todo)} month(s) to run', file=sys.stderr)

    with ThreadPoolExecutor(max_workers=a.parallel) as pool:
        futures = [(w, pool.submit(run_window, a.config, cfg, w)) for w in todo]
        for w, f in futures:
            try:
                print(report(w, f.result()), flush=True)
            except RuntimeError as e:
                # a failed month is left uncached and retried on the next run
                print(f'{w.name}  FAILED: {e}', file=sys.stderr, flush=True)


if __name__ == '__main__':
    main()
