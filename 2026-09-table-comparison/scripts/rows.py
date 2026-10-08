"""Print the actual rows of both tables for a few key hashes, on every shard, to see what differs.
The hashes, days and windows come from summary.py ("samples"). The calendar month of <day> is scanned, within
the window if one is given; a source that ranks rows (sql/sources/) is expensive without it.

usage: rows.py <config> <day> <hash> [hash ...] [--window N] [--print-sql]
"""
from __future__ import annotations

import argparse

from common import load_bounds, load_config, rows_sql, run_query, windows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('day', help='YYYY-MM-DD')
    parser.add_argument('key_hashes', nargs='+')
    parser.add_argument('--window', type=int)
    parser.add_argument('--print-sql', action='store_true')
    args = parser.parse_args()
    config = load_config(args.config)
    key_range = '1'
    if args.window is not None:
        bounds = load_bounds(args.config, config)
        if bounds is None:
            raise SystemExit('no current bounds for this config; run compare.py first')
        key_range = windows(bounds)[args.window].key_range
    sql = rows_sql(config, args.day, [key_hash.upper() for key_hash in args.key_hashes], key_range)
    print(sql if args.print_sql else run_query(sql, 'PrettyCompactNoEscapes'))


if __name__ == '__main__':
    main()
