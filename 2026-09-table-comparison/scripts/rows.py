"""Print the actual rows of both tables for a few key hashes, on every shard, to see what differs.
The hashes and days come from summary.py ("samples"). The whole calendar month of <day> is scanned.

usage: rows.py <config> <day> <hash> [hash ...] [--print-sql]
"""
from __future__ import annotations

import argparse

from common import load_config, rows_sql, run_query


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('day', help='YYYY-MM-DD')
    parser.add_argument('key_hashes', nargs='+')
    parser.add_argument('--print-sql', action='store_true')
    args = parser.parse_args()
    config = load_config(args.config)
    sql = rows_sql(config, args.day, [key_hash.upper() for key_hash in args.key_hashes])
    print(sql if args.print_sql else run_query(sql, 'PrettyCompactNoEscapes'))


if __name__ == '__main__':
    main()
