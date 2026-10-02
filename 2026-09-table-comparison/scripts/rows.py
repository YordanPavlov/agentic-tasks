"""Print the actual rows of both tables for a few key hashes, on every shard, to see what differs.
The hashes and days come from summary.py ("samples"). The whole month of <day> is scanned.

usage: rows.py <config> <day> <hash> [hash ...] [--print-sql]
"""
from __future__ import annotations

import argparse

from common import ch, load_cfg, rows_sql, window_of


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('config')
    ap.add_argument('day', help='YYYY-MM-DD')
    ap.add_argument('hashes', nargs='+')
    ap.add_argument('--print-sql', action='store_true')
    a = ap.parse_args()
    cfg = load_cfg(a.config)
    sql = rows_sql(cfg, window_of(cfg, a.day), [h.upper() for h in a.hashes])
    print(sql if a.print_sql else ch(sql, 'PrettyCompactNoEscapes'))


if __name__ == '__main__':
    main()
