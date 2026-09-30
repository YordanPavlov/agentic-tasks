"""Tier 2 drill-down: print the actual rows of both sides for sample key hashes (the `samples` of Tier 2).

usage: rows.py <config> <group> <kh> [kh ...] [--key old|new]
"""
import sys
from common import load_cfg, meta, rekey, ch, key_expr, side_where
from tier2 import group_where

if __name__ == '__main__':
    args = sys.argv[3:]; key = None
    if '--key' in args: i = args.index('--key'); key = args[i + 1]; del args[i:i + 2]
    cfg = load_cfg(sys.argv[1]); m = meta(cfg)
    if key: m = rekey(m, key)
    lo, hi, extra = group_where(cfg, sys.argv[2])
    khs = ', '.join(args)
    # explicit column list: SELECT * skips MATERIALIZED columns and differs between sides
    cols = ', '.join(m['key'] + m['vals'])
    for side in ('old', 'new'):
        print(f'== {side}')
        print(ch(f"SELECT {key_expr(m)} kh, {cols} FROM {cfg[side]} "
                 f"WHERE {side_where(cfg, side, lo, hi, extra)} AND kh IN ({khs}) ORDER BY kh", fmt='PrettyCompactNoEscapes'))
