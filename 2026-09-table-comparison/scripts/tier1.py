"""Tier 1: per-day (and split_by), dedup-invariant fingerprints of each side over the full history.
Per group: rows, uniqExact/min/max block, uniqExact tx, uniqExact key, sumDistinct(hash(key, truncated content)).
One worker per side (2 long queries in flight). Resumable: one cached TSV per (side, window).

usage: tier1.py <config> [lo hi]    optional lo/hi restrict to windows inside that date range
"""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor
from common import load_cfg, cfg_dir, meta, windows, ch, key_expr, vals_expr, side_where

COLS = ['rows', 'blocks', 'bmin', 'bmax', 'txs', 'keys', 'fp']


def build_sql(cfg, m):
    b, tx, dt = cfg['block'], cfg['tx'], cfg['dt']
    split = ''.join(f'toString({c}) s_{i}, ' for i, c in enumerate(cfg['split_by']))
    split_g = ''.join(f', s_{i}' for i in range(len(cfg['split_by'])))
    inner = ', '.join([dt] + [c for c in (b, tx) if c] + cfg['split_by'])
    blocks = f'uniqExact({b}) blocks, min({b}) bmin, max({b}) bmax' if b else '0 blocks, 0 bmin, 0 bmax'
    txs = f'uniqExact(cityHash64({tx})) txs' if tx else '0 txs'
    return f"""
SELECT toDate({dt}) d, {split}count() rows, {blocks}, {txs}, uniqExact(kh) keys, sumDistinct(vh) fp
FROM (SELECT {inner}, {key_expr(m)} kh, cityHash64(kh, {vals_expr(m)}) vh
      FROM {{table}} WHERE {{where}})
GROUP BY d{split_g} ORDER BY d{split_g}"""


def run_side(cfg, side, wins, sql):
    out = cfg_dir(cfg['name'], 'tier1')
    t_start = time.time()
    for i, (lo, hi) in enumerate(wins):
        path = os.path.join(out, f'{side}_{lo}_{hi}.tsv')
        if os.path.exists(path): continue
        t0 = time.time()
        res = ch(sql.format(table=cfg[side], where=side_where(cfg, side, lo, hi)))
        open(path + '.tmp', 'w').write(res); os.rename(path + '.tmp', path)
        print(f'{side} {i + 1}/{len(wins)} {lo}..{hi} {time.time() - t0:.1f}s (total {time.time() - t_start:.0f}s)', flush=True)
    return time.time() - t_start


if __name__ == '__main__':
    cfg = load_cfg(sys.argv[1]); m = meta(cfg)
    wins = windows(cfg)
    if len(sys.argv) > 3: wins = [w for w in wins if w[0] >= sys.argv[2] and w[1] <= sys.argv[3]]
    sql = build_sql(cfg, m)
    print(f'{len(wins)} windows per side', flush=True)
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {s: ex.submit(run_side, cfg, s, wins, sql) for s in ('old', 'new')}
        for s, f in futs.items(): print(f'{s} done in {f.result():.0f}s', flush=True)
