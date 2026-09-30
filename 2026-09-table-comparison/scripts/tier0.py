"""Tier 0 (metadata only, seconds): schema diff, comparison key and value columns, coverage per side,
monthly row estimates, window plan. Run first; re-run with --reset after changing the config's key/exclude/cutoff.

usage: tier0.py <config> [--reset]
"""
import os, sys
from common import load_cfg, cfg_dir, meta, month_rows, windows, ch

if __name__ == '__main__':
    cfg = load_cfg(sys.argv[1])
    if '--reset' in sys.argv:
        for f in ('meta.json', 'windows.json'):
            p = os.path.join(cfg_dir(cfg['name']), f)
            if os.path.exists(p): os.remove(p)
    m = meta(cfg)
    for side in ('old', 'new'):
        s = m[side]
        print(f"{side}: {cfg[side]} -> {s['shard']} ({s['engine']})\n     sorting key: {', '.join(s['key'])}")
    print(f"comparison key ({cfg['key']}): {', '.join(m['key'])}")
    if m['old']['key'] != m['new']['key']: print('  !! sorting keys differ: see "Key changes" in table-comparison.md')
    print(f"value columns: {', '.join(m['vals'])}")
    for k in ('only_old', 'only_new', 'type_diff'):
        if m[k]: print(f'  !! {k}: {m[k]}')
    print(f"shards: {m['shards']}")

    mr = month_rows(cfg, m); dmaxes = []
    for side in ('old', 'new'):
        db, t = m[side]['shard'].split('.', 1)
        lo, hi, n, parts, l0, mod = ch(
            f"SELECT min(min_time), max(max_time), sum(rows), count(), countIf(level = 0), max(modification_time) "
            f"FROM system.parts WHERE database='{db}' AND table='{t}' AND active").strip().split('\t')
        dmax = ch(f"SELECT max({cfg['dt']}) FROM {cfg[side]} WHERE {cfg['dt']} >= toDateTime('{hi}') - INTERVAL 30 DAY").strip()
        dmaxes.append(dmax)
        print(f'{side}: local shard {lo} .. {hi}, {int(n):,} rows in {parts} active parts ({l0} unmerged level-0), '
              f'last part written {mod}; all shards max {cfg["dt"]} = {dmax}')
    print(f'suggested cutoff: {min(dmaxes)[:10]} minus a few days (earlier max {cfg["dt"]} of the two sides)')
    print('rows per month: local shard x shards, a rough estimate (off by up to ~40% per window)')
    print('month       old rows      new rows   new/old')
    for mo in sorted(mr['old'].keys() | mr['new'].keys()):
        o, n = mr['old'].get(mo, 0), mr['new'].get(mo, 0)
        ok = (o and 0.8 < n / o < 1.25) or (cfg['cutoff'] and mo >= cfg['cutoff'][:7])
        flag = '' if ok else '  <--'
        print(f'{mo[:7]}  {o:13,} {n:13,}   {n / o if o else float("inf"):6.2f}{flag}')

    if cfg['start'] and cfg['cutoff']:
        w = windows(cfg)
        print(f"window plan: {len(w)} windows per side, {cfg['start']} .. {cfg['cutoff']} (cached in windows.json)")
    else:
        print('no window plan: set start and cutoff in the config, then re-run with --reset')
