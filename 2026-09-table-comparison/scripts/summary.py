"""Summarize the cached results of compare.py: coverage, keys per category, failing days, keys that moved
between shards or months, sample key hashes for rows.py, and a verdict.

usage: summary.py <config> [--days N]
  --days N   failing days to list (default 30, the largest first)
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict

from common import MAX_KEYS, OK_CATEGORIES, ResultRow, load_cfg, load_result, windows

# Order in which categories are printed, roughly from most to least serious.
CATEGORY_ORDER = ('only_old', 'only_new', 'value_diff', 'float_diff', 'new_multi', 'old_multi',
                  'old_multi_new_matches', 'float_noise', 'equal')
MEANING = {
    'only_old': 'key only in old: lost in new',
    'only_new': 'key only in new: added in new',
    'value_diff': 'one version each, a non-float value differs',
    'float_diff': 'one version each, a float differs by more than the tolerance',
    'new_multi': 'new has several versions of the key',
    'old_multi': 'old has several versions of the key, none equals new',
    'old_multi_new_matches': "old has several versions of the key, one equals new (old's duplicates)",
    'float_noise': 'floats differ within the tolerance',
    'equal': 'identical',
}


def moved_keys(rows: list[ResultRow]) -> tuple[int, bool]:
    """Keys that are only_old in one (shard, month) and only_new in another, i.e. the same key placed
    differently, not lost. Returns (count, complete); complete is False when some only_* lists were truncated."""
    places: dict[str, dict[str, set[tuple[str, str]]]] = {'only_old': defaultdict(set), 'only_new': defaultdict(set)}
    complete = True
    for r in rows:
        if r['category'] in places:
            complete &= len(r['sample']) == r['keys']
            for h in r['sample']:
                places[r['category']][h].add((r['host'], r['day'][:7]))
    return len(places['only_old'].keys() & places['only_new'].keys()), complete


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('config')
    ap.add_argument('--days', type=int, default=30)
    a = ap.parse_args()
    cfg = load_cfg(a.config)

    rows: list[ResultRow] = []
    missing: list[str] = []
    elapsed = 0.0
    for w in windows(cfg):
        res = load_result(a.config, cfg, w)
        if res is None:
            missing.append(w.name)
        else:
            rows += res['rows']
            elapsed += res['elapsed_s']

    print(f'# {cfg.old} vs {cfg.new}, {cfg.start} .. {cfg.cutoff} (exclusive)')
    print(f'months: {len(windows(cfg)) - len(missing)} done, {len(missing)} missing or stale'
          + (f": {', '.join(missing[:12])}{' …' if len(missing) > 12 else ''}" if missing else '')
          + f'; query time {elapsed / 60:.0f} min')

    keys, old_rows, new_rows = Counter[str](), 0, 0
    for r in rows:
        keys[r['category']] += r['keys']
        old_rows += r['old_rows']
        new_rows += r['new_rows']
    old_keys = sum(n for c, n in keys.items() if c != 'only_new')
    new_keys = sum(n for c, n in keys.items() if c != 'only_old')
    print(f'\nold: {old_rows:,} rows, {old_keys:,} keys, {old_rows - old_keys:,} duplicate rows')
    print(f'new: {new_rows:,} rows, {new_keys:,} keys, {new_rows - new_keys:,} duplicate rows')
    per_host = Counter[str]()
    for r in rows:
        per_host[r['host']] += r['keys']
    print('keys per host: ' + ', '.join(f'{h} {n:,}' for h, n in sorted(per_host.items())))

    print('\n| category | keys | meaning |\n|---|---:|---|')
    for c in sorted(keys, key=lambda c: CATEGORY_ORDER.index(c) if c in CATEGORY_ORDER else 99):
        print(f'| {c} | {keys[c]:,} | {MEANING.get(c, "")} |')

    moved, complete = moved_keys(rows)
    if keys['only_old'] or keys['only_new']:
        print(f'\nkeys only_old in one shard/month and only_new in another: {moved:,}'
              + ('' if complete else f' (lower bound: hash lists are capped at {MAX_KEYS:,} per shard, day and category)'))

    bad = [r for r in rows if r['category'] not in OK_CATEGORIES]
    per_day: dict[str, Counter[str]] = defaultdict(Counter)
    for r in bad:
        per_day[r['day']][r['category']] += r['keys']
    if per_day:
        print(f'\nfailing days: {len(per_day):,}; the {min(a.days, len(per_day))} largest:')
        for day, cats in sorted(per_day.items(), key=lambda kv: -sum(kv[1].values()))[:a.days]:
            print(f'  {day}  ' + ', '.join(f'{c}={n:,}' for c, n in cats.most_common()))
        print('\nsamples (python3 rows.py <config> <day> <hash> ...):')
        shown: set[str] = set()
        for r in sorted(bad, key=lambda r: -r['keys']):
            if r['category'] not in shown and r['sample']:
                shown.add(r['category'])
                print(f"  {r['category']:<22} {r['day']}  {' '.join(r['sample'][:3])}")

    if missing:
        verdict = 'INCOMPLETE: months missing'
    elif not per_day:
        verdict = 'PASS: every key equal (floats within tolerance)'
    elif set(keys) - set(OK_CATEGORIES) == {'old_multi_new_matches'}:
        verdict = "PASS with caveat: differences are only old's duplicates"
    else:
        verdict = 'DIFFERS: explain the categories above'
    print(f'\nverdict: {verdict}')


if __name__ == '__main__':
    main()
