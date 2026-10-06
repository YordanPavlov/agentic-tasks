"""Summarize the cached results of compare.py: coverage, keys per category, failing days, keys that moved
between shards or months, sample key hashes for rows.py, and a verdict.

usage: summary.py <config> [--days N]
  --days N   failing days to list (default 30, the largest first)
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict

from common import MAX_KEY_HASHES, OK_CATEGORIES, ResultRow, load_config, load_result, windows

# Order in which categories are printed, roughly from most to least serious.
CATEGORY_ORDER = ('only_old', 'only_new', 'value_diff', 'float_diff', 'new_multi', 'old_multi', 'float_noise',
                  'equal')
MEANING = {
    'only_old': 'key only in old: lost in new',
    'only_new': 'key only in new: added in new',
    'value_diff': 'one version each, a non-float value differs',
    'float_diff': 'one version each, a float differs by more than the tolerance',
    'new_multi': "new has several different rows for the key: the key is coarser than new's sorting key",
    'old_multi': "old has several different rows for the key: the key is coarser than old's sorting key",
    'float_noise': 'floats differ within the tolerance',
    'equal': 'identical',
}


def moved_keys(rows: list[ResultRow]) -> tuple[int, bool]:
    """Keys that are only_old in one (shard, month) and only_new in another, i.e. the same key placed
    differently, not lost. Returns (count, complete); complete is False when some hash lists were capped."""
    hashes = {'only_old': set[str](), 'only_new': set[str]()}
    complete = True
    for row in rows:
        if row['category'] in hashes:
            complete &= len(row['key_hashes']) == row['keys']
            hashes[row['category']].update(row['key_hashes'])
    return len(hashes['only_old'] & hashes['only_new']), complete


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('--days', type=int, default=30)
    args = parser.parse_args()
    config = load_config(args.config)

    rows: list[ResultRow] = []
    missing_months: list[str] = []
    query_time_s = 0.0
    for window in windows(config):
        result = load_result(args.config, config, window)
        if result is None:
            missing_months.append(window.name)
        else:
            rows += result['rows']
            query_time_s += result['elapsed_s']

    month_count = len(windows(config))
    missing_list = f": {', '.join(missing_months[:12])}{' …' if len(missing_months) > 12 else ''}"
    print(f'# {config.old} vs {config.new}, {config.start} .. {config.cutoff} (exclusive)')
    print(f'months: {month_count - len(missing_months)} done, {len(missing_months)} missing or stale'
          + (missing_list if missing_months else '') + f'; query time {query_time_s / 60:.0f} min')

    keys_per_category = Counter[str]()
    keys_per_host = Counter[str]()
    old_rows = new_rows = 0
    for row in rows:
        keys_per_category[row['category']] += row['keys']
        keys_per_host[row['host']] += row['keys']
        old_rows += row['old_rows']
        new_rows += row['new_rows']
    old_keys = sum(keys for category, keys in keys_per_category.items() if category != 'only_new')
    new_keys = sum(keys for category, keys in keys_per_category.items() if category != 'only_old')
    # rows as FINAL shows them; more rows than keys means the configured key is coarser than the sorting key
    print(f'\nold: {old_rows:,} rows, {old_keys:,} keys')
    print(f'new: {new_rows:,} rows, {new_keys:,} keys')
    print('keys per host: ' + ', '.join(f'{host} {keys:,}' for host, keys in sorted(keys_per_host.items())))

    def print_order(category: str) -> int:
        return CATEGORY_ORDER.index(category) if category in CATEGORY_ORDER else len(CATEGORY_ORDER)
    print('\n| category | keys | meaning |\n|---|---:|---|')
    for category in sorted(keys_per_category, key=print_order):
        print(f'| {category} | {keys_per_category[category]:,} | {MEANING.get(category, "")} |')

    if keys_per_category['only_old'] or keys_per_category['only_new']:
        moved, complete = moved_keys(rows)
        caveat = f' (lower bound: hash lists are capped at {MAX_KEY_HASHES:,} per shard, day and category)'
        print(f'\nkeys only_old in one shard/month and only_new in another: {moved:,}' + ('' if complete else caveat))

    differing_rows = [row for row in rows if row['category'] not in OK_CATEGORIES]
    differences_per_day: dict[str, Counter[str]] = defaultdict(Counter)
    for row in differing_rows:
        differences_per_day[row['day']][row['category']] += row['keys']
    if differences_per_day:
        largest_days = sorted(differences_per_day.items(), key=lambda item: -sum(item[1].values()))[:args.days]
        print(f'\nfailing days: {len(differences_per_day):,}; the {len(largest_days)} largest:')
        for day, categories in largest_days:
            print(f'  {day}  ' + ', '.join(f'{category}={keys:,}' for category, keys in categories.most_common()))
        print('\nsamples (python3 rows.py <config> <day> <hash> ...):')
        shown: set[str] = set()
        for row in sorted(differing_rows, key=lambda row: -row['keys']):
            if row['category'] not in shown and row['key_hashes']:
                shown.add(row['category'])
                print(f"  {row['category']:<22} {row['day']}  {' '.join(row['key_hashes'][:3])}")

    if missing_months:
        verdict = 'INCOMPLETE: months missing'
    elif not differences_per_day:
        verdict = 'PASS: every key equal (floats within tolerance)'
    else:
        verdict = 'DIFFERS: explain the categories above'
    print(f'\nverdict: {verdict}')


if __name__ == '__main__':
    main()
