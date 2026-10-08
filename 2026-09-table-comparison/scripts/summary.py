"""Summarize the cached results of compare.py: coverage, keys per category, failing days, keys that moved
between shards, sample key hashes for rows.py, and a verdict.

usage: summary.py <config> [--days N]
  --days N   failing days to list (default 30, the largest first)
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict

from common import MAX_KEY_HASHES, OK_CATEGORIES, ResultRow, load_bounds, load_config, load_result, windows

# Order in which categories are printed, roughly from most to least serious.
CATEGORY_ORDER = ('only_old', 'only_new', 'value_diff', 'float_diff', 'new_multi', 'old_multi', 'soft_diff',
                  'float_noise', 'equal')
MEANING = {
    'only_old': 'key only in old: lost in new',
    'only_new': 'key only in new: added in new',
    'value_diff': 'one version each, a non-float value differs',
    'float_diff': 'one version each, a float differs by more than the tolerance',
    'new_multi': "new has several different rows for the key: the key is coarser than new's sorting key",
    'old_multi': "old has several different rows for the key: the key is coarser than old's sorting key",
    'soft_diff': 'only a soft value differs (config.soft_values); passes',
    'float_noise': 'floats differ within the tolerance',
    'equal': 'identical',
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('--days', type=int, default=30)
    args = parser.parse_args()
    config = load_config(args.config)

    print(f'# {config.old} vs {config.new}, {config.start} .. {config.cutoff} (exclusive)')
    bounds = load_bounds(args.config, config)
    if bounds is None:
        print('\nverdict: INCOMPLETE: no windows yet, or the config changed; run compare.py')
        return

    # Aggregated window by window: all rows with their key hashes do not fit in memory.
    keys_per_category = Counter[str]()
    keys_per_host = Counter[str]()
    old_rows = new_rows = 0
    # only_old on one shard and only_new on another is the same key placed differently, not lost
    moved_hashes = {'only_old': set[str](), 'only_new': set[str]()}
    moved_complete = True  # False when some hash lists were capped
    differences_per_day: dict[str, Counter[str]] = defaultdict(Counter)
    samples: dict[str, tuple[ResultRow, int]] = {}  # category -> (row with the most keys, window index)
    missing_windows: list[str] = []
    query_time_s = 0.0
    for window in windows(bounds):
        result = load_result(args.config, bounds, window)
        if result is None:
            missing_windows.append(window.name)
            continue
        query_time_s += result['elapsed_s']
        for row in result['rows']:
            category = row['category']
            keys_per_category[category] += row['keys']
            keys_per_host[row['host']] += row['keys']
            old_rows += row['old_rows']
            new_rows += row['new_rows']
            if category in moved_hashes:
                moved_complete &= len(row['key_hashes']) == row['keys']
                moved_hashes[category].update(row['key_hashes'])
            if category not in OK_CATEGORIES:
                differences_per_day[row['day']][category] += row['keys']
            if ((category not in OK_CATEGORIES or category == 'soft_diff') and row['key_hashes']
                    and (category not in samples or row['keys'] > samples[category][0]['keys'])):
                samples[category] = ({**row, 'key_hashes': row['key_hashes'][:3]}, window.index)

    window_count = len(windows(bounds))
    missing_list = f": {', '.join(missing_windows[:12])}{' …' if len(missing_windows) > 12 else ''}"
    print(f'windows: {window_count - len(missing_windows)} done, {len(missing_windows)} missing'
          + (missing_list if missing_windows else '') + f'; query time {query_time_s / 60:.0f} min')

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
        moved = len(moved_hashes['only_old'] & moved_hashes['only_new'])
        caveat = f' (lower bound: hash lists are capped at {MAX_KEY_HASHES:,} per window, shard, day and category)'
        print(f'\nkeys only_old on one shard and only_new on another: {moved:,}' + ('' if moved_complete else caveat))

    if differences_per_day:
        largest_days = sorted(differences_per_day.items(), key=lambda item: -sum(item[1].values()))[:args.days]
        print(f'\nfailing days: {len(differences_per_day):,}; the {len(largest_days)} largest:')
        for day, categories in largest_days:
            print(f'  {day}  ' + ', '.join(f'{category}={keys:,}' for category, keys in categories.most_common()))
        print('\nsamples:')
        for row, window_index in sorted(samples.values(), key=lambda sample: -sample[0]['keys']):
            print(f"  {row['category']:<12} python3 rows.py {args.config} {row['day']} "
                  f"--window {window_index} {' '.join(row['key_hashes'])}")

    if missing_windows:
        verdict = 'INCOMPLETE: windows missing'
    elif not differences_per_day:
        verdict = 'PASS: every key equal (floats within tolerance)'
    else:
        verdict = 'DIFFERS: explain the categories above'
    print(f'\nverdict: {verdict}')


if __name__ == '__main__':
    main()
