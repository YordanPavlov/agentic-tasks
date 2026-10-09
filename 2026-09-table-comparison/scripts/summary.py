"""Summarize the results of compare.py: coverage, the failing months, the failing cells of the first failing month,
keys per category and a verdict.

usage: summary.py <config> [--lines N]
  --lines N   failing groups (or days, without group_by) of the first failing month to list (default 30, the most
              failing keys first)
"""
from __future__ import annotations

import argparse
from collections import Counter
from typing import Any, Iterator

from common import (CATEGORIES, Month, coverage_path, failing_keys, load_config, load_json, month_path,
                    months)

MEANING = {
    'missing_in_new': 'key only in old',
    'missing_in_old': 'key only in new',
    'differs': 'one row each, the values differ by more than the tolerance',
    'new_multi': "new has several rows for the key: the key is coarser than new's sorting key",
    'old_multi': "old has several rows for the key: the key is coarser than old's sorting key",
    'equal': 'equal within the tolerance',
}


def cells(node: dict[str, Any], path: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], dict[str, Any]]]:
    """(group values and day, cell) of the nested deviations."""
    if 'examples' in node:
        yield path, node
        return
    for key, child in node.items():
        yield from cells(child, path + (key,))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('--lines', type=int, default=30)
    args = parser.parse_args()
    config = load_config(args.config)
    print(f'# {config.old} vs {config.new}, {config.start} .. {config.cutoff} (exclusive)')

    if config.group_by:
        groups = load_json(coverage_path(args.config), config)
        if groups is None:
            print('\ncoverage: not computed; run compare.py')
        else:
            print(f"\ncoverage ({', '.join(config.group_by)}): {len(groups['only_in_old'])} groups only in old, "
                  f"{len(groups['only_in_new'])} only in new" + (', not compared' if config.common_groups_only
                                                                    else '') + f'; see {coverage_path(args.config)}')

    results: list[Month] = []
    missing = []
    for month in months(config.start, config.cutoff):
        result = load_json(month_path(args.config, month), config)
        if result is None:
            missing.append(month)
        else:
            results.append(result)
    print(f'\nmonths: {len(results)} done, {len(missing)} missing'
          + (f": {', '.join(missing[:12])}{' …' if len(missing) > 12 else ''}" if missing else ''))

    failing = [result for result in results if failing_keys(result['keys'])]
    if failing:
        print(f'\n{len(failing)} failing months ({len(results) - len(failing)} pass):')
        print('| month | old rows | new rows | ' + ' | '.join(CATEGORIES[:-1]) + ' |')
        print('|---|---:|---:|' + '---:|' * (len(CATEGORIES) - 1))
        for result in failing:
            print(f"| {result['month']} | {result['rows']['old']:,} | {result['rows']['new']:,} | "
                  + ' | '.join(f"{result['keys'].get(category, 0):,}" for category in CATEGORIES[:-1]) + ' |')

        # the failing cells of the first failing month, per group (config.group_by), or per day without groups
        first = failing[0]
        entries: dict[tuple[str, ...], tuple[Counter[str], list[str], float]] = {}
        for path, cell in cells(first['deviations']):
            key = path[:-1] if config.group_by else path
            counts, days, max_diff_pct = entries.get(key, (Counter[str](), [], -1.0))
            counts.update({category: cell[category] for category in CATEGORIES if category in cell})
            entries[key] = (counts, days + [path[-1]], max(max_diff_pct, cell.get('max_diff_pct', -1.0)))
        largest = sorted(entries.items(), key=lambda entry: -failing_keys(entry[1][0]))[:args.lines]
        print(f"\nfirst failing month {first['month']}: {len(entries):,} failing "
              f"{'groups' if config.group_by else 'days'}; the {len(largest)} largest:")
        for key, (counts, days, max_diff_pct) in largest:
            where = ' '.join(f'{column}={value}' for column, value in zip(config.group_by, key))
            if config.group_by:
                where += f'  {min(days)}..{max(days)} ({len(days)} days)' if len(days) > 1 else f'  {days[0]}'
            else:
                where = key[-1]
            counts_text = ', '.join(f'{category}={counts[category]:,}' for category in CATEGORIES if counts[category])
            max_text = f', max {max_diff_pct}%' if max_diff_pct >= 0 else ''
            print(f'  {where}  {counts_text}{max_text}')
        print(f"  examples: {month_path(args.config, first['month'])}")

    keys = Counter[str]()
    rows = Counter[str]()
    for result in results:
        keys.update(result['keys'])
        rows.update(result['rows'])
    print(f"\nold: {rows['old']:,} rows; new: {rows['new']:,} rows")
    print('\n| category | keys | meaning |\n|---|---:|---|')
    for category in CATEGORIES:
        if keys[category]:
            print(f'| {category} | {keys[category]:,} | {MEANING[category]} |')

    if failing:
        verdict = 'DIFFERS: explain the categories above' + (f'; {len(missing)} months not compared' if missing else '')
    elif missing:
        verdict = 'INCOMPLETE: months missing'
    else:
        verdict = 'PASS: every key equal within the tolerance'
    print(f'\nverdict: {verdict}')


if __name__ == '__main__':
    main()
