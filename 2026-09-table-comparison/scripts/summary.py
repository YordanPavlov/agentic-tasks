"""Summarize the results of compare.py: coverage, the failing months, the failing cells of the first failing month,
keys per category and a verdict.

usage: summary.py <config> [--lines N]
  --lines N   failing groups (or days, without group_by) of the first failing month to list (default 30, the most
              failing keys first)
"""
from __future__ import annotations

import argparse
from collections import Counter

from common import (CATEGORIES, Month, cell_failing_keys, cells, coverage_path, failing_keys, load_config,
                    load_json, month_path, months, range_days)

MEANING = {
    'missing_in_new': 'key only in old',
    'missing_in_old': 'key only in new',
    'differs': "the sums of the key's values differ by more than the tolerance",
    'equal': 'equal within the tolerance',
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('config')
    parser.add_argument('--lines', type=int, default=30)
    args = parser.parse_args()
    config = load_config(args.config)
    print(f'# {config.old} vs {config.new}, {config.start} .. {config.cutoff} (exclusive)')

    if config.group_by:
        groups = load_json(coverage_path(args.config))
        if groups is None:
            print('\ncoverage: not computed; run compare.py')
        else:
            print(f"\ncoverage ({', '.join(config.group_by)}): {len(groups['only_in_old'])} groups only in old, "
                  f"{len(groups['only_in_new'])} only in new" + (', not compared' if config.common_groups_only
                                                                    else '') + f'; see {coverage_path(args.config)}')

    results: list[Month] = []
    missing = []
    for month in months(config.start, config.cutoff):
        result = load_json(month_path(args.config, month))
        if result is None:
            missing.append(month)
        elif 'days' not in result:
            raise SystemExit(f'{month_path(args.config, month)} is in an older format; re-run with --force')
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
        entries = list(cells(first.get('groups') or first['days']))
        largest = sorted(entries, key=lambda entry: -cell_failing_keys(entry[1]))[:args.lines]
        print(f"\nfirst failing month {first['month']}: {len(entries):,} failing "
              f"{'groups' if config.group_by else 'days'}; the {len(largest)} largest:")
        for path, cell in largest:
            where = ' '.join(f'{column}={value}' for column, value in zip(config.group_by, path)) or path[0]
            parts = []
            for category, stats in cell.items():
                details = {name: value for name, value in stats.items() if name not in ('keys', 'days') and value}
                parts.append(f"{category}={stats['keys']:,}"
                             + (f" ({', '.join(f'{name} {value:,}' for name, value in details.items())})"
                                if details else '')
                             + (f" on {range_days(stats['days'])} days: {', '.join(stats['days'][:3])}"
                                f"{' …' if len(stats['days']) > 3 else ''}" if 'days' in stats else ''))
            print(f"  {where}  {'; '.join(parts)}")

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
