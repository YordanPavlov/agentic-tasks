# Old-vs-new table comparison

Compares two versions of a ClickHouse table key by key, over the full history, reading each partition once.

## Method

For each month (the tables' partition), one query:
1. reduces every row of both tables to a key hash, a hash of the values and the float values,
2. groups by the key hash, so the old and new versions of a key land in one row,
3. gives each key a category,
4. counts keys per day and category, keeping up to 10,000 key hashes per category for drill-down.

The query is in `sql/compare.sql` and is meant to be read. `compare.py --print-sql` shows the query generated for
a config.

Where it runs:
- **Sharded tables** (`cluster` set in the config): `cluster(..., view(...))` sends the query to one replica of
  each shard, and it runs against that shard's local tables. This needs old and new to place a key on the
  same shard. `summary.py` checks this: a key that is `only_old` on one shard and `only_new` on another is
  reported as moved, not lost.
- **Fully replicated tables** (`cluster=None`): the query runs on the broker the connection lands on.

Duplicates are fine. Rows of a key that ReplacingMergeTree has not merged yet only raise the row counts. Identical
duplicates are not a difference.

### Categories

| category | meaning |
|---|---|
| `equal` | identical |
| `float_noise` | floats differ by at most 1e-9 relative; everything else is identical |
| `old_multi_new_matches` | old has several versions of the key, and one of them equals new |
| `old_multi` / `new_multi` | that side has several different versions of the key |
| `value_diff` | one version on each side, and a non-float value differs |
| `float_diff` | one version on each side, and a float differs by more than 1e-9 relative |
| `only_old` / `only_new` | the key exists on one side only |

The keys are compared exactly as configured. A key column that the pipeline computes, such as `nonce` in stacks,
is part of the test: a renumbering shows up as `only_old` + `only_new` pairs.

## Prerequisites

`clickhouse-client` on the PATH. Connection: `TCMP_HOST` (default `clickhouse.production.san`), `TCMP_PORT`
(`30900`), `TCMP_USER` (`readonly`). The `readonly` user is enough.

## Usage

Run from this directory. `<config>` is a key of `CONFIGS` in `configs.py`.

1. Add a config to `configs.py`: the local tables, cluster, keys, values, start and cutoff.
2. `python3 compare.py <config> 2024-01`: try one heavy month first and time it.
3. `nohup python3 compare.py <config> > compare.log 2>&1 &`: run all months, 2 in parallel.
4. `python3 summary.py <config>`: totals, failing days, samples and the verdict.
5. `python3 rows.py <config> <day> <hash> ...`: the actual rows behind sample hashes.

Results are cached per month under `$TCMP_CACHE/<config>/` (default `~/.cache/table-cmp`), so a run resumes where it
stopped. A change to the config or the SQL templates makes cached months stale, and they are re-run. After the
table data changes (a backfill progressed, merges), use `--force`.

Type check: `mypy --strict *.py`.
