# Old-vs-new table comparison

Compares two versions of a ClickHouse table key by key, over the full history, reading each partition once.

## Method

For each month (the tables' partition), one query:
1. reduces every row of both tables to a key hash, a hash of the values and the float values,
2. groups by the key hash, so the old and new row of a key land side by side,
3. gives each key a category,
4. counts keys per day and category, keeping up to 10,000 key hashes per category for drill-down.

The query is in `sql/compare.sql` and is meant to be read. Its header lists every `$placeholder`: tables,
window, tolerance, column lists and filters, all filled in from the config. `compare.py <config> <month>
--print-sql` prints the filled-in query, ready to paste into `clickhouse-client`.

Where it runs:
- **Sharded tables** (`cluster` set in the config): `cluster(..., view(...))` sends the query to one replica of
  each shard, and it runs against that shard's local tables. This needs old and new to place a key on the
  same shard. `summary.py` checks this: a key that is `only_old` on one shard and `only_new` on another is
  reported as moved, not lost.
- **Fully replicated tables** (`cluster=None`): the query runs on the broker the connection lands on.

Both sides are read with `FINAL`, i.e. as consumers see them: for each sorting key, the latest inserted row (or the
highest version). Unmerged copies are not differences.

### Categories

| category | meaning |
|---|---|
| `equal` | identical |
| `float_noise` | floats differ by at most 1e-9 relative; everything else is identical |
| `value_diff` | one row on each side, and a non-float value differs |
| `float_diff` | one row on each side, and a float differs by more than 1e-9 relative |
| `only_old` / `only_new` | the key exists on one side only |

The configured key must identify exactly one row per table under `FINAL`: the sorting key, or an equivalent.
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
