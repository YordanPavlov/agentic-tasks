# Old-vs-new table comparison

Compares two versions of a ClickHouse table key by key, over the full history, reading each granule about once.

## Method

The key space is split into **windows**: ranges of a prefix of the sorting key, of about 20M rows per shard
(both sides together; `TCMP_WINDOW_ROWS`), which takes about 7 GiB per query. For each window, one
query:
1. reduces every row of both tables to a key hash, a hash of the values and the float values,
2. groups by the key hash, so the old and new versions of a key land in one row,
3. gives each key a category,
4. counts keys per day and category, keeping up to 10,000 key hashes per category for drill-down.

The query is in `sql/compare.sql` and is meant to be read. Its header lists every `$placeholder`: tables,
window, tolerance, column lists and filters, all filled in from the config. `compare.py <config> <N>
--print-sql` prints the filled-in query of window N, ready to paste into `clickhouse-client`.

### Windows

A single query over a large month can exceed the server's memory (the GROUP BY holds every key), so the work is
cut by key, not by time:
- **Bound columns:** the longest prefix shared by both tables' sorting keys, of configured key columns with the same
  type on both sides, e.g. `dt, assetRefId, …` for XRP balances and `contractAddress, address, sign` for stacks.
  Every row of a key then falls in one window, so the results of the windows simply add up.
- **Bounds** (`sql/bounds.sql`, via `sql/index.sql`): `mergeTreeIndex()` lists the first key of every granule
  (8192 rows) of both tables in [start, cutoff), on every shard. Ranked by key, they give row-count quantiles of the
  key, however skewed it is. Only the indexes are read; this takes seconds.
- **The filter** is written as nested column comparisons (`a > x OR (a = x AND b >= y)`), which the primary index
  uses. It does not use a tuple comparison `(a, b) >= (x, y)`. Strings are written as `unhex('…')`.
- The bounds are computed once per config and cached. Stale bounds after merges or new data are still correct, the
  windows are just less even, so `--force` keeps them. Delete `bounds.json` to recompute them.
- The windows ignore months: for balances (`dt` leads) a window is a time range; for stacks, a range of contracts
  and addresses across all months.

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
| `old_multi` / `new_multi` | after `FINAL`, that side still has several different rows for the key: the configured key is coarser than the table's sorting key |
| `value_diff` | one row on each side, and a non-float value differs |
| `float_diff` | one row on each side, and a float differs by more than 1e-9 relative |
| `only_old` / `only_new` | the key exists on one side only |

The keys are compared exactly as configured. A key column that the pipeline computes, such as `nonce` in stacks,
is part of the test: a renumbering shows up as `only_old` + `only_new` pairs.

## Prerequisites

`clickhouse-client` on the PATH. Connection: `TCMP_HOST` (default `clickhouse.production.san`), `TCMP_PORT`
(`30900`), `TCMP_USER` (`readonly`). The `readonly` user is enough.

## Usage

Run from this directory. `<config>` is a key of `CONFIGS` in `configs.py`.

1. Add a config to `configs.py`: the local tables, cluster, keys, values, start and cutoff.
2. `python3 compare.py <config> 0`: computes the bounds, prints the number of windows and runs window 0. The
   windows are about equal, so its time times the number of windows, halved for 2 in parallel, estimates the run.
3. `nohup python3 compare.py <config> > compare.log 2>&1 &`: run all windows, 2 in parallel.
4. `python3 summary.py <config>`: totals, failing days, samples and the verdict.
5. `python3 rows.py <config> <day> <hash> ...`: the actual rows behind sample hashes (scans the month of day).

The bounds and the results per window are cached under `$TCMP_CACHE/<config>/` (default `~/.cache/table-cmp`), so a
run resumes where it stopped. A change to the config, the SQL templates or `TCMP_WINDOW_ROWS` makes the bounds
stale: they are recomputed and every window is re-run. After the table data changes (a backfill progressed,
merges), use `--force`.

Type check: `mypy --strict *.py`.
