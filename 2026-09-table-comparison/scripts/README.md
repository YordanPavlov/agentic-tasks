# Old-vs-new table comparison

Compares two versions of a ClickHouse table key by key, over the full history, reading each granule about once,
and writes the differences per month, ordered by date.

## Method

A table is described by its **keys** and one **value**. A key's value is the sum over its rows (after `FINAL`),
compared as Float64 with a relative tolerance of 1e-9. With the sorting key as the key, that is one row per key;
a coarser key compares the table the way its consumers aggregate it, e.g. stacks by `(address, sign, assetRefId,
dt, odt)`, so how an amount is split into rows is not a difference. Every key of both tables gets a category:

| category | meaning |
|---|---|
| `equal` | the values are equal within the tolerance |
| `differs` | the key exists on both sides, and the sums differ |
| `missing_in_new` / `missing_in_old` | the key exists on one side only |

A key column that differs makes a key `missing_in_*` on both sides; a key that does not match and a value that
differs need the same investigation.

### Periods and windows

The work is cut by time, then by key:
- **Period:** a month for tables partitioned by month (`monthly=True`), the whole range otherwise. A month filter
  on a table that isn't partitioned by month would read the same granules once per month.
- **Window:** a range of the **bucket columns** (`buckets`, a prefix of both sorting keys and part of the key)
  of about `TCMP_WINDOW_ROWS` rows per shard, both sides together (default 20M). The query of a window holds all
  its keys in memory: ~10.5 GiB per shard at 20M rows of stacks. All rows of a key fall into one window, so the results of the windows add up.
- **Cutting** (`sql/counts.sql`, `sql/cuts.sql`): per period, `count()` with `FINAL` per bucket and side, then
  a running sum cuts the buckets into windows. A bucket is never split. The window filter is written as nested
  column comparisons (`a > x OR (a = x AND b >= y)`), which the primary index uses; strings as `unhex('…')`.
- **Check:** the rows of a period's windows must add up to these counts, or the period fails.

Periods run in date order, `--parallel` window queries at a time (default 2). A month's file is written once its
period is done, so an interrupted run resumes with the first unfinished period.

**Early stop:** once the failing keys of all months pass `--max-failing-keys` (default 100,000), no further months
are written or started: with that many differences, the new table needs a closer look before the rest is worth
comparing. A period that covers the whole range is still computed in full; only its later months are not written.

### The comparison query

`sql/compare.sql` is meant to be read; its header lists every `$placeholder`. For one window it:
1. reads the rows of both sides with `FINAL`, i.e. as consumers see them: for each sorting key, the latest
   inserted row (or the highest version). Unmerged copies are not differences.
2. groups them by the key columns, so the old and new rows of a key land in one row, sums each side's values, and
   gives the key a category and the relative difference |new − old| / max(|old|, |new|),
3. counts the keys per category and **cell**: the `group_by` columns and the day. Equal keys are counted per
   month only. Per cell it also keeps the largest difference.

`compare.py <config> --print-sql <period> <window>` prints the filled-in query of one window (it cuts the period
first, which takes seconds). To list the failing keys of a cell, replace its outer `SELECT … GROUP BY category,
cell` with `SELECT * … WHERE category = 'differs' AND cell = [...] LIMIT 10`.

### Where it runs

- **Sharded tables** (`cluster` set): `cluster(..., view(...))` sends each query to one replica of each shard,
  and it runs against that shard's local tables. This assumes old and new place a key on the same shard; a key
  placed differently shows up as `missing_in_new` on one shard and `missing_in_old` on another. A key whose rows
  are spread over shards is compared once per shard, so a coarser key needs all its rows on one shard.
- **Fully replicated tables** (`cluster=None`, e.g. the metrics tables): the query runs on the broker the
  connection lands on.

### Groups and coverage

`group_by` breaks the output down by key columns before the day, e.g. `asset_id, metric_id` for metrics. With
`common_groups_only`, only the groups present on both sides are compared; `coverage.json` lists the others (e.g.
metrics the experimental run did not compute), with their rows. A group present on both sides is expected to be
complete: days missing on one side are failures.

## Output

Under `results/<config>/`, pretty-printed JSON:
- `YYYY/YYYY-MM.json`, one per month:
  - `config`: what was compared (tables, keys, value, `group_by`, tolerance, filters)
  - `period`: the period the month was compared in, its number of `windows`, and when it `started` and
    `finished` (UTC wall clock; periods overlap with `--parallel`)
  - `rows`: rows per side as `FINAL` shows them; `keys`: keys per category
  - `deviations`: the failing cells only, nested by the `group_by` values, then the day; one line per cell. A cell
    holds the keys per failing category, and `max_diff_pct` for `differs`.
- `coverage.json`: the groups present on one side only (configs with `group_by`).

A month whose file exists is done, and a restart skips it. After a change to the data, the config or the SQL,
use `--force` (or delete `results/<config>/`): the files don't record which version of the SQL made them. A run
reads the SQL templates once, at start.

## Prerequisites

`clickhouse-client` on the PATH. Connection: `TCMP_HOST` (default `clickhouse.production.san`), `TCMP_PORT`
(`30900`), `TCMP_USER` (`readonly`). The `readonly` user is enough. `TCMP_RESULTS` moves the output.

## Usage

Run from this directory. `<config>` is a key of `CONFIGS` in `configs.py`.

1. Add a config to `configs.py`: the local tables, cluster, keys, value, buckets, start and cutoff.
2. `python3 compare.py <config> <YYYY-MM>`: one heavy month. Its time, times the number of comparable months,
   halved for 2 in parallel, estimates the run.
3. `nohup python3 compare.py <config> > compare.log 2>&1 &`: all periods, one line per month.
4. `python3 summary.py <config>`: coverage, failing months, the failing groups (or days) of the first failing
   month, keys per category and the verdict.

Type check: `mypy --strict *.py`.
