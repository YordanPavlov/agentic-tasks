# Old-vs-new table comparison

Compares two versions of a ClickHouse table key by key, over the full history, reading each granule about once,
and writes the differences per month, ordered by date.

## Method

A table is described by its **keys** (the columns that identify a row) and one **value** (the column that is
compared, as Float64 with a relative tolerance of 1e-9). Every key of both tables gets a category:

| category | meaning |
|---|---|
| `equal` | the values are equal within the tolerance |
| `differs` | one row on each side, and the values differ |
| `missing_in_new` / `missing_in_old` | the key exists on one side only |
| `old_multi` / `new_multi` | after `FINAL`, that side has several rows for the key: the configured key is coarser than the table's sorting key |

A key column that differs makes a key `missing_in_*` on both sides; a key that does not match and a value that
differs need the same investigation. A source can derive a key column that leaves out a known benign difference,
e.g. `nonce_rank` for stacks (below).

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
1. reads the rows of both sides,
2. groups them by the key columns, so the old and new rows of a key land in one row, and gives the key a category
   and the relative difference |new − old| / max(|old|, |new|),
3. counts the keys per category and **cell**: the `group_by` columns and the day. Equal keys are counted per
   month only. Per cell it also keeps the largest difference and up to 3 example keys.

`compare.py <config> --print-sql <period> <window>` prints the filled-in query. To list failing keys, replace its
outer `SELECT … GROUP BY category, cell` with `SELECT * … WHERE category = 'differs' LIMIT 10`.

### Sources

Each side is read through a **source** (`sql/sources/`, `source` in the config), the innermost query of
`compare.sql`. The default, `plain.sql`, reads the table with `FINAL`, i.e. as consumers see it: for each sorting
key, the latest inserted row (or the highest version). Unmerged copies are not differences.

A table-specific source may derive key columns, e.g. `xrp_stacks.sql` ranks `nonce` within its block, so that a
renumbering is not a difference. A source must apply `$key_range` to the bucket columns as they are in the table,
and return exactly the rows of the table with `FINAL`: the check counts the table.

### Where it runs

- **Sharded tables** (`cluster` set): `cluster(..., view(...))` sends each query to one replica of each shard,
  and it runs against that shard's local tables. This assumes old and new place a key on the same shard; a key
  placed differently shows up as `missing_in_new` on one shard and `missing_in_old` on another.
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
  - `config`: what was compared (tables, keys, value, `group_by`, tolerance, filters), and a `fingerprint`
  - `period`: the period the month was compared in, its number of windows and query time
  - `rows`: rows per side as `FINAL` shows them; `keys`: keys per category
  - `deviations`: the failing cells only, nested by the `group_by` values, then the day; one line per cell. A cell
    holds the keys per failing category, `max_diff_pct` for `differs`, and `examples` per category: the key
    columns not in `group_by`, with the old and new value; for `differs` the largest difference comes first.
- `coverage.json`: the groups present on one side only (configs with `group_by`).

A change to the config, the SQL templates or `TCMP_WINDOW_ROWS` changes the fingerprint: files with another
fingerprint are stale and their periods re-run. After the table data changes, use `--force`.

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
