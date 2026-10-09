# Comparing an old and a new version of a table (runbook)

**Started:** 2026-09-30
**Status:** Per-month output (`scripts/`, described in `scripts/README.md`), implemented 2026-10-09; trial on
stacks 2024-01 and a first `daily_metrics` run done (see "Runs"). Next: a full `xrp_stacks` run.
**Origin:** [`2026-09-rerun-comparison-framework`](../2026-09-rerun-comparison-framework/rerun-comparison-framework.md)
has the development history and the first XRP balances results.

## When to use

A pipeline was re-run and wrote a new version of a table (transfers, balances, stacks, or metrics in
`*_experimental` tables). Before the new version replaces the old one, we need to know:
- what differs, and where (days, keys)
- why, as far as the data shows it
- a verdict, written up as an executive summary (template at the end)

Ground rules:
- **Judge by the data, not by the code change.** The tool works for any table, given its keys and value.
- **The full dataset, not samples.** Aim for ~1 h per comparison, 2 h at most.
- **Prod is read-only.** At most **2 long-running queries** at a time; small probes alongside are fine.
- **Leave the live tail out.** Set the cutoff a few days back: recent data is still being written.
- **One value per table,** compared with a 1e-9 relative tolerance. Every other column that matters is key and
  must match exactly, including columns the pipeline computes (stacks compares `nonce_rank`, see the README).
- **Stop early.** Many differences in the early years mean the new run needs a look before the rest is compared.

## The tool

`scripts/README.md` describes the method, the output and the usage; `scripts/configs.py` documents the config
fields. In short: `compare.py` compares month by month in date order and writes `scripts/results/<config>/`;
`summary.py` reads it.

Reading the categories:

| category | usually |
|---|---|
| `equal` | OK |
| `differs` | look at the examples in the month file |
| `missing_in_new` / `missing_in_old` | lost in new / added in new (e.g. recovered data), or a key column that differs: then they come in equal numbers on the same days |
| `old_multi` / `new_multi` | the configured key is coarser than that table's sorting key (see "Key changes") |

### Design decisions (2026-10-09)

The previous version kept up to 10,000 key hashes per category for drill-down; the full stacks run left a 36 GB
cache that `summary.py` could not load, and its results were spread over key ranges, not dates.
- **One value column.** A key that doesn't match and a value that differs need the same investigation, so every
  other column is key. Balances `old*` columns are checked as invariants instead (below).
- **Actual keys, no hashes.** Examples in the output can be queried directly. Costs ~50% more memory per window.
- **Output per month, failing cells only**, self-describing, as input for a follow-up analysis tool (metric names,
  cross-metric dependencies).
- **Metrics:** `daily_metrics_v2_experimental` holds a few assets (Optimism ERC-20 tokens) and lacks the metrics
  that were not part of the run. `where` limits old to its assets; `common_groups_only` leaves the missing
  metrics out of the comparison and lists them in `coverage.json`. A metric that is present must be complete.

## Procedure

1. **Check that the new table is completely written.** Per side, look at `max(dt)`, the unmerged parts and the
   last write:
   ```sql
   SELECT hostName(), max(max_time), countIf(level = 0), max(modification_time)
   FROM cluster('default_cluster', system.parts)
   WHERE database = 'default' AND table = '<local table>' AND active GROUP BY 1
   ```
   - A side that is still being written has level-0 parts and a recent last write. The live old table always does.
   - Ask the owner of the re-run whether the backfill has finished and whether its end date is intended. If you
     can't ask, put the question in the summary and go on assuming it has finished.
2. **Write the config.**
   - Set the cutoff a few days before the earlier of the two `max(dt)`.
   - Compare the sorting keys of both local tables (`system.tables.sorting_key`). If they differ, see "Key changes".
3. **Time one heavy month:** `python3 compare.py <config> 2024-01`. Then estimate the full run from the rows per
   month (see "Cost").
   - If the estimate is over budget, ask whether a longer run (overnight, resumable) is acceptable.
   - Otherwise run the most recent 2–3 years plus one heavy month per earlier year, and report the rest as not
     checked.
4. **Run everything** in the background: `nohup python3 compare.py <config> > compare.log 2>&1 &`. It prints one line
   per month, and stops after the month in which the failing keys pass `--max-failing-keys` (default 100k).
5. **`python3 summary.py <config>`.** For every failing category, take the examples from the month file and query
   both tables for them. Find the cause from the rows.
6. **Optionally, run table invariants** (below) on both sides.
7. **Write the executive summary**, and add the run to "Runs" below.

### Key changes

When the two tables have different sorting keys, each side's ReplacingMergeTree has deduplicated by its own key.
Rows that are distinct under one key can have been merged away under the other.
- Choose the key to compare by explicitly. For XRP stacks, old uses `(…, dt, nonce)` and new `(…, blockNumber,
  nonce)`. The config's key holds both `blockNumber` and `dt` (with `nonce_rank` in place of `nonce`).
- To size the effect, run a second config with the other key and compare the summaries. A difference that
  disappears under the other key comes from the key change, not the data. Which key is right is a design question
  for the table's owner.

### Invariants (table-specific, not scripted)

They catch errors that old and new share, which a comparison cannot see:
- **balances:** chain continuity. Every (address, asset, `oldBlockNumber`, `oldBalance`) must point to an
  existing (address, asset, `blockNumber`, `balance`). XRP balances old has ~2,862 dangling pointers from 12 lost
  ledgers; new should have none.
- **stacks:** Σ `sign * amount` per block ≈ 0, apart from issuance (`table_qa/*_stacks.py`,
  `get_stacks_with_huge_delta`).
- **transfers:** tx count and block coverage against the chain.
- The yearly `table_qa/test/` checks in `clickhouse-tables` (see the `santiment-add-table-qa-tests` skill).

### Cost

- Stacks 2024-01 (2026-10-09): 498M rows with `FINAL`, both sides together; 9 windows, 264 s query time, 2:44
  wall at `--parallel 2`. A window query takes 22–57 s per shard and ~10.5 GiB; the counting query 31 s, 1.2 GiB.
  `TCMP_WINDOW_ROWS=14000000` brings a window back to ~7 GiB. Small months take 1–2 s.
- `daily_metrics` (one period, 2 windows): ~180–240 s of query time, plus up to 4 min of Python parsing the
  rows of every failing point.
- Cluster load can change the speed by an order of magnitude. Check the load with
  `SELECT count(), countIf(elapsed > 60) FROM system.processes` (that sees one broker only), and prefer evenings
  and weekends.
- With `--parallel 2`, a full run uses both long-query slots, so nothing heavy can run beside it.

## Misc

**ClickHouse:**
- Port 30900 lands on the load balancer (a random broker); 30941, 30942 and 30943 land on the shard hosts
  `clickhouse-0`, `-1` and `-2`. `cluster(...)` reaches every shard from any of them; the `readonly` user has the
  `REMOTE` grant this needs.
- The `readonly` user has `readonly=2`: it can change settings and create temporary tables.
- The server runs the old query analyzer (`enable_analyzer=0`).
- In the generated SQL, the `cmp_` prefixes keep aliases from shadowing real table columns. With the old analyzer, a
  shadowed name would change the expressions, and `sum(x) AS x` fails with `ILLEGAL_AGGREGATION`.
- `SELECT *` skips MATERIALIZED columns (e.g. `assetRefId` in balances). Always list columns.
- `system.parts`, `system.processes` and `system.query_log` cover one broker. Use `cluster(...)` for the whole
  cluster, and measure time on the client.
- `clickhouse-client --format` overrides a `FORMAT` clause in the query.
- The server quotes 64-bit integers in JSON. `common.run_query` turns that off.

**Environment:**
- `pkill -f <pattern>` can match its own shell; kill by PID.
- Keep outputs local. Don't publish artifacts or docs unless asked.

## Executive summary template

```
# <old> vs <new>: <PASS | PASS with caveats | DIFFERS>
Scope: <start> .. <cutoff>, months run / skipped; rows and keys per side; run date; wall time.
Verdict: one or two sentences (e.g. "new ⊇ old, equal within 1e-9; new recovers N keys on <days>").
| category | keys | where | cause |    (one row per category that is not OK)
Discrepancies: per class: size, where (dates), cause if found (with an example row), benign or not.
Not checked: months skipped, invariants not run, open questions (e.g. backfill end date).
```

## Runs

| date | config | scripts | result |
|---|---|---|---|
| 2026-09-30 | `xrp_balances` | tier scripts | new ⊇ old, equal within 1e-9 (Tier 1 97 min, Tier 2 28 min); details in the origin doc |
| 2026-09-30 | `xrp_stacks`, probe | tier scripts | no keys lost; `nonce` renumbered; holdings and ages equal |
| 2026-10-02 | `xrp_balances` 2013-01..02, `xrp_stacks` 2013-02..03 and 2024-01 | current | validation of the new scripts, below |
| 2026-10-07 | `xrp_stacks` 2013-01 .. 2026-09-03, full | key-range windows (314) | DIFFERS, below |
| 2026-10-09 | `xrp_stacks` 2013-02..06, 2024-01 | per-month output | equal; trial, below |
| 2026-10-09 | `daily_metrics` | per-month output | DIFFERS, stopped at 2017-07, below |

**Validation, 2026-10-02:**
- Key and row counts match `uniqExact` / `count()` on the Distributed tables exactly (stacks 2013-03, balances
  2013-02).
- Stacks 2024-01-03 categories match the earlier Tier 2 run: 61,369 `only_old`/`only_new` pairs; 3,879
  `value_diff` + 112 `old_multi` + 1 `float_diff` = Tier 2's 3,992 `value_diff`.
- **Stacks 2024-01, under the strict rule: DIFFERS.**
  - old: 498M rows on disk, i.e. the whole month twice. With `FINAL`, both sides have 249,030,813 rows.
  - 1.9M `only_old`/`only_new` pairs: the `nonce` renumbering.
  - 97,924 `value_diff`, 8 `float_diff`. No key moved between shards.
  - `FINAL` vs a plain read of the same month: 284 s vs 203 s, under heavier load (17 queries over 60 s).
    - The plain read's 1.7M `old_multi_new_matches` became `equal`: the latest version is the one that matches.
    - Its 1,660 `old_multi` became `value_diff`: the version consumers see differs from new.
- Balances 2013-02: equal under `FINAL`. Old holds one row twice, with balances that differ in the last ULP
  (8.114739e-9 vs 8.114739000000001e-9). `FINAL` returns the newer copy (part `201302_4_4_0`), and it equals new.
- Side finding (2026-09-30): 2013 stack rows with `sign = 1` have `odt = 1970-01-01` on both sides.
- Cache of these runs: `~/.cache/table-cmp-v2/`.

**`xrp_stacks` full run, 2026-10-07:** 274 min query time. 8.41B keys per side (new +143k).
- `only_old` 10,200, `only_new` 153,310 (none moved between shards), `value_diff` 4.0M, `float_diff` 9.3M,
  `soft_diff` 1.13B (`nonce` offset only), `equal` 7.26B.
- 443 failing days. 2026-05-31 alone has 2.43M `value_diff`; mid-June to mid-August 2025 is mostly
  `float_diff`, with `only_new` spikes on 2025-06-17 and 2025-06-26.
- The cache was deleted on 2026-10-08; the redesign makes this run obsolete.

**Trial of the per-month output, 2026-10-09:**
- Stacks 2013-03..06 cut into 1 window and into up to 30 windows give the same rows and keys per category.
- Stacks 2024-01: equal (the 97,924 `value_diff` of 2026-10-02 came from keying on raw `nonce`). Checked
  independently: a checksum of the key columns is identical on both sides; on a 1/50 sample, 25 of 5.3M `amount`s
  differ, all by 1 ULP (2.2e-16 relative).

**`daily_metrics_v2` vs `_experimental`, 2026-10-09:** 18 assets; 1,666 (asset, metric) groups only in old (not
part of the run), none only in new. Equal 2009-01 .. 2017-06. Stopped at 2017-07: 108,760 `missing_in_old`, i.e.
experimental has 2017-07-12..31 for 5,438 groups that old lacks, often with value 0.

## Open

- Full `xrp_stacks` run with the per-month output.
- `daily_metrics`: why experimental starts 5,438 metrics at 2017-07-12 where old has nothing.
- An HTML page for the results: daily stacked bars per category, a year → month → day table.
- Full `xrp_balances` run.
- Stacks: explain the 2026-05-31 `value_diff`s and the 2025-06..08 `float_diff`s.
- Script the invariants, starting with the balance chain.
