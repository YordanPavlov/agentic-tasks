# Comparing an old and a new version of a table (runbook)

**Started:** 2026-09-30
**Status:** Single-pass, per-month comparison (`scripts/`), in place since 2026-10-02. Validated on XRP balances
and stacks months; no full run with the new scripts yet (see "Runs").
**Origin:** [`2026-09-rerun-comparison-framework`](../2026-09-rerun-comparison-framework/rerun-comparison-framework.md)
has the development history and the first XRP balances results.

## When to use

A pipeline was re-run and wrote a new version of a table (transfers, balances, stacks, or metrics in
`*_experimental` tables). Before the new version replaces the old one, we need to know:
- what differs, and where (days, keys)
- why, as far as the data shows it
- a verdict, written up as an executive summary (template at the end)

Ground rules:
- **Judge by the data, not by the code change.** The tool works for any table, given its keys and values.
- **The full dataset, not samples.** Aim for ~1 h per comparison, 2 h at most.
- **Prod is read-only.** At most **2 long-running queries** at a time; small probes alongside are fine.
- **Leave the live tail out.** Set the cutoff a few days back: recent data is still being written.
- **Floats:** 1e-9 relative tolerance. Everything else must match exactly.
- **Keys are compared exactly.** That includes key columns the pipeline computes, such as `nonce` in stacks.

## How it works

`compare.py` runs one query per month (the tables' partition), which reads each table once:
1. Every row of both tables becomes a key hash, a hash of the values and the float values.
2. Grouping by the key hash puts the old and new versions of each key into one row.
3. Each key gets a category (below).
4. Keys are counted per day and category. Up to 10,000 key hashes per category are kept for drill-down.

The query is in `scripts/sql/compare.sql` and is meant to be read. Its header lists every `$placeholder`: tables,
window, tolerance, column lists and filters, all filled in from the config. `compare.py <config> <month>
--print-sql` prints the filled-in query, ready to paste into `clickhouse-client`.

**Where the query runs:**
- **Sharded tables:** the query goes to one replica of every shard, via `cluster(..., view(...))`. Each shard
  compares its own local tables, and nothing crosses the network except the result.
  - This relies on old and new putting a key on the same shard. That held on every month checked so far.
  - `summary.py` checks it: a key that is `only_old` on one shard and `only_new` on another is reported as moved,
    not lost.
- **Fully replicated tables** (`cluster=None`): the query runs on the broker the connection lands on.

**Duplicates are harmless.** ReplacingMergeTree rows of the same key that have not merged yet only raise the row
counts. A month that was inserted twice shows up as duplicate rows, not as a difference.

### Categories

| category | meaning | usually |
|---|---|---|
| `equal` | identical | OK |
| `float_noise` | floats differ by at most 1e-9 relative, everything else identical | OK |
| `old_multi_new_matches` | old has several versions of the key, one equals new | old's unmerged duplicates; benign |
| `old_multi` | old has several versions, none equals new | look at the rows |
| `new_multi` | new has several versions of the key | new-side problem, or the backfill is still merging |
| `value_diff` | one version on each side, a non-float value differs | look at the rows |
| `float_diff` | one version on each side, a float differs by more than 1e-9 | look at the rows |
| `only_old` | key only in old | lost in new, a renumbered key, or a key change (below) |
| `only_new` | key only in new | added in new (e.g. recovered data), a renumbered key, or a key change |

`only_old` and `only_new` in equal numbers on the same days usually means the same rows with a different key
value, e.g. a changed `nonce`. Use `rows.py` to confirm, then judge whether the change is acceptable. The test
itself does not accept it.

## Scripts (`scripts/`)

Plain Python 3.11 plus `clickhouse-client`. The code is typed (`mypy --strict *.py`). Run everything from `scripts/`.

| script | what | cost |
|---|---|---|
| `configs.py` | one `Config` per comparison | |
| `compare.py <config> [YYYY-MM ...] [--parallel N] [--force] [--print-sql]` | compares month by month, 2 months in parallel by default | ~3M rows/s (both sides together) |
| `summary.py <config> [--days N]` | totals, failing days, moved keys, sample hashes, verdict | local |
| `rows.py <config> <day> <hash> ...` | the actual rows of both tables for sample hashes, with the host each row is on | scans the month of `<day>` |

**Connection:** `TCMP_HOST` (default `clickhouse.production.san`), `TCMP_PORT` (`30900`), `TCMP_USER`
(`readonly`). The `readonly` user is enough.

**Cache:** results are cached per month under `$TCMP_CACHE/<config>/YYYY-MM.json` (default `~/.cache/table-cmp`).
- An interrupted run resumes where it stopped. A failed month stays uncached and is retried on the next run.
- A change to the config or to the SQL templates marks cached months as stale, and they are re-run.
- After the data itself changed (a backfill progressed, merges ran), re-run with `--force`.

### Config

```python
'xrp_balances': Config(
    old='default.xrp_balances_shard_v8', new='default.xrp_balances_shard_v10', cluster='default_cluster',
    dt='dt',
    keys=('dt', 'assetRefId', 'address', 'blockNumber', 'transactionIndex'),
    values=('balance', 'oldDt', 'oldBlockNumber', 'oldBalance', 'currency', 'issuer', 'issuerCurrency',
            'addressType', 'transactionHash'),
    start='2013-01-01', cutoff='2026-09-26'),
```

- `old`, `new`: the **local** tables, not the Distributed ones. The Distributed table's engine names them:
  `SELECT engine_full FROM system.tables WHERE database='default' AND name='xrp_balances'`.
- `cluster`: the cluster named in that same engine definition. `None` for a table that every broker holds in full.
- `keys`: the columns that identify a row, normally the ReplacingMergeTree sorting key.
- `values`: every other column that matters. Leave out bookkeeping columns that always differ (`computed_at`,
  insert time).
- `start`, `cutoff`: `'YYYY-MM-DD'`. The cutoff is exclusive.
- `where`, `where_old`, `where_new`: optional filters, e.g. to limit old to the metrics the experimental table
  holds.

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
3. **Time one heavy month:** `python3 compare.py <config> 2024-01`. Then estimate the full run from the number of
   months.
   - If the estimate is over budget, ask whether a longer run (overnight, resumable) is acceptable.
   - Otherwise run the most recent 2–3 years plus one heavy month per earlier year, and report the rest as not
     checked.
4. **Run everything** in the background: `nohup python3 compare.py <config> > compare.log 2>&1 &`. It prints one line
   per month.
5. **`python3 summary.py <config>`.** For every category that is not OK, pick a sample hash and run `rows.py` on it.
   Find the cause from the rows.
6. **Optionally, run table invariants** (below) on both sides.
7. **Write the executive summary**, and add the run to "Runs" below.

### Key changes

When the two tables have different sorting keys, each side's ReplacingMergeTree has deduplicated by its own key.
Rows that are distinct under one key can have been merged away under the other.
- Choose the key to compare by explicitly. For XRP stacks, old uses `(…, dt, nonce)` and new `(…, blockNumber,
  nonce)`. `dt` follows from `blockNumber`, so the config uses new's key and compares `dt` as a value.
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

- Observed: stacks 2024-01, 750M rows on both sides together, took 250 s (~3M rows/s) on 2026-10-02 at midday.
  Small months take 1–2 s.
- Cluster load can change the speed by an order of magnitude. Check the load with
  `SELECT count(), countIf(elapsed > 60) FROM system.processes` (that sees one broker only), and prefer evenings
  and weekends.
- With `--parallel 2`, a full run uses both long-query slots, so nothing heavy can run beside it.

## Gotchas

**ClickHouse:**
- The shard hosts (`clickhouse-0..2`) cannot be reached from outside the cluster; only the load balancer can.
  Every connection lands on a random broker. `cluster(..., view(...))` is how to reach every shard's local tables.
  The `readonly` user has the `REMOTE` grant this needs.
- The `readonly` user has `readonly=2`: it can change settings and create temporary tables.
- The server runs the old query analyzer (`enable_analyzer=0`).
- In the generated SQL, the `cmp_` prefixes keep aliases from shadowing real table columns. With the old analyzer, a
  shadowed name would change the expressions.
- Hash functions return NULL if any argument is NULL. Nullable columns are hashed as `isNull(x), ifNull(x, …)`.
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
| category | keys | where | cause |    (one row per category that is not OK, plus duplicates per side)
Discrepancies: per class: size, where (dates), cause if found (with a sample row from rows.py), benign or not.
Not checked: months skipped, invariants not run, open questions (e.g. backfill end date).
```

## Runs

| date | config | scripts | result |
|---|---|---|---|
| 2026-09-30 | `xrp_balances` | tier scripts | new ⊇ old, equal within 1e-9 (Tier 1 97 min, Tier 2 28 min); details in the origin doc |
| 2026-09-30 | `xrp_stacks`, probe | tier scripts | no keys lost; `nonce` renumbered; holdings and ages equal |
| 2026-10-02 | `xrp_balances` 2013-01..02, `xrp_stacks` 2013-02..03 and 2024-01 | current | validation of the new scripts, below |

**Validation, 2026-10-02:**
- Key and row counts match `uniqExact` / `count()` on the Distributed tables exactly (stacks 2013-03, balances
  2013-02).
- Stacks 2024-01-03 categories match the earlier Tier 2 run: 61,369 `only_old`/`only_new` pairs; 3,879
  `value_diff` + 112 `old_multi` + 1 `float_diff` = Tier 2's 3,992 `value_diff`.
- **Stacks 2024-01, under the strict rule: DIFFERS.**
  - old: 498M rows, i.e. the whole month twice; new: 249M rows. Both sides have 249,166,962 keys.
  - 1.9M `only_old`/`only_new` pairs: the `nonce` renumbering.
  - 96k `value_diff`, 1,660 `old_multi`, 8 `float_diff`.
  - 1.7M `old_multi_new_matches`.
  - No key moved between shards.
- Balances 2013-02: 7 `old_multi_new_matches`. Old holds the same row twice with balances that differ in the last
  ULP (8.114739e-9 vs 8.114739000000001e-9).
- Side finding (2026-09-30): 2013 stack rows with `sign = 1` have `odt = 1970-01-01` on both sides.
- Cache of these runs: `~/.cache/table-cmp-v2/`.

## Open

- Full runs of `xrp_balances` and `xrp_stacks` with the new scripts.
- Stacks: explain the `nonce` renumbering, the 96k `value_diff` and the 1,660 `old_multi` with `rows.py`.
- A drill-down aid that marks `only_old`/`only_new` pairs differing only in `nonce`.
- Script the invariants, starting with the balance chain.
