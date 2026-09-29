# Re-run comparison framework (old vs new source tables / metrics)

**Started:** 2026-09-29
**Status:** paused after first iteration (XRP balances). Approach being redesigned for time budget.
**Repo target (later):** `clickhouse-tables` (`table_qa/`)

## Goal

We regularly re-run pipelines and produce new versions of source tables (transfers,
balances, stacks) and of daily/intraday metrics (which land in `*_experimental`
tables with the same structure). Build a **common, black-box framework** that compares
old against new:
- It finds the key points of discrepancy and, where possible, their cause.
- It generates an **executive summary document**.

Guidelines from the user:
- **Black box:** ignore what changed in the code (this round it was the ForSt backend
  switch for both XRP jobs). The checks must generalize across tables.
- **Full dataset, not samples**, but on a **time budget of about 1 h per comparison (2 h max)**.
  Reducing scope to basic guardrails is acceptable. Discrepancies usually show up where
  the `table_qa/test/` checks look: blocks, tx counts, unique balance changes,
  per-address chains, stacks sum-to-zero.
- **Prod is read-only**, with at most **2 long-running queries** at a time (small fast
  probes alongside are fine).
- **Historic cutoff** a few days back (live data adds little). Tolerance: **1e-9 relative**.
- XRP native first; all tokens if feasible.

## First iteration: XRP balances

`default.xrp_balances` (→ `xrp_balances_shard_v8`) vs `test.xrp_balances_test`
(→ `shard_v10`), prod, cutoff `dt < 2026-09-26`.
Stacks (`test.xrp_stacks_test` → `xrp_stacks_shard_v9`, key uses blockNumber instead of dt)
was only backfilled to 2021-11 on 2026-09-29. **Wait for the backfill to finish**:
unmerged parts would distort the comparison.

Prior art: `2026-09-xrp-balances-async-state/qa/qa-output-comparison.md` (2026-09-14,
earlier state of the same new table, 2013-01 → 2025-04, PASS, ~2.5 h). I found it only
after running this iteration. Its method (per-month dedup plus address buckets) and
gotchas (monster IOUs up to 1e95 weaken sum-based float checks) apply here.

### What was run

| layer | what | status |
|---|---|---|
| L1 | per (side, ~30M-row window) query: cross-shard dedup by GROUP BY key hash → **per-hour** key-set hash, exact content hash, dup/conflict counts, blocks, XRP delta | **done**, full history, ~3 h |
| L2 | differing hours only: in-DB FULL JOIN old/new per day → per-field diff counts with 1e-9 tolerance, max abs/rel diff, sample key hashes | stopped at 2019-03 (44%, ~1.7 h) |
| L3 | fetch actual rows for sample keys | used on 1 category |

Scripts in `scripts/`. The monthly L1 rollup is in `scripts/l1_monthly_result.txt`.

### Results

- **Key sets are identical in 119,743 of 119,745 hours.**
- **The old table is missing 12 ledgers** that the new one has (2,862 rows):
  - `96868120–96868121` (2025-06-17 11:56)
  - `97066622–97066631` (2025-06-26 11:06)

  These are ingestion gaps in the old run, and the only missing or extra rows found.
  The addresses touched there should also have different `oldBlockNumber` pointers
  on their next rows (not yet verified).
- **The old table carries heavy duplication:** 16.27B rows for 13.24B keys (3.03B dups).
  It also has 3.0M conflicting keys (same key, different float values), 1.9M of them
  in 2024-07/08.
- **The new table is clean:** 13.24B rows = keys, 0 dups, 0 conflicts. On 09-14 it still
  had 14.7M first-run dups, which have since merged.
- **All content differences found are float noise.** Exact content hashes differ in
  47.5k hours. L2 (through 2019-03, 1.26B keys) found only noise, max relative diff
  3.9e-16, with zero tolerance violations and no non-float column differences. Each
  conflicting old key has a version that matches new, except 2 keys, which are ULP
  noise too (an issuer's own IOU, 2015).
- **Verdict so far: new ⊇ old.** New is cleaner and recovers 12 ledgers. 2019-03 →
  2026-09 content has not been checked at the field level.

### Learned / gotchas

- **Cost is in the per-key cross-shard GROUP BY and whole-row hashing**, not in the scan.
  It ran at ~2.5M rows/s per query and 3–5 GiB, so 29B rows took ~3 h. That's too slow
  for the budget.
- **Shards:** there are 3 shards with **no sharding key**, so duplicates of one key can
  sit on different shards. Dedup must be global: GROUP BY through the Distributed
  table, or dedup-invariant aggregates.
- **Dedup-invariant, tolerance-aware fingerprint (prototype, `scripts/guardrail_day.sql`):**
  `uniqExact(key_hash)` plus `sumDistinct(hash(key_hash, float bits >> 20, …))`. The
  `>> 20` keeps ~32 mantissa bits (~2e-10 relative), so ULP noise falls into the same
  bucket. It gave **identical fingerprints for 2024-01-01/02**, where L1's exact hash
  differed in every hour (84k conflicts). Timing not yet measured.
- **SQL:**
  - `cityHash64` returns NULL if any argument is NULL. XRP `issuer` is NULL, so
    canonicalize with `isNull(x), ifNull(x, default)`. This bit me once.
  - `SELECT *` skips MATERIALIZED `assetRefId`.
  - `x IN (col, col)` is invalid; use `=`/`OR`. The old analyzer's error message for it
    is misleading. The analyzer isn't needed, so don't override it.
  - IN subqueries on Distributed tables need `GLOBAL IN` (`distributed_product_mode = deny`).
- **Environment:**
  - `system.query_log` / `system.processes` are per-broker, and connections are load
    balanced, so timing must be measured client-side.
  - The container lacks `column`, `bc`, `/usr/bin/time`.
  - `pkill -f <pattern>` matched its own shell; kill by PID.

## Plan

1. **Tiered checks with a budget.** Only Tier 1 runs over the full data every time:
   - **Tier 0 (free):** `system.parts` rows per partition, min/max dt/block per side.
   - **Tier 1 (full data, target ≤1 h, 2 queries):** per day and side, all dedup-invariant:
     - blocks via `uniqExact` + min/max, which catches gaps like the 12 ledgers
     - tx count
     - key count
     - truncated-content `sumDistinct` fingerprint
     - raw rows (duplication census)

     These mirror the `table_qa` yearly checks at daily resolution. First measure
     `guardrail_day.sql` timing. If it's too slow, split cheap block/tx counts from
     the key/content fingerprint.
   - **Tier 2 (only failing days):** hourly drill-down, then L2 join limited to those hours.
   - **Tier 3 (new table alone, invariants):** balance-chain continuity (`oldBalance`/
     `oldBlockNumber` = previous row); stacks sum to zero per block; total supply.
   - **Tier 4 (optional):** per-issuerCurrency float sums for monster IOUs.
2. **Generalize:** a per-table config (key cols, float cols, nullable cols, block/tx cols)
   that drives the same queries for transfers, balances and stacks. Metrics use the
   `metric_baselines.py` signature idea over `*_experimental`.
3. **Executive summary generator** from the cached results (verdict, table of checks,
   discrepancies with cause).
4. **Next runs:** re-run XRP balances with Tier 1 to validate time and verdict against
   this iteration, then XRP stacks after its backfill completes.
5. **Cleanup:** scratchpad caches `xrpcmp/l1`, `xrpcmp/l1v2` (removal was blocked by the
   safety check; they're ephemeral anyway).
