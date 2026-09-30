# Re-run comparison framework (old vs new source tables / metrics)

**Started:** 2026-09-29
**Status:** XRP balances done with the tiered design (Tier 1 + 2, 2026-09-30). **Superseded** by the generic
runbook and scripts in [`2026-09-table-comparison`](../2026-09-table-comparison/table-comparison.md); continue there.
This doc keeps the development history.
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

  These are the only missing or extra rows found. Their cause (verified 09-30): lost output
  rows, not a processing gap. See the second iteration.
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
  2026-09 content has not been checked at the field level (closed by the second iteration).

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
  differed in every hour (84k conflicts). This became Tier 1.
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

## Second iteration: tiered checks, XRP balances (2026-09-30)

Same tables and cutoff. Scripts: `scripts/tier1.py`, `tier1_compare.py`, `tier2.py`, `tier2_summary.py`.

- **Per-table config:** `tier1.CONFIGS` holds the old/new table and the dt/block/tx columns. The key is
  the shard table's `sorting_key`, value columns and types come from `system.columns`, and window
  sizes (~60M rows) from `system.parts` (Tier 0).
- **Caching:** results are cached per (side, window) under `$TIER1_CACHE`. This run used the session
  scratchpad, which is ephemeral.
- **Method:** as in L1, the same query runs on each side and the results are compared locally.

| tier | what | scope | wall time |
|---|---|---|---|
| T1 | per day and side: `count`, `uniqExact`/min/max blocks, `uniqExact` tx, `uniqExact` keys, `sumDistinct` of hash(key, content with floats `>> 20`) | full history, 5,015 days | **97 min** (1 worker per side, ~2.7M rows/s avg) |
| T2 | T1's flagged days: UNION ALL old+new, GROUP BY key, distinct content hashes per side → category, exact max rel diff per float column, mismatch count per other column | 148 days, 1.46B rows | 28 min (2 workers) |

### Results

- **T1 reproduced L1 exactly:**
  - new has +2,862 keys and +12 blocks
  - old has 3.03B dups
  - only 2 days differ in blocks/txs/keys (2025-06-17: +2 ledgers, 2025-06-26: +10)
  - 146 more days differ in content only
- **T2 classified every flagged day:**
  - 13,584 keys `old_conflict_new_matches`: old has extra versions, one of which equals new. Benign.
  - 9 keys `value_diff` (2013–2015), max relative diff 1.1e-16. Bucket-edge noise.
  - 2,862 keys `only_new` on the 2 gap days.
  - 0 tolerance violations, 0 non-float mismatches.
- **Cause of the 12-ledger gap:** in old, the row after the gap (e.g. ledger 97066632) has
  `oldBlockNumber = 97066631` and the correct `oldBalance`. So the old job processed those ledgers and
  their rows were lost on write. Old's balances are correct, but it has ~2,862 dangling chain pointers.
  That's also why no downstream mismatches appear.
- **Verdict: new ⊇ old, content-equal within tolerance over the full history.** New is clean, has no
  dups, and recovers the 12 ledgers.

### Learned

- **Cold vs warm matters a lot.** The first cold 2-day probe took 48 s, the same query warm took 9.6 s,
  and a cold heavy month ran at ~5.6M rows/s. The full-run average (~2.7M rows/s) is dragged down by
  hundreds of small early windows, since per-query overhead dominates there.
- **Truncation bound:** `>> 20` guarantees that any change larger than ~4.7e-10 relative moves the
  fingerprint, which is below the 1e-9 tolerance. Its false positives are old's multi-version keys and
  values sitting on a bucket edge. T2 clears both.
- **T2 speed:** ~0.8M rows/s, i.e. 84 s for a heavy day and a few seconds for small ones.
- **SQL:**
  - `tier2.py` canonicalizes values into `v_<col>` aliases. Shadowing real column names would change
    the hash expression.
  - `SELECT *` in UNION ALL fails because of MATERIALIZED `assetRefId` in old.
  - `clickhouse-client --format` overrides a `FORMAT` clause in the query, so pass the format
    through the client.
- **Query catalog:** one example per type, with status and cost, was published as a private claude.ai
  artifact: https://claude.ai/code/artifact/9fc086bf-da16-451f-916b-c61d08fda02b. That was a mistake,
  since outputs should stay local (now a rule in CLAUDE.md). Delete it from the gallery, and keep a
  local copy here if one is wanted.

## Plan

1. **Tiered checks with a budget.** T1 and T2 are done for XRP balances; the remaining items follow.
   Only Tier 1 runs over the full data every time:
   - **Tier 0 (free):** `system.parts` rows per partition, min/max dt/block per side.
   - **Tier 1 (full data, target ≤1 h, 2 queries):** per day and side, all dedup-invariant:
     - blocks via `uniqExact` + min/max, which catches gaps like the 12 ledgers
     - tx count
     - key count
     - truncated-content `sumDistinct` fingerprint
     - raw rows (duplication census)

     These mirror the `table_qa` yearly checks at daily resolution. **Done: 97 min.** To get under
     1 h:
     - test bucketing keys/fp by `kh % 16`, a parallel two-level merge (`sumDistinct` sums add up
       mod 2^64)
     - merge the small early windows into fewer, larger ones
   - **Tier 2 (only failing days): done.** If many days fail, first re-run T1 per hour on those days.
   - **Tier 3 (invariants, per table):**
     - **Balance-chain continuity.** Sketch: the set of produced (address, block, balance), minus
       each chain's last state, should equal the set of consumed (address, oldBlockNumber,
       oldBalance). Also, chain starts should equal the number of (asset, address) pairs.
     - Run it on both sides: expect ~2,862 dangling pointers in old and 0 in new.
     - Still open: how `oldBlockNumber` points when an address has several rows in one ledger.
     - Also planned: stacks sum to zero per block, and total supply.
   - **Tier 4 (optional):** per-issuerCurrency float sums for monster IOUs.
2. **Generalize:** a per-table config (key cols, float cols, nullable cols, block/tx cols)
   that drives the same queries for transfers, balances and stacks. T1/T2 already derive
   key and columns from metadata; next is adding stacks/transfers entries to `CONFIGS`. Metrics use the
   `metric_baselines.py` signature idea over `*_experimental`.
3. **Executive summary generator** from the cached results (verdict, table of checks,
   discrepancies with cause).
4. **Next runs:** XRP balances validated with T1/T2 (09-30). XRP stacks comes after its backfill
   completes. On 09-30 `test.xrp_stacks_test` covered 2013 → 2021-12-31 (block 68.7M).
5. **Cleanup:**
   - scratchpad caches `xrpcmp/l1`, `xrpcmp/l1v2` (removal was blocked by the safety check; they're
     ephemeral anyway)
   - dead `fp_nc` line in `l1_analyze.classify`
