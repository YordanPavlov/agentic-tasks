# stack_circulation_100y — cancel-free total supply

**Started:** 2026-07-27
**Repo:** `clickhouse-tables` (branch `stackCirculation100`)
**PR:** [#2308](https://github.com/santiment/clickhouse-tables/pull/2308) — introduce `stack_circulation_100y`, root `total_supply` on it

## What this task IS

`stack_circulation_20y` doubles as the total-supply proxy
(`total_supply = coalesce(custom_total_supply, stack_circulation_20y)`), but
`circulation_job.compute_delta_cancels` books an aging-out cancel at
`odt + period` into the delta-futures tables for every delta. In the supply
role those cancels are meant to never fire, yet all are persisted — and they
*do* eventually fire, breaking the proxy. This task introduces
`stack_circulation_100y` computed with **no future cancels at all**, re-roots
`total_supply` (and later the other supply-intent consumers) on it, and paves
the way to deprecating the 20y family.

Spun out of the parked [ltc-stacks-deprecation](../2026-06-ltc-stacks-deprecation/ltc-stacks-deprecation.md)
task (findings F4/F6 context only — none of that task's work is in scope here).

## Decisions

- **Balances-based total supply (`total_supply_from_balances`) DECLINED as a
  concept** (operator, 2026-07-27). Not worked on; supply stays in the stacks
  family.
- **No per-metric `futureCancels` flag** (operator: configurable = misuse
  surface). Instead a hard rule in `circulation_job`:
  `INFINITE_WINDOW_PERIOD_DAYS = 36500`; periods at/above it are an infinite
  window and are excluded from `compute_delta_cancels` entirely. The deltas
  side needs no change (`dt − odt < period` is always true at 100y; the
  futures read finds zero rows).
- `stack_circulation_delta_100y` = `period: 36525` with the standard
  `coinAgeMetric` + `circulationDelta` labels → slots into the existing
  per-chain circulation cronjobs; `stack_circulation_100y` (`sumMetric`) →
  picked up by the existing cumulative-sums cronjobs. Zero new cronjobs.

## Validated findings (prod, 2026-07-27)

- Code path: `circulation_job.py` writes `future_dt = toDate(odt + period)`
  into `daily_delta_futures`; same pattern copied in
  `circulation_intraday_job`, `realized_cap_job`, `realized_cap_intraday_job`
  (+ active-addresses/holders/creation-ts-intervals jobs, which are
  genuinely windowed).
- **Daily** `daily_delta_futures` (replicated, 5.8B rows / 36 GiB per broker):
  `stack_circulation_delta_20y` (32) = 136M rows, **99% future-dated** (to
  2046); `stack_realized_cap_usd_delta_20y` (60) = 141M rows, 99.1%.
- **Intraday** `intraday_delta_futures` (94.8B rows / 843 GiB per broker):
  `stack_circulation_intraday_delta_20y` (348) = 4.82B rows and
  `stack_realized_cap_usd_intraday_delta_20y` (311) = 4.56B rows — both
  **100% future-dated**, zero ever consumed; ~92 GiB per broker ≈ 285 GiB
  cluster-wide for the 20y pair alone.
- **Correctness cliff:** first 20y cancels land in-range **2029-01-09 for
  BTC and BCH** (LTC 2031-10, XRP 2032-12, DOGE 2033-12, ETH 2035-07). From
  that day `circ_20y` — and `total_supply`, marketcap, dominance, inflation,
  S2F, NVT — silently decays as old coins age out of the 20y window.
- Anomaly noted for the equality check: daily 20y partition holds cancels
  with `future_dt` as early as 2009 ⇒ some rows carry pre-genesis garbage
  `odt` (≈1989). Coins "older" than 20y from garbage odt are excluded by
  `circ_20y` but included by `circ_100y` — any equality diff is that
  pre-existing garbage, not methodology.
- Consumer inventory of `circ_20y`-as-supply (to re-root later):
  `total_supply`, `stack_mean_creation_timestamp`, `stack_total_age`,
  `standard_deviation_of_coin_age`, `percent_of_total_supply_in_profit`,
  `percent_of_total_supply_on_exchanges`, `non_exchange_token_supply`, `nvt`,
  `nvt_transaction_volume`, `stock_to_flow_ratio`, `annual_inflation_rate`
  (uses `delta_20y`), liquity/makerdao/maple supply+marketcap aliases.
  Entanglement: `mean_realized_price_20y = rc_20y/circ_20y` (daily and
  intraday), and `rc_20y` is the all-time denominator in ~10 hodl-band
  composites — deprecating `circ_20y` drags the whole 20y family
  (circulation + realized cap, daily + intraday).

## Plan

1. **PR #2308 — MERGED 2026-08-21** (`4484d218`, released `v3.0.3`/`v3.0.4`).
   Shipped: infinite-window rule in all four jobs (`circulation_job`,
   `circulation_intraday_job`, `realized_cap_job`,
   `realized_cap_intraday_job`) + **8** 100y metric specs (circulation +
   realized cap, daily + intraday). The `total_supply` re-root was **dropped
   from the merge** — master's `total_supply_metrics.yaml` still depends on
   `stack_circulation_20y`, so nothing consumes `circ_100y` yet. Metadata
   registered: prod 2622–2629, stage 4001–4008. Genesis backfill run for
   **BTC + BCH** on 2026-08-25 12:04–12:06 (daily 6447 days, intraday ~1.86M
   points per metric). Equality check vs the 20y twins: **BTC passes, BCH
   fails on all 8 pairs** — root cause is pre-existing BCH source damage, not
   this change. See "BCH divergence" below.
2. Re-root the remaining supply-intent consumers (separate PR, value-neutral
   today; includes moving `annual_inflation_rate`/`stock_to_flow_ratio` to
   `delta_100y` — `delta_20y` starts including firing cancels in 2029).
   **Blocked for BCH** until its distribution deltas are repaired.
3. Backfill the remaining stacks chains (only BTC + BCH have 100y history).
4. Deprecate the 20y family: remove specs, drop futures partitions (daily 32
   & 60; intraday `(348,*)` & `(311,*)`) → ~285 GiB reclaim. **Prereq:**
   check sanbase for direct API exposure of the 20y metric names (outside
   clickhouse-tables).

## BCH divergence (investigated 2026-08-26)

Not a methodology difference: a fresh recomputation exposed damage in the
shared source table. **BTC is clean** — max relative diff 1.7e-5 % on 2 of
6439 days (float noise). **BCH differs on all 8 pairs** (prod, deduped by
`argMax(value, computed_at)`):

| metric | compared | differing | max diff | first diff |
|---|--:|--:|--:|---|
| `stack_circulation` | 6439 d | 6439 | 100% | 2009-01-09 |
| `stack_circulation_delta` | 6439 d | 872 | 100% | 2009-01-09 |
| `stack_realized_cap_usd` | 6439 d | 3322 | 106% | 2017-07-23 |
| `stack_realized_cap_usd_delta` | 6439 d | 3184 | 100% | 2017-07-23 |
| `stack_circulation_intraday` | 1.854M pts | 1.854M | 100% | 2009-01-09 |
| `stack_circulation_intraday_delta` | 1.854M pts | 76k | 1489% | 2009-01-09 |
| `stack_realized_cap_usd_intraday` | 1.854M pts | 956k | 106% | 2017-07-23 |
| `stack_realized_cap_usd_intraday_delta` | 1.854M pts | 344k | 11323% | 2017-07-23 |

Live (dt 2026-08-25): circulation 20,710,675 (20y) vs 19,713,101 (100y) =
**−4.82 %**; realized cap $6.366B vs **−$406.8M** — the 100y realized cap goes
*negative* (spends without their matching acquisitions). No prod regression:
`total_supply` still tracks `circ_20y` (re-root not merged).

### Source damage

Real source of all these jobs is `distribution_deltas_5min` /
`age_distribution_5min_delta` (**metric_id 162**), not the stacks table.
BCH (`asset_id 2484`) has two holes there:

- **Head gap** — the BCH 5min series starts **2009-02-03**; 2009-01-09→02-02
  missing (25 active days, **−139,200 BCH**, the constant offset visible
  2010→2020).
- **643-day gap — 2024-08-16 → 2026-05-20**, BCH entirely absent.
  **BCH-only**: BTC/ETH/LTC/DOGE/XRP each have 643/643 days.
- **2021–2023** (272 + 135 + 12 days): both series non-zero but the legacy 20y
  values carry spikes that no longer reproduce — 2021-07-30: 20y = 289,148 vs
  **812.49** recomputed by hand from the source = the 100y value (same on
  2021-01-14 → 887.49, 2021-01-31 → 987.50). **The 100y values are the
  faithful ones**; the 20y series is stale there.

### Root cause: a DELETE that raced its own writer

```
distribution_deltas_5min
DELETE WHERE metric_id IN [162] AND asset_id IN [2484] AND seq_num > 166
create_time = 2026-05-25 08:36:50   is_done = 1
```

Part of a wider seq-rewind campaign that week: `asset_id = 1452` (bitcoin) on
05-19/05-20 → BTC's full history rewritten 05-20 14:08–21:17; `asset_id = 2462`
(litecoin) on 05-21 → LTC rewritten 05-21 20:06–21:45; a global
`DELETE WHERE seq_num > 62310` on 05-22. BTC and LTC got their replays; **BCH
got the delete and only a partial replay.**

- **The data existed.** `age_distribution_1day_delta` (derived *from* the 5min
  metric) is intact across the gap; its BCH rows for 2025-07-04 were written
  2025-07-14 and sum to **80,453.765** = exactly the legacy `delta_20y` value.
  That 1day series is now the only surviving copy of the gap-era data —
  **do not re-run `bch-age-distribution-1day-deltas` for those months before
  the 5min data is restored** (it would overwrite good values with zeros, and
  `computed_at` versioning means the zeros win).
- **Why it was never replayed.** Surviving pre-gap `max(seq_num)` = 166, but the
  replay resumed at **62311** — the *deleted* rows' max + 1. The writer read its
  cursor before the mutation materialised (delete 08:36:50, first replayed write
  08:57:16), mapped that block to ≈2026-05-20/21 via `date_for_sequence_num`,
  and resumed at the tip. Seq numbers **167–62310 are now orphaned**.
  Reproducible: any DELETE against a sequenced destination while its writer runs
  will skip the deleted range permanently.
- **Structural handover**: blocks ≤ 858815 / dt ≤ 2024-08-13 = date-driven job
  (`seq_num = 0`, 255.36M rows); ≥ 858816 = sequenced job (1.07M rows,
  seq 1–166 then 62311–72654).
- **Why BCH only.** BCH is the *only* chain whose age-distribution job was moved
  to the sequenced/immutable DAG (`age_distribution_intraday_sequenced_job` in
  `intraday-metrics-bch-immutable`). The other 14 chains keep the date-driven
  `age_distribution_intraday_job` inside `intraday-metrics-<ticker>` — which is
  the graph `intraday-metrics-<ticker>-historical` is built from. So BCH is the
  only asset with **no date-range replay path**, and the only one that took a
  seq rewind without a full replay.

### Repair options considered

`bch_stacks` (now `bch_stacks_v12_shard`) covers the **entire** gap — all 22
months, every day — so nothing upstream is lost. Volume to rewrite ≈ **7–9.5M
rows** (measured 10.7–14.7k rows/day).

- **No historical task exists** for the metric; clearing the immutable task
  doesn't backfill (`sequenced_job` derives its start from
  `latest_block_and_seq_num(destination)` and walks forward, ignoring the data
  interval; `catching_up_factor = 30` batches/run).
- (A) Mirror BTC: add a date-driven spec to `intraday-metrics-bch.yaml` → task
  appears in `intraday-metrics-bch-historical`, clear per month. Writes
  `seq_num = 0` rows into the clock's range.
- (B) Custom DAG (`custom-historical-metrics`) — needs (A)'s spec, since the
  existing job name resolves to the sequenced script and ignores dates.
- (C) **Preferred — bounded cursor override** on `sequenced_job`
  (`fromBlock`/`toBlock` or `fromDate`/`toDate` + `seqBase`). Same SQL (both
  variants share `query()` from `age_distribution_intraday_job`), but every
  repaired batch stays a proper clock entry. Daily batches → ~643 seq entries
  inside the orphaned 167–62310 window, 5-min output fidelity unchanged
  (batch window ≠ output granularity), and ~30 batches per monthly interval
  fits the 30-batch cap.
- **No pause needed.** The backfill is insert-only; the cursor is
  `maxIf(block_number|seq_num, is_finalized)` over `dt > now() - 150d`, and
  every backfilled value is strictly below the live ones (seq < 62311, blocks
  < 965779) landing in dt slots with no live rows — an INSERT can only move a
  `max` upward. Required guard (hard `_require`, data-derived, no hardcoded
  date): **refuse any batch whose dt range already contains rows with
  `seq_num > 0`** for that metric+asset. Pausing is only for *deletes* — and
  then wait for `system.mutations.is_done`.
- **Downstream won't self-heal**: consumer cursors sit at the tip, so rows
  numbered 167+ fall below their watermark. Each downstream repair must be run
  explicitly (bounded), intraday before daily.

### `seq_num` scheme — evaluation of `seq = to_block`

`seq_num` is a dense `+1` counter per batch, written as a constant alongside
`block_number = ` the batch's end block (`daily_metrics_queries.py:494,702`).
Density is exact in practice (0 missing across 10,346 and 166 values). It
conflates two roles: **position** (the cursor) and **coverage** (holes in the
counter = missing batches).

- **Wins of `seq = to_block`**: no allocator → order-independent, idempotent,
  parallel backfills; collapses the two cursors into one (removing the
  replica-disagreement class `sequence_number.py:38-42` exists to work around);
  self-describing; no orphaned ranges after a delete; forward-only migration
  since block numbers are an order of magnitude above the counters (965,781 vs
  73,586).
- **Blocking cost**: `calculate_sequence_gaps` is
  `(to - from) - count(DISTINCT column)` (`sequence_number.py:261-266`), and two
  jobs run it with `monotonic_sequence_field="seq_num"` —
  `stack_age_consumed_intraday_sequenced_job:21` and
  `cumulative_sum_sequenced_job:71` — because their source is an intermediate
  metrics table with no dense chain column. Block-valued seq is sparse by the
  blocks-per-batch: BCH tip ≈1–2 (deceptively passes), BCH catch-up ≈144, ETH
  5-min ≈25 → `_require(gaps_count == 0)` fails. It would look healthy in the
  BCH pilot and break on rollout.
- **Variant that works**: `seq = to_block` **plus** recording the batch's
  `from_block`. Coverage then asserted on chain intervals
  (`from_block == prev_end_block + 1`): holes become positively detectable with
  their exact block range, from the destination alone, and the job can self-heal
  by mapping that range back to a dt window. That half is what would have caught
  this incident the next morning — neither the current scheme nor a bare
  `seq = to_block` notices a delete in the *middle* of the range, since `max()`
  is untouched.
- **Scope is small today**: the logical-clock scheme has one tenant —
  `intraday_metrics` with `seq_num > 0` over 7 days = **1 asset, 10 metrics,
  8,184 rows**; `distribution_deltas_5min` = BCH only. Cheap to change now.
- **Open before adopting**: external readers of `seq_num` (sanbase/API, outside
  this repo); the other writers (`airflow/dags/sf_intraday_metrics.py`,
  `sf_intraday_metadata_aware.py` also insert `seq_num`/`block_number`, one via
  `ROW_NUMBER() OVER (PARTITION BY address ORDER BY dt)` — a different concept
  sharing the column name); `is_finalized` monotonicity across re-runs.

## Confirmation queries (prod, `-u readonly`)

```sql
-- daily (seconds)
SELECT m.name, count() AS rows, countIf(f.future_dt > today()) AS future_dated,
       round(future_dated / rows * 100, 1) AS pct, max(f.future_dt) AS max_fdt
FROM daily_delta_futures f
LEFT JOIN metric_metadata m ON m.metric_id = f.metric_id
WHERE f.metric_id IN (32, 60) GROUP BY m.name;

-- intraday: same on intraday_delta_futures, metric_id IN (348, 311) (~4 min)

-- the 2029 cliff
SELECT a.name, min(f.future_dt) AS first_cancel_fires
FROM daily_delta_futures f LEFT JOIN asset_metadata a ON a.asset_id = f.asset_id
WHERE f.metric_id = 32 AND f.future_dt > today()
GROUP BY a.name ORDER BY first_cancel_fires LIMIT 5;

-- 100y vs 20y for one asset (dedup by computed_at — the 20y rows carry duplicates)
WITH ded AS (
  SELECT metric_id, dt, argMax(value, computed_at) AS v
  FROM daily_metrics_v2
  WHERE metric_id IN (20, 2626) AND asset_id = 2484  -- bitcoin-cash; btc = 1452
  GROUP BY metric_id, dt),
j AS (
  SELECT dt, anyIf(v, metric_id = 20) AS v20, anyIf(v, metric_id = 2626) AS v100
  FROM ded GROUP BY dt)
SELECT toYear(dt) AS y, count() AS days,
       countIf(abs(v100 - v20) > 1e-6 * greatest(abs(v20), 1)) AS diff_days,
       round(sum(v100 - v20), 1) AS sum_diff
FROM j GROUP BY y ORDER BY y;

-- the BCH source holes (metric 162 = age_distribution_5min_delta)
SELECT toYear(dt) AS y, uniqExact(toDate(dt)) AS days, max(computed_at) AS last_write
FROM distribution_deltas_5min
PREWHERE metric_id = 162 AND asset_id = 2484
GROUP BY y ORDER BY y;

-- the mutation that caused it
SELECT command, create_time, is_done FROM system.mutations
WHERE table = 'distribution_deltas_5min' AND command LIKE '%2484%'
ORDER BY create_time DESC;

-- the live cursor the sequenced writer reads (assert unchanged during a backfill)
SELECT maxIf(block_number, is_finalized) AS cur_block,
       maxIf(seq_num, is_finalized)      AS cur_seq
FROM distribution_deltas_5min
WHERE metric_id = 162 AND asset_id = 2484 AND dt > now() - INTERVAL 150 DAY;
```

---

## Session log

### 2026-07-27 — validation, PR #2308, plan

Re-validated F4 against prod (numbers above; intraday 20y pair turned out
100% future-dated, sharper than the parent doc's estimate). Operator declined
both the balances-based supply concept and a configurable no-cancel flag →
hard `INFINITE_WINDOW_PERIOD_DAYS` rule. Shipped PR #2308 (circulation_job
rule + `stack_circulation_[delta_]100y` specs + `total_supply` re-wire;
+36/−3). Not merged — backfill gate documented in the PR body. Next: metadata
registration + stage backfill/equality run, then consumer re-root PR.

### 2026-08-25 — prod equality validation: BTC clean, BCH broken

The 100y family is now live on prod (all 8 metrics — circulation + realized
cap, daily + intraday, ids 2622–2629) with genesis backfill for BTC and BCH.
Ran the 20y-equality check on both chains, all 8 pairs each:
**BTC passes** (equal within Float64 noise, ≤1 satoshi / sub-cent; the
"bit-equal" expectation is literally false only due to summation order).
**BCH fails everywhere** — circ gap −997.6k BCH (~5%), RC gap $11.7B (6.4%),
frozen since 2026-05-09 (live computation agrees again). Three components:
−139.2k accumulated over 2009-01-09..02-02; −550k in discrete 2021 steps
(candidate: the known master bugfix); and `delta_100y` = literal 0 for
2024-08-16..2026-05-09 (~−308k net) — smells like a backfill defect/data
hole, not a bugfix. Full method + numbers:
[100y-vs-20y-prod-validation](100y-vs-20y-prod-validation.md). Next: map the
known bugfix onto the 2021 steps, check `bch_stacks` for the zero-window.

### 2026-08-19 — PR #2308 CI blocked (not a defect in this change)

The `build and push` job fails because the two new metrics are not yet in
`metric_metadata_versioned` and the August CI refactor now generates the
dependency graphs *during* the image build. Metadata registration — already the
next step here — has become a hard prerequisite for a green build, and the CI
path that would do it runs dry-run on branches. Analysis and fix options:
[ci-metric-registration-gap](../2026-08-ci-metric-registration-gap/ci-metric-registration-gap.md).

### 2026-08-26 — equality check: BTC clean, BCH root-caused

PR #2308 turned out to be merged (2026-08-21) and BTC + BCH backfilled
(2026-08-25) in sessions that were never journaled; reconstructed the state from
prod `computed_at` values and the merge diff. BTC passes the equality check;
**BCH differs on all 8 metric pairs**, and the cause is not the 100y rule —
it is a 643-day hole (2024-08-16 → 2026-05-20) plus a 25-day head gap in
`distribution_deltas_5min` for BCH, created by a `seq_num > 166` DELETE on
2026-05-25 that **raced its own writer**: the sequenced job read its cursor
before the mutation materialised, so it resumed at the tip instead of at the
hole and orphaned seq 167–62310. BCH is the only chain exposed to this because
it is the only one whose age-distribution job lives in the sequenced/immutable
DAG, hence the only one with no date-range replay path. Full write-up, repair
options and the `seq = to_block` evaluation in the sections above. All
read-only — no code changed, nothing scheduled.

**Next (separate session):** most likely **revert the sequencing approach for
BCH** and move it onto the common time-interval jobs the other 14 chains use
(`age_distribution_intraday_job` in `intraday-metrics-bch` → gives BCH a
historical DAG task like everyone else), then repair the gap through it. That
supersedes repair options (A)–(C) above and makes the `seq_num` redesign moot
for this asset — the logical-clock pilot would then have no production tenant,
which is itself a decision worth taking deliberately.
