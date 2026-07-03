# Action plan — migrate LTC off `ltc_stacks` onto the balances tank

**Date:** 2026-06-18
**Author:** Yordan + Claude analysis session
**Repo:** `clickhouse-tables` (Daily Metrics Framework)
**Goal:** stop LTC metric computation from reading `ltc_stacks`, computing instead off the `ltc_balances` tank (`balance`, `averageBalanceBirthTimestamp = τ`). Phased; Phase 1 is fully specified and testable, later phases are sketched.

**Analysis basis:** [`../stack-circulation-and-realized-price-on-average-balance-age.md`](../stack-circulation-and-realized-price-on-average-balance-age.md).

> **Read these first.** Before working this plan, read the foundational readmes under `~/src/santiment-cheatsheets/claude-analysis/` — they define what `stacks` and `balances` actually are and why the migration is hard:
> - [`stacks-from-construction-to-usage.md`](../stacks-from-construction-to-usage.md) — the full pipeline: Flink builds per-cohort LIFO segments (a spend emits **one `−1` per consumed cohort, each stamped with that cohort's real `odt`**); the seam → `distribution_deltas_5min`; the dozen consumer jobs. The load-bearing invariant: every cancel reuses a prior install's `odt`.
> - [`stack-circulation-and-realized-price-on-average-balance-age.md`](../stack-circulation-and-realized-price-on-average-balance-age.md) — why the single-average `(B, τ)` **balances tank** (a spend emits **one `−1` at the pooled-average τ**) can't faithfully replace per-cohort stacks; what survives, what drifts, what breaks.
> - [`bounding-flink-stack-cohort-state-with-5min-buckets.md`](../bounding-flink-stack-cohort-state-with-5min-buckets.md) — the state-growth problem on the producer side.

---

## 0. The load-bearing distinction (why this attempt differs)

The previous multi-chain age-balances migration failed at **one** step: the **futures/cancels** windowing in `circulation_job` → `cumulative_sum_job`. It schedules `−amount` at `odt+T` and relies on a spend's cancel landing on the same day as the receive's age-out ("cancel the cancel"). Fed the tank's synthetic average `τ` as the outflow `odt`, the cancel lands at `τ+T` instead of `t1+T` → deterministic phantom ± pulses.

A plain `arrayCumSum` over the delta stream books **no cancel**, so there is nothing to mispair. Therefore:

- **Keep** the delta stream + plain cumulative sums for every metric that does **not** schedule an in-range cancel.
- **Replace** the windowing-cancel step with a **backwards balance-dip scan** — but only for windowed metrics, in a later phase.

This means Phase 1 is mostly *reuse*: re-source the existing delta pipeline from the tank and let the existing cumsum jobs run unchanged. No balance-dip and no per-address cancel-chain fix are needed in Phase 1 (no cancels are consumed).

---

## Execution status & findings (2026-06-18)

Phase-1 code/spec changes are **implemented on branch `ltcAgeBalance`** (not yet run). Where this section conflicts with §1 below, this section wins (notably: **supply is no longer `circ_100y`** — see finding F4).

### Implemented

| Change | File |
|---|---|
| Age-distribution emitter `stacksFunction` `ltc_stacks.LTCStacks` → `ltc_balances.LTCAgeBalances` | `specs.d/dags/intraday-metrics-ltc.yaml` |
| Restored τ-based `UTXOAgeBalancesBase` (single-row delta, `odt = coalesce(oldAverageBalanceBirthTimestamp, averageBalanceBirthTimestamp)`) | `job_functions/utxo_age_balances_base.py` |
| `LTCAgeBalances` (sources live `ltc_balances`) | `job_functions/ltc_balances.py` |
| `LTCRawBalances` — raw `balance`/`oldBalance` passthrough (no stacks proxy) | `job_functions/ltc_balances.py` |
| `total_supply_from_balances_job` — `Σ(balance−oldBalance)`/day, **no odt, no futures** | `jobs/total_supply_from_balances_job.py` |
| Metrics `total_supply_from_balances_delta` + `total_supply_from_balances` (cumsum) | `specs.d/metrics/total_supply_metrics.yaml` |
| LTC delta cronjob | `specs.d/cronjobs/total_supply_from_balances_cronjobs.yaml` |

The live `total_supply` and its riders (marketcap, dominance, FDV, nvt, s2f, inflation) are **untouched** — `total_supply_from_balances` runs **side by side** (zero blast radius); re-rooting the riders is deferred to a later phase. The old `migrate-stacks-to-age-balances` skill was deleted (it encoded the failed wholesale delete-and-rewire approach).

### Findings & decisions (with why)

- **F1 — The LTC/DOGE/BCH age-balances migration was already done and reverted.** Commit `355ff914` ("LTC and Doge migrated to age balances model"), reverted by `1ad1ca0c` ("Restore usage of 'stacks' for BCH, LTC and Doge metrics"). That is the "old failed migration." `UTXOAgeBalancesBase` was deleted in the revert; restored from `9295b996`.
- **F2 — Correct emitter is `LTCAgeBalances`, not the leftover `LTCBalances`.** `ltc_balances` carries **both** `dt`/`oldDt` (event times) and `averageBalanceBirthTimestamp`/`oldAverageBalanceBirthTimestamp` (τ). `LTCBalances` (used by active-addresses) emits `odt = oldDt`/`dt` = **last-touch** → all-time mean-age/creation-timestamp would become "time since last activity," not pooled coin age. *RULED OUT as the age-distribution emitter.* `LTCAgeBalances` emits `odt = τ`, giving exact `Σ B·τ` (§5.5). The single-row vs. the validated 2-row cancel-chain fix is immaterial to the Phase-1 cumsum-safe set (only matters for windowed-cancel metrics = Phase 2).
- **F3 — Source table is the live `ltc_balances`** (1.66B rows, 2011→now). The reverted code's `ltc_age_balances` table **does not exist**.
- **F4 — Supply must NOT route through a circulation window (`circ_100y`). RULED OUT.** `circulation_job.compute_delta_cancels` writes a `−amount` future cancel at `future_dt = toDate(odt + period)` into the futures table; for a 100y period that is ≈ year 2111 — persisted indefinitely and never consumed (storage cost + non-obvious intent). (The existing `circ_20y` has the same latent issue, dated ≈2039.)
- **F5 — Reusing `creation_timestamp_job` (moment:0) for supply. RULED OUT.** It *is* a futures-free delta→cumsum and `moment:0` mathematically equals `Σ amount` = supply, but the job's identity is "creation-timestamp moments of the age distribution" — reusing it re-introduces the same intent-obfuscation as F4.
- **F6 — DECISION: supply = dedicated no-futures `Σ(balance − oldBalance)` accumulator,** read from the tank's **real balance columns** (the `(sign, amount)` `LTCBalances` output is a stacks-shaped band-aid). The cumsum reuses the generic `cumulative_sum_job` (a running total carries no intent baggage); the metric is named `total_supply_from_balances` to make intent explicit.
- **F7 — Total-supply re-root stays side-by-side for now** (Yordan): leave the live `total_supply`/riders on `circ_20y`, validate the balance-derived supply first, re-root later.

### Remaining

**UPDATE 2026-06-19: executed — see "Execution log & local-run setbacks" below.** (Metadata isolated via experimental table+dict rather than registering in prod `metric_metadata`; the cumsum-safe set ran over a genesis-anchored 3y window; foundation now populated but values diverge widely — value-correctness investigation pending.)

---

## Execution log & local-run setbacks (2026-06-19)

Phase-1 experimental run **EXECUTED** for LTC against prod `_experimental` tables over a genesis-anchored window. The idea of moving the **173 GiB `ltc_balances` tank to a local ClickHouse was RULED OUT** (too much data); all computation runs via `main.py` against prod CH, writing only to `_experimental` tables.

### State now
- **Isolated metadata** (zero writes to shared prod metadata): `metric_metadata_experimental` table + `metrics_by_name_experimental` dict on **`default_cluster` (internal nodes only, NOT `global_cluster`/facing)**. New metrics `total_supply_from_balances_delta` = **10001**, `total_supply_from_balances` = **10002** (ids from a deliberately separate pool; prod max ≈ 2595).
- **`.env.dev`**: outputs → the 4 `_experimental` tables; `DAILY_METRIC_METADATA_TABLE` + `DAILY_METRIC_METADATA_VERSIONED_TABLE` → the snapshot; `DAILY_METRICS_BY_NAME_DICT` → experimental dict; `DAILY_ASSETS=litecoin/2019-01-01`; genesis-anchored dates; `DAILY_DRY_RUN=true` default (override `=false` on the CLI for writes).
- **Comparison tool**: `compare_ltc_experimental.py` (lives in this `action-plans/` dir) — prod vs `_experimental` per `metric_id` (coverage, max/mean rel-diff, first-divergence, verdict). Per-table numeric column: daily/intraday `value`, distribution_deltas **`measure`** (its `value` is a DateTime/odt).
- **Supply path** (10001/10002, reads `ltc_balances` directly) confirmed creating full-window rows. Age-distribution path validated only after fixing the emitter (setback #1).

### Setbacks overcome — CONSIDER FOR PHASE 2
1. **DAG-embedded cronjobs are NOT runnable locally.** `job.from_yaml` loads only `kind: DailyMetricsCronJob`; the LTC intraday cronjobs (incl. the age-distribution **emitter**) live only in `specs.d/dags/intraday-metrics-ltc.yaml` (`kind: AirflowDags`) since **f14df082** (PR #2203 "separate-intraday-dags", 2026-05-14). Effect: the emitter never ran → `distribution_deltas` empty → every age-distribution-derived metric silently = 0. Worked around with standalone **DO-NOT-MERGE** specs (`specs.d/cronjobs/ltc_intraday_experimental_DO_NOT_MERGE.yaml`). **Phase-2/prod fix needed**: either `spec.dag: intraday-metrics-ltc` on standalone specs + drop the inline DAG copies, or teach the loader to ingest `AirflowDags` (with a duplicate-name guard); verify against the Airflow/etl repo — and confirm prod LTC intraday itself runs correctly post-migration.
2. **Prod double-run risk.** DAG membership = a `spec.dag` field (`job_factory`); **no** standalone cronjob currently sets it, so all run in the "common" set. A plain no-`dag` standalone cronjob merged to prod would run in the common run **and** the per-chain DAG → executed twice (hence DO-NOT-MERGE).
3. **`default_simple_job` has no cursor/cap** — it recomputes the **entire** `[DAILY_START, DAILY_END]` range every invocation (the `catching_up_factor = 30` cap applies only to the sequence-number generator). Wide dates = full-history backfill (timed out over 2011→2026). Use short, genesis-anchored windows; full backfill needs an explicit loop. Phase-2 windowed metrics likely use the sequenced (cursor) generator — different mechanics to plan for.
4. **Cumsums require a genesis start** — cumulative metrics accumulate from the first row, so confirmation windows must begin at LTC genesis (2011-10-01), not mid-history, or absolutes are wrong.
5. **Metric-id resolution split.** Spec `metric_id` resolves from `metric_metadata_versioned_table` (NOT the view); dependency lookups go through `dictGet(metrics_by_name)` which was **hardcoded** — now honors `config.metrics_by_name_dict`, and `metrics_by_name_dict` was added to `ENV_STRING_CONFIGS`. Both must point at experimental objects.
6. **`total_supply_from_balances_job` SQL bug (fixed)**: `GROUP BY toDate(dt)` over an aliased `toDate(dt) AS dt` double-wrapped to `toDate(toDate(dt))` (CH error 215) → changed to `GROUP BY … dt`.

### Value-correctness analysis — RESOLVED (2026-06-19)

Entry tool: **`compare_ltc_experimental.py`** (beside this doc). venv `clickhouse_driver` + VPN to prod CH:
`~/santiment/src/clickhouse-tables/.venv/bin/python compare_ltc_experimental.py --start 2011-10-01 --end 2014-10-01 --table all`
Per `metric_id`, prod vs `_experimental`, LTC `asset_id=2462`. The genesis-anchored 3y window is computed in the `_experimental` tables.

**The "everything DIVERGE" report was a single ordering bug plus two comparison-tool artifacts. After fixing both, the migrated metrics match prod as predicted.**

#### Root cause of the headline failure: a missing `dependsOn` (NOT a methodology bug)

The planner **does** topologically sort jobs — but `job_factory.sort_factories` builds edges **only** from each metric's declared `dependsOn`; it does **not** infer the dependency from which table a `distributionFunction` reads. Daily `stack_age_consumed` (metric 8) had **no `dependsOn`** (just `type: stackAgeConsumed`), whereas its intraday twin `stack_age_consumed_5min` (95) and the creation-timestamp delta (90) both declare `dependsOn: age_distribution_5min_delta`. With no edge, the sort placed daily-8 **before** the distribution emitter → it read an empty `distribution_deltas_5min` → emitted all zero-defaults → `stack_cumulative_age_consumed` (163, cumsum of 8) = 0 too. The creation-timestamp chain (90→91) was fine because it *declared* the dependency. **In production this gap is masked**: the intraday DAG always runs before the daily DAG, so the distribution is already populated; the single mixed local run removed that temporal separation and exposed it.

**Fix applied:** added `dependsOn: age_distribution_5min_delta/2019-01-01` to the daily `stack_age_consumed` metric (`metrics.yaml`), mirroring metric 90 / the intraday twin. Verified with `job_order.py` that the emitter now sorts before daily-8. (Also a legitimate latent-correctness fix for prod, currently load-bearing only via DAG ordering.) Truncated all 3 LTC-only `_experimental` tables and re-ran clean (203s).

#### Post-fix results (clean 3y re-run + fixed comparison tool)

| Metric | mean rel-diff | verdict | reading |
|---|--:|---|---|
| `stack_total_creation_timestamp[_delta]` (90/91) | ~0.4–0.5% | **MATCH** | the per-cohort-vs-pooled-τ drift, as predicted |
| `stack_total_creation_squared_timestamp[_delta]` (173/174) | ~0.5% | minor-drift | τ² scale; matches |
| `stack_cumulative_age_consumed` (163) | ~4.8% | minor-drift | flow timing reallocation, converging in the integral |
| `stack_age_consumed` daily (8) / `_5min` (95) | ~14% / ~19% | DIVERGE (per-day) | LIFO-youngest-first vs pooled-avg-τ **day-by-day** timing; **converges in the sum** (below) |
| `age_distribution_5min_delta` (162) | ~3.9% on matched cohorts | minor-drift | + large coverage gap: prod has many odt cohorts/dt, exp one pooled-τ/dt (structural, expected) |

**The age_consumed divergence is timing reallocation, not loss** — the meaningful aggregates match:

| Aggregate | PROD | EXP | ratio |
|---|--:|--:|--:|
| Daily age_consumed Σ (8) | 10.81e9 | 10.98e9 | **1.016×** |
| Cumulative age_consumed final (163) | 10.81e9 | 10.98e9 | **1.016×** |
| Intraday age_consumed_5min Σ (95) | 21.60e9 | 10.98e9 | 0.51× |

EXP is **internally consistent** (daily Σ = 5min Σ = cumulative = 10.98e9) and the **daily metric matches prod within 1.6%** — exactly the §1.5 "timing reallocation, converging in the cumulative" prediction.

#### CORRECTION to an earlier hypothesis (do not propagate the old claim)

A mid-investigation note claimed age_consumed is a "~2× undercount on the tank" (net-vs-gross UTXO churn). **That was wrong** — it compared exp-5min against **prod-5min, which is itself 2× prod's own daily** (21.6e9 vs 10.81e9). The migrated daily metric is within 1.6%.

**The "prod `stack_age_consumed_5min` = 2× prod daily" question — RESOLVED 2026-06-22: NO prod anomaly. It was a deduped-vs-non-deduped query mismatch.** `daily_metrics_v2` and `intraday_metrics` are `ReplicatedReplacingMergeTree`; in the genesis window the row-versions were never merged, so **both** tables carry ~2× physical versions per `dt` (5-min: 1.96 rows/dt, daily: 1.994 rows/dt; 100 distinct `computed_at` from a re-backfill). The 2× came from an **ad-hoc mid-investigation query that summed the raw 5-min table with no dedup (21.6e9)** compared against a **daily figure that had been deduped (10.81e9)** — not from `compare_ltc_experimental.py` (which already dedups via `argMax(value, computed_at)`, the FINAL-equivalent). Deduped, prod daily Σ = prod 5-min Σ = **10.81e9** (conservation holds), and ratio = **1.000** for BTC/ETH/WETH/LTC across **every year 2009→2026** (lone 1.004 for ETH 2020). Our experimental run = 10.98e9 → 1.016× vs the *correct* prod value, for both daily and 5-min, as predicted.

**Fix landed so it can't recur:** `compare_ltc_experimental.py` now reports deduped `prod_sum` / `exp_sum` / `sum_ratio` per metric, so absolute levels and cross-metric conservation (daily Σ vs `_5min` Σ) are visible in-report — no separate ad-hoc (and un-deduped) sum needed. **Lesson:** never `sum(value)` an `intraday_metrics`/`daily_metrics_v2` slice without `argMax(value, computed_at)` (or `FINAL`) — unmerged versions double-count.

#### Comparison-tool fixes (committed in `compare_ltc_experimental.py`)

1. **Symmetric rel-diff** `|pv-ev| / mean(|pv|,|ev|)`, bounded [0,2] — kills the old 1e17–1e22 phantoms from `*_delta` zero-crossings and `*_squared_*` (τ²) near-zero prod denominators.
2. **Distribution joined on `(metric_id, dt, value=odt)`**, not collapsed per `(metric_id, dt)` via `argMax(measure)` (which compared arbitrary odt buckets). Now reports real matched-cohort drift + honest `prod_only`/`exp_only` coverage gaps.
3. **Verdict classifies on `mean_rel`**, not `max_rel` — one near-zero-crossing day no longer flags a clean series DIVERGE.

#### Still open
- ~~Prod `stack_age_consumed_5min` = 2× prod daily~~ — **RESOLVED 2026-06-22 (above): no anomaly, a deduped-vs-non-deduped query mismatch.**
- Per-day age_consumed timing reallocation is real and expected; decide whether to surface it.
- Then proceed to Phase 2 (windowed metrics via the balance-dip scan).

#### Phase-1 full metric ledger — every produced metric vs prod (2026-06-22, genesis window 2011-10-01..2014-10-01)

| metric (id) | mean_rel | sum_ratio | verdict | reading |
|---|--:|--:|---|---|
| `stack_total_creation_timestamp`[_delta] (91/90) | ~0.4% | 0.999 | **MATCH** | pooled-τ vs per-cohort drift |
| `stack_mean_creation_timestamp` (92) | **0.03%** | 1.000 | **MATCH** | ratio cancellation (num+denom drift together) |
| `stack_mean_age_days` (93) | 4.2% | 0.960 | minor-drift | tiny `mean_ct` residual amplified by `(date+1)−mean_ct` subtraction |
| `stack_total_age` (164) | 4.4% | 0.961 | minor-drift | inherits 93 |
| `stack_liveliness` (165) | 4.8% | 1.038 | minor-drift | inherits 164 |
| `stack_cumulative_age_consumed` (163) | 4.8% | 1.02 | minor-drift | flow integral, converges |
| `stack_total_creation_squared_timestamp`[_delta] (174/173) | ~0.5% | 0.999 | minor-drift | τ² scale |
| `total_supply_from_balances`[_delta] (10002/10001) | 0.46% vs `circ_20y`/`total_supply` (619) | 1.000 | **MATCH** | new balance supply; prod `total_supply`≡`circ_20y` for LTC (no `custom`) |
| `age_distribution_5min_delta` / `_1day_delta` (162/270) | per-cohort high | 1.0 / 0.999 | **structural** | exp has 1 pooled-τ cohort/dt vs prod's many odt cohorts — per-cohort join can't match by construction; **daily totals conserve** |
| `stack_age_consumed` daily (8) / `_5min` (95) | 9–30%/yr per-day | 1.016 | **DIVERGE (per-day)** | inherent LIFO-youngest-first vs pooled-avg-τ timing reallocation; **integral conserved**; per-day error shrinks with activity density (2011 30% → 2014 9%) |

**Composite re-root applied:** `stack_mean_creation_timestamp` (92) and `stack_total_age` (164) now divide by `total_supply_from_balances` instead of `stack_circulation_20y` (`mean_age.yaml`). **LTC-only — `total_supply_from_balances` exists for LTC alone; gate per-chain (or wait until all chains have a balance supply) before any prod merge.** Run via adding `ltc-composite-metrics` to `DAILY_JOBS`; the composite job reads its inputs from the already-populated `_experimental` daily table and filters `NaN`/non-finite, so windowed composites with missing Phase-2 deps drop out cleanly.

**Bottom line:** every produced metric is **MATCH or minor-drift EXCEPT** (a) `stack_age_consumed` daily/5min per-day (inherent timing reallocation — conserved in the integral, cannot be per-day minor-drift on a single-τ tank), and (b) the `age_distribution_*_delta` emitter rows at the **cohort** level (structural by design — the migration's whole point is one pooled-τ cohort/dt; daily totals conserve). Both are expected, not bugs; both are the model boundary, not fixable without per-cohort age.

#### Full-history results — all LTC history 2011-10-01 .. 2026-06-21 (2026-06-22)

Backfill completed for the **entire** LTC history (deltas continued from 2014-10-01; cumsums/composites from genesis). Daily metrics: 5,378 days; emitter `age_distribution_5min_delta` 323M rows; intraday `stack_age_consumed_5min` 1.55M rows. **Two operational fixes were required for the wide range** (both in this branch): `max_partitions_per_insert_block: 0` on the CH client (`context.py`) — a full-range single-block INSERT spans >100 monthly partitions on `intraday_metrics`/`distribution_deltas` (`toYYYYMM(dt)`) and trips the default-100 guard (daily table is unpartitioned, so daily was unaffected); and the run was split delta-jobs-from-2014 / cumsum+composite-from-genesis. **Decision: keep experimental partitioning identical to prod** (representative validation; can't `ALTER PARTITION BY` in place anyway).

Full-history verdicts (symmetric mean_rel, deduped):

| metric (id) | mean_rel | sum_ratio | verdict |
|---|--:|--:|---|
| `stack_total_creation_timestamp` (91) | 0.18% | 1.001 | **MATCH** |
| `stack_mean_creation_timestamp` (92) | 0.085% | 1.001 | **MATCH** |
| `stack_total_creation_squared_timestamp` (174) | 0.27% | 1.002 | **MATCH** |
| `total_supply_from_balances` (10002) vs `circ_20y` (20) | 0.094% | 1.000 | **MATCH** |
| `stack_cumulative_age_consumed` (163) | 2.0% | 1.013 | minor-drift |
| `stack_liveliness` (165) | 2.0% | 1.017 | minor-drift |
| `stack_mean_age_days` (93) | 3.2% | 0.968 | minor-drift |
| `stack_total_age` (164) | 3.3% | 0.968 | minor-drift |
| creation-ts / squared deltas (90/173) | 1.6% / 2.5% | ~1.00 | minor-drift |
| `stack_age_consumed` daily (8) / 5min (95) | **12.5% / 30.6% per-day** | 1.018 | **DIVERGE (per-day)** |

Over the full series the all-time/cumulative family is **tighter** than on the genesis window alone (91/92/174/supply MATCH; 163/165/93/164 ≤3.3%) — early-era noise is diluted.

**CORRECTION to the genesis-window hypothesis (do not propagate):** the genesis breakdown suggested age_consumed per-day divergence "shrinks with activity density (30%→9%)." **The full history disproves that** — per-year per-day mean_rel settles into a **persistent ~8–18% band** (lowest 7.5% in 2019–2021, rising back to 15–18% in 2024–2026), never converging. The daily/5min `stack_age_consumed` is a genuine, irreducible per-day reallocation (LIFO-youngest-first vs pooled-avg-τ) that is conserved only in the **integral** (cumulative 163 = 2%, sum_ratio 1.018), not day-to-day. It is the model boundary of a single-τ tank and cannot be made per-day minor-drift without per-cohort age. **Open decision:** accept the daily/5min metric as a documented conserved-integral shift (its cumulative/liveliness forms are minor-drift), or hold it as a blocker.

#### age_consumed divergence — directional attribution (2026-06-22)

**Direction confirms the LIFO-vs-tank model.** Per-dt, **exp > prod on 79.4% of days** (signed mean **+7.9%**, positive in *every* year 2011–2026). Prod's LIFO spends youngest-first (small `dt−odt` → low age); the tank spends at the pooled-average `τ` (older → higher age). So the tank systematically charges *more* age per spend — exactly why `sum_ratio` = 1.018. The divergence is a **directional, model-explained bias, not noise.**

**The ~20% reversion days (exp < prod) are an AGE effect, NOT netting.** Decomposed the largest-gap reversion days from metric 162 into gross outflow **volume** × **avg age** (prod vs exp):

| day | out_vol prod→exp | avg age prod→exp |
|---|---|---|
| 2017-02-24 | 3.863M → 3.869M (+0.2%) | 75.3d → 37.5d |
| 2022-07-20 | 33.66M → 33.72M (+0.2%) | 37.9d → 20.3d |
| 2024-11-02 | 62.56M → 62.58M (+0.04%) | 55.1d → 45.2d |
| 2025-12-11 | 215.2M → 215.2M (+0.01%) | 3.6d → 2.0d |

**Gross outflow volume is identical (≤0.3%) on every reversion day → netting drives essentially none of it.** The entire gap is **average age** (tank younger). **CORRECTION to the earlier framing** ("balances nets sub-block transfers"): netting does *not* reduce daily volume — it reallocates **age**. The tank averages an in-block *receive-then-send* into a single younger `τ` (the receipt pulls the birth timestamp forward), while prod's stacks preserve the actual older cohort LIFO consumed. This also resolves why exp can fall *below* prod despite "pooled-τ ≥ LIFO-youngest": that inequality assumes identical coin ages, but the tank's `τ` is an upstream approximation that in-block churn pulls younger than the true cohort age.

**Root mechanism of reversion days — confirmed by age-band + cumulative analysis.** On a reversion day, prod and exp consume the **same volume**, but prod books it in an **older age band**: e.g. 2022-07-20 prod consumes 910K coins in the **2–5y** band (avg 1040d) that in exp appear as **1–2y** (1.49M coins, avg 450d, with only 490 left in 2–5y); 2017-02-24 prod has 379K coins at **1–2y** (570d) that exp places at **90–365d** (1.23M coins, 108d). The cumulative series (metric 163) around 2022-07-20 shows the timing directly: on **07-18/07-19 exp daily > prod** (the tank amortizes the old-cohort age *early*), pushing the cumulative gap (exp−prod) up to +2165M; then on the **07-20 dip prod spikes 1274M vs exp 684M**, collapsing the gap to +1575M (prod catches up ~590M in one day); afterward the gap is stable. **Interpretation:** prod's LIFO preserves an old cohort untouched and books its entire age in a single "dip" day; the tank's moving-average `τ` smears that same old age across the preceding days. Pure **timing reallocation of old-cohort age** — integral conserved (exp cumulative runs ~1.013–1.018× ahead, consistent with front-loading), only *when* it's booked differs. This is the definitive characterization of the daily/5min `stack_age_consumed` divergence.

---

## Phase 1 — cumsum-safe metrics (deltas + plain cumulative sums)

### 1.1 Metrics in scope

| Group | Metrics | Tank form / why safe |
|---|---|---|
| **Age consumed (flow)** | `stack_age_consumed`, `stack_age_consumed_5min`, `stack_cumulative_age_consumed` | `Σ (dt−τ)·outflow`; no window, no cancel. Cumulative integral is policy-conserved (`∫B dt`) |
| **Creation timestamp (all-time)** | `stack_total_creation_timestamp` (+ delta), `stack_mean_creation_timestamp` | `Σ B·τ`, plain cumsum |
| **Age (all-time)** | `stack_total_age`, `stack_mean_age_days` | `Σ B·(dt−τ)`; `dt − mean_creation` |
| **Liveliness** | `stack_liveliness` | `cum_age_consumed / (cum_age_consumed + total_age)` |
| **All-held supply** | **`stack_circulation_100y`** (NEW — replaces the `circ_20y` supply role) | cancels land at `odt+100y`, beyond the data horizon → never fire in-range → plain cumsum yields `Σ balances` permanently (safe to ~2109) |
| **Supply re-root** | `total_supply` (→ `circ_100y`), and its riders `daily_marketcap_usd`, `fully_diluted_valuation_usd`, dominance, `nvt_transaction_volume`, `stock_to_flow_ratio`, `annual_inflation_rate`, `percent_of_total_supply_on_exchanges`, `non_exchange_token_supply` | arithmetic over `circ_100y` / price; ride for free once `circ_100y` and `total_supply` are set |

> **Why `circ_100y`, not `circ_20y`:** `circ_20y`'s cumsum-safety is a function of today's date, not structure. BTC data reaches 20 years ≈ **2029**, at which point the `odt+20y` cancels fire in-range and the `τ`-break resurfaces; separately, coins dormant past 20y drop out of a 20y window, so `circ_20y` would also begin **undercounting** total supply (a latent bug in the prod `coalesce(custom, circ_20y)` fallback too). A 100y window is effectively all-time and permanently cancel-free. Today `circ_100y ≡ circ_20y ≡ Σ balances`.

Not in Phase 1 (later phases): windowed circulation `1d…365d`, realized-cap / `mean_realized_price` / `mvrv` (all windows), windowed mean-age, `dormant_circulation`, `nvt`/`token_velocity` (need `circ_1d`), `stack_price_consumed` (Jensen-biased), hodl-waves, spent-bands, std-dev, NPL.

### 1.2 Implementation

Routing is **per-metric, in job specs** (`specs.d/dags/intraday-metrics-ltc.yaml` + the LTC cronjobs), via `stacksFunction: ltc_stacks.LTCStacks` / `distributionFunction: distribution_deltas.LTCDistribution`. It is a code/spec change, not a config flag.

1. Add an LTC tank delta-emitter job function that reads `ltc_balances` and emits the age-balances `(dt, odt=τ, amount, sign)` shape (the existing `age_balances_base` emission; UTXO base). Build on / alongside the existing `ltc_balances.py`.
2. Point the LTC `age_distribution` emitter (which feeds `age_distribution_5min_delta` → `distribution_deltas`) at the tank emitter instead of `ltc_stacks.LTCStacks`.
3. Register `stack_circulation_100y` (window = 100y) in `circulation_metrics.yaml`; re-root the `total_supply` composite (`total_supply_metrics.yaml`) and the all-time mean-age/total-age denominators on it.
4. Run **only the Phase-1 (non-cancel) jobs** for LTC in the experimental run — exclude the windowed-cancel jobs (they would emit broken pulses on tank deltas; they belong to Phase 2).

### 1.3 Test configuration (experimental tables already created in CH prod)

Point every **written** table to its `_experimental` variant (env vars, no auto-suffix); source `ltc_balances` stays prod:

| Table | Env var | Experimental value |
|---|---|---|
| daily output | `DAILY_DAILY_METRICS_TABLE` | `daily_metrics_v2_experimental` |
| intraday output | `DAILY_INTRADAY_METRICS_TABLE` | `intraday_metrics_experimental` |
| age-distribution / deltas | `DAILY_DISTRIBUTION_DELTAS_TABLE` | `distribution_deltas_5min_experimental` |
| futures | `DAILY_DELTA_FUTURES_TABLE` | `daily_delta_futures_experimental` |

### 1.4 Run (LTC only, Phase-1 jobs)

```bash
set -a && . ./.env.dev && set +a && cd daily_metrics && ../.venv/bin/python main.py
```

### 1.5 Verify — pinpoint anomalies

Diff per `metric_id`, LTC only: daily `daily_metrics_v2` vs `daily_metrics_v2_experimental`; intraday `intraday_metrics` vs `intraday_metrics_experimental`. Pre-loaded expectations (so analysis separates *expected shift* from *bug*):

| Behaviour | Metrics | Expectation |
|---|---|---|
| **Exact (snapshot state)** | `circ_100y`, `total_supply`, `stack_total_age`, all-time `stack_mean_age_days` / `mean_creation_timestamp` | ≈ exact, modulo small balances-vs-stacks source drift (balances net sub-block transfers; ~0.2–0.6%/day on BTC, LTC TBD) |
| **Conserved integral (flow)** | `stack_cumulative_age_consumed`, `stack_liveliness` | track prod closely |
| **Timing reallocation (flow)** | daily/5min `stack_age_consumed` | bounded day-to-day divergence (tank spends at avg `τ`; LIFO youngest-first), converging in the cumulative |
| **Validation by proxy** | experimental `circ_100y` | ≈ prod `stack_circulation_20y` (equal today) and ≈ `Σ balances` |

Also confirm whether LTC's `total_supply` uses `custom_total_supply` or the `circ_20y` fallback — decides whether the `→ circ_100y` switch changes the published `total_supply` at all.

### 1.6 Analysis

Per migrated `metric_id` (LTC `asset_id` from `asset_metadata`): ratio + absolute diff over the full series; first-divergence date and max-divergence point; classify **{expected shift | real bug | investigate}**; output a short verdict table. *(Concrete query text — asset_id, date range, thresholds — pinned at run time, not now.)*

---

## Phase 2 — windowed metrics via the balance-dip *(sketch)*

Replace the windowing-cancel step with the backwards trough scan (`Σ_address argMax(balance,dt) − min(oldBalance)` over `dt > D−T`, positive diffs), recomputed per day:

- **Circulation** `1d/7d/30d` firm, `60d…365d` as performance permits; `dormant_circulation` (complement); `nvt`, `token_velocity` (consume `circ_1d`).
- **Realized-cap / MRP / MVRV / mean-age**, windows `1d…365d` — the same scan carrying acquisition price/`odt`. All-time realized cap via a new **`RV` cost-basis accumulator** (`Σ RV / Σ B`). *(Correction — see the 2026-06-22 exploration below: `RV` can NOT be maintained in Flink "like `τ`" — pricing is unavailable upstream — so it is a **SQL-side** accumulator in its own per-address table.)*
- **Known methodology shift to expect:** dip circulation ~1.19× @1d, ~1.11× @7d, converging <0.2% by 30d.

### Phase-2 design exploration — realized-cap / price-age family (2026-06-22)

Deep-dive on the hardest case. **The family splits cleanly: all-time is tractable (incremental); every *windowed* variant is rescan-or-drop.** Realized cap = *price-weighted circulation*, so it inherits circulation's machinery and its failure modes.

**RULED OUT** (recorded so they aren't re-explored):

- **Windowed realized cap via the single-`τ` tank emission** (the §4.1 deltas-as-coins, extended to price). Breaks *twice*, both on the outflow side: (a) the cancel-the-cancel timing pulses (the spend's cancel lands at `τ+T`, not the cohort's `t0+T`), and (b) a **`price(τ)` Jensen bias** — the synthetic outflow is valued at the pooled-average-birth price, not the real cohort prices. Exact only for single-cohort receive-and-hold; error ∝ within-address age+price spread.
- **Windowed circulation / realized cap as a cumulative sum** — impossible on the tank. The windowed value rests on the trough `min(oldBalance)`, a **sliding-window min, which is not invertible** (a row leaving the trailing edge can raise the min, with nothing to un-subtract). The *only* cumsum-able tank form is the 2-row self-consistent "cancel-old/install-new" emission — and it **telescopes to the §4.2 whole-balance step function**. So on the tank: cumsum-able ⇒ whole-balance ⇒ wrong. **Trilemma:** of {LIFO-correct, incremental, bounded-state} you get **two**; the migration keeps *correct + bounded* by giving up *incremental* (per-day rescan).
- **Carrying `avgCost` does NOT unlock windowed RV as a cumsum.** Cost is the *value* axis; windowing is a *threshold on the age* axis — orthogonal. Averaging is **exact for value** (realized cap is linear: `Σ aᵢ·pᵢ = B·avgCost`) but **lossy for age-windowing** (threshold-gated: `[avg_age<T] ≠ avg of [ageᵢ<T]` → the step function). One column fixes value; age needs a whole distribution = stacks.
- **All-time `RV` as a plain `arrayCumSum` of pre-known deltas** — no. Realized value is **path/order-dependent** (`buy,buy,sell` → RV 1800 vs `buy,sell,buy` → 2400 for the same events) because the outflow removes at the running `avgCost = RV/B`. A sum commutes; RV doesn't ⇒ a **stateful per-address accumulator** is required. (Contrast `total_supply`: its `−Δ` removal is value-free → genuinely a plain cumsum — why Phase 1 worked.)
- **Maintaining `RV` upstream in Flink** (as the §Phase-2 sketch assumed) — **not possible: pricing is unavailable in Flink, and some tokens lack it.** The cost accumulator must live **SQL-side**, where price resolves from other feeds.
- **A single mutable `(B, RV)`-per-address checkpoint** — unsafe. Read-modify-write shared state; a concurrent **current run + backfill** clobber each other (ClickHouse won't serialize it).
- **No-table "recompute `avgCost` from genesis each run" as the real-time step** — disqualified; re-folding all history per tick is infeasible. Viable only as the **one-time bootstrap**.
- **Reusing `distribution_deltas.acquisition_price` for `RV`** — that is `price(odt)` ≈ `price(τ)` on the tank (Jensen-biased), not cost basis. RV must price outflows at `avgCost`.

**Working approach (current direction, not foreclosed):**

- **Windowed circulation** — trough scalar `max(0, argMax(balance,(dt,blockNumber,txPos)) − min(oldBalance))` GROUP BY address. Two aggregates, **no fold, no price join, no `τ`** — cheap. `min(oldBalance)` (not `min(balance)`) is required: it carries the **window-opening floor** (first in-window row's `oldBalance` = balance as of `D−T`); the `>0` clamp covers "current balance is itself the trough." Dedup versions (`FINAL`) before the `min`.
- **Windowed realized-cap / MRP / mean-age** — the **suffix-min decomposition** as a **window function** (not `arrayFold`): `surviveᵢ = max(0, suffixMinᵢ − oldBalanceᵢ)` per inflow row, `suffixMinᵢ = min(balance) OVER (PARTITION BY address ORDER BY dt ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)`; then `Σ surviveᵢ·priceᵢ` (realized cap, + ASOF price join) / `Σ surviveᵢ·dtᵢ` (mean age). Verified against an explicit LIFO fold (old-floor & overlapping-band cases).
- **Cost** — circulation ≈ free; the priced family adds a window-fn + ASOF join. **Backfill ≈ T× the stacks-cumsum cost** (no per-cohort `odt` ⇒ re-derive survival each ≤T in-window day vs one scheduled aging-out cancel); that T× is the windowing stacks pre-pays in Flink state — the tank pays it once at query time with bounded state. Steady-state daily is cheap (one T-window/day). ⇒ tier 1d/7d/30d firm, 60–365d perf-permitting, multi-year rare-DAG. Lever: pre-collapse the tank to address-day before scanning.
- **All-time realized-cap / MRP / MVRV** — the **`RV` stateful accumulator (average-cost)**: `inflow RV += Δ·price(dt)`; `outflow RV *= balance/oldBalance` (remove at `RV/B`). `realized_cap = Σ RV`, `MRP = Σ RV/Σ B`, `MVRV = price/(Σ RV/Σ B)`. Structurally a **price-weighted twin of `τ`** (shift-on-inflow, preserve-on-outflow). Policy = **avg-cost, NOT LIFO** (LIFO needs the per-cohort stack we're escaping) ⇒ **expect divergence from prod's LIFO stacks on churny price-heterogeneous addresses** (e.g. 1800 vs 900 on one address); likely closer in aggregate (HODLer-dominated) but **not** the ~0.2% circulation match — must validate side-by-side.
- **`RV` via the existing delta+cumsum mechanics** — works for **all-time** (no aging-out ⇒ no futures/cancels), exactly like Phase-1 `total_supply_from_balances`, **iff the outflow cost travels as data**: daily delta `= Σ_addr [ +Δ·price(dt) (in) ; −|Δ|·avgCost_before (out) ]` → plain cumsum (≡ the snapshot `Σ balance·avgAcqPrice`).

**Storage + concurrency — the realized-value layer (derived requirements, not free choices):**

- **A dedicated per-address realized-value table is required.** No existing table fits (`daily_metrics_v2` is per-(asset,day); `distribution_deltas_5min` is per-(asset,odt) with biased price; `ltc_balances` is Flink-owned and can't price). It is the SQL-side, price-aware companion to the on-chain balances tank and the **shared foundation for the whole realized-price family across every stacks-deprecating chain** — not a single-metric crutch. **Bounded** (one cost scalar per address), **not** per-cohort ⇒ does not reintroduce stacks state-growth.
- **Store it as an immutable append-only `(address, dt)` series — NOT a mutable cell.** Safety = **determinism + idempotency + cursor ordering**, *not* single-writer: each row is a deterministic function of immutable source, written idempotently (`ReplacingMergeTree`); concurrent current+backfill write **disjoint `(address, dt)` rows**; the **`sequenced_job` cursor** serializes the frontier so a tick never seeds from a row another writer is still producing. Same in-practice safety as the existing windowed cumsum (which also seeds from prior stored state).
- **Real-time forces this table.** Bootstrap = the one-time genesis fold (builds the series). Steady-state tick is cheap and mirrors delta+cumsum: `realized_cap(D+1) = realized_cap(D) + Σ_{addresses active in (D,D+1]} (RV_new − RV_old)` — `RV_old` from the stored series tail, `RV_new` from folding only the increment ⇒ touches only active addresses, O(tick activity).

**The "drop all windows" simplification (live option).** If the windowed term structure isn't worth the T× backfill, Phase 2 collapses to **essentially just the `RV` accumulator** (+ optionally a trivial `circ_1d`, T=1 ≈ 1× cost, to keep `nvt`/`token_velocity`). **Survives:** `total_supply` + riders (Phase 1), all-time age family (Phase 1, done), all-time `realized_cap`/`MRP`/headline `MVRV` (new `RV`). **Deprecate:** all windowed `circulation` / `realized_cap` / `mean_realized_price` / `mvrv` / `mean_age` / `dormant_circulation` / `circulation_usd`, `mvrv_usd_long_short_diff`, windowed `stakers_*`. **Casualty needing a call:** `nvt` / `token_velocity` (need `circ_1d`). This is a **product decision** — windowed MVRV/MRP/circulation are live API metrics; the all-time/headline forms survive.

---

## Phase 3 (last) — multi-year windows + retire `ltc_stacks` *(to be clarified when reached)*

The multi-year windows **2y / 5y / 10y** (and `3y` — *open, see below*) and their dependents (circulation, realized-cap/MRP/MVRV, windowed mean-age, dormant, hodl-wave bands, stakers-if-applicable) are **not dropped**. Because the dip scan over multi-year windows is expensive, they are computed by a **separate Airflow DAG invoked very rarely** (low cadence, performance-bounded). The `20y` family is **not** here — it is renamed to `100y` (Phase 1 for supply; the all-time realized/MVRV forms in Phase 2). This phase also performs the **final retirement of `ltc_stacks`** once every consumer is re-sourced. Detailed design deferred to this phase.

---

## Open questions

1. **`3y`** — grouped with 2y/5y/10y in the rare-DAG band by default; confirm (you listed 2y/5y/10y/20y, not 3y).
2. **`mvrv_z` / `mctc` / `thermocap` / `nvtv`** — confirm they root on the all-time realized cap (→ re-root to 100y) rather than dropping.
3. **`stakers_*` applicability for LTC** — staking-asset family; confirm LTC produces it at all before listing it as dragged.
4. **Dip performance ceiling** (Phase 2) — how far up the window tier the per-day rescan is affordable before the rare-DAG takes over.
5. **`custom_total_supply` coverage for LTC** (§1.5).
6. **avg-cost vs LIFO divergence (decisive for the `RV` path).** Quantify side-by-side on LTC: all-time `realized_cap` / `MVRV` from the balance-`RV` average-cost accumulator vs prod LIFO stacks. Is the aggregate gap acceptable (HODLer-dominated → maybe), or is the per-address divergence on churny addresses too large?
7. **Keep `circ_1d`, or drop `nvt` / `token_velocity`?** `circ_1d` is ~free (T=1, 1× scan); it's the only windowed survivor needed to keep NVT/velocity under the "drop all windows" path.
8. **`RV` table mechanics.** Bootstrap (one-time genesis fold) + cursor-driven incremental; per-address series size at LTC scale (collapse to address-day?); is the in-practice cursor safety enough, or do we want full genesis-recompute (max-safe, expensive) as a fallback?
9. **Product sign-off on deprecating the windowed term structure** (windowed `mvrv` / `mean_realized_price` / `circulation` / `mean_age` / `dormant`) if the "drop all windows" path is taken — these are live API metrics; the all-time/headline forms survive.
