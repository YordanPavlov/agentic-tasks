# Metrics regression guard — catching non-reproducible history

> A nightly guard that recomputes a small fixture with **HEAD code** and diffs it
> against a committed **baseline**, so a commit that silently changes how a
> HISTORICAL metric is computed is caught the next night — instead of surfacing
> years later during a rare full recompute. Scoped to metrics reproducible from
> **transfers + balances + stacks + prices**.
>
> **Author:** Yordan + Claude · **Started:** 2026-07-07
> **Code:** clickhouse-tables PR [#2274](https://github.com/santiment/clickhouse-tables/pull/2274) + docker-airflow PR [#1714](https://github.com/santiment/docker-airflow/pull/1714), branch `metricsQA` in both.

---

## 0. Why (motivation)

The XRP odt-bucketing validation ([`../2026-06-odt-bucketing-xrp/`](../2026-06-odt-bucketing-xrp/verifying-the-batched-odt-migration.md), §16.3/§16.4) found that our **served historical metrics are not reproducible by current code** — they are "fossils":

- **Methodology change** — the acquisition-price ASOF grid changed `toStartOfHour → toStartOfFiveMinute` (commit `352af466`, 2022-03-09) and later moved from on-the-fly compute to a precomputed seam column. Prod's stored price-family metrics predate this and recompute to different values (`stack_price_consumed` recompute-from-prod-seam = **0.000** vs stored, all years; `stack_age_consumed` = **1.000**, exact — isolating the divergence to the price step).
- **Data-loss bug** — PR #2132's `INNER JOIN` dropped stack cohorts whose acquisition 5-min bucket had no price (pre-price-data cohorts), live ~11.5 months (2025-03-19 → 2026-03-02), unnoticed until a manual recompute. Confirmed: ~35.48M XRP of 2013-cohort pops missing from prod's Apr–May 2025 seam.

The lesson (discussed at length this session): long-lived pipelines drift, and we had **no cheap, continuous detector** for "does today's code still reproduce the history we serve?" Full recomputes are rare, so drift accumulates silently. This task builds that detector.

## 1. Goal

Two products from one mechanism:

1. **Ongoing (the real product):** catch, *the next night*, any commit that changes historical metric computation. HEAD recompute vs a committed baseline → red Airflow task on divergence.
2. **One-time diagnostic (byproduct of seeding a baseline):** measure how much of a chain's served history current code can't reproduce — i.e. **do we have fossils** here, like XRP? (`_guard` HEAD recompute vs served prod.)

## 2. Design

- **Baseline = committed, human-readable signatures.** Per `(metric, year)`: `count`, `sum`, `first`, `last` (deduped `argMax(value, computed_at)`, never `FINAL`). Compact (few numbers/metric/year → KB), sensitive (any real change moves `sum`), float-noise-tolerant (`isclose(rtol, atol)`). Keyed by **metric NAME** (ids resolved from `metric_metadata`/`asset_metadata` at run time) so the committed file is reviewable. This is the `table_qa` pattern extended from raw tables to computed metrics.
- **Recompute into `*_guard` scratch tables**, full raw→seam→metric (so emitter-level bugs like #2132 are in scope). Reads real prod raw+prices; writes only `*_guard`.
- **Write-surface gate** — a dry-run whose SQL is parsed to assert every persistent write targets a `*_guard`/ephemeral table; a missing output redirect (→ a prod mutation) fails the run before any write.
- **Scope by exclusion, not curation** — the recompute set = the chain's jobs from the job graph MINUS out-of-scope source families; the baseline is the single source of *asserted* metrics (a subset of what's recomputed; `check` flags baselined-but-not-produced as `missing`).
- **`master` image on purpose** — the guard must catch regressions in the LATEST code, not the deployed env tag.
- **On a red check:** classify **fix vs redefinition** (a correctness fix → recompute+replace; a definition change → version it), and if intentional, re-record the baseline in a PR — whose diff is the audit trail.

## 3. Scope

Metrics reproducible from **transfers, balances, stacks, prices** only. Excluded (need other sources): social, label-based, NFT, derivatives (bitmex), staking (beacon/consensus), fees/receipts. Encoded as `EXCLUDE_SUBSTR` in the DAG (`social|label|nft|bitmex|exchange|deposit|withdraw|beacon|gas|dex|bridge|funding|perpetual|price-index|staker|fee|transaction-size`). ETH on-chain set = **55 jobs**.

## 4. What was built (components)

clickhouse-tables (PR #2274, branch `metricsQA`):
- `table_qa/metric_baselines.py` — signature compute, name→id resolution, isclose diff, `record`/`check` CLI (+ `table_suffix` so one baseline checks against `*_guard`).
- `table_qa/guard_write_surface.py` — the write-surface safety gate.
- `table_qa/guard_seed_prices.py` — seeds daily price metrics into `daily_metrics_v2_guard` (an input; no in-scope DMF job computes daily prices — loaded externally).
- `table_qa/guard_tables.sql` — the 7 `*_guard` scratch tables (`ON CLUSTER default_cluster`, codec-free, own ZK paths). Operator-run (db/ migrations are deprecated).
- `table_qa/baselines/eth_metrics_baseline.json` — ETH sample baseline.

docker-airflow (PR #1714, branch `metricsQA`):
- `dags/metrics_regression_guard.py` — nightly DAG, per chain: **seed-prices → assert-write-surface → recompute → check**, on `clickhouse-tables:master`. Jobs derived from the job graph minus `EXCLUDE_SUBSTR`. ETH fixture: `ethereum/2019-01-01` (asset_id 1681), recompute window genesis → 2017-01-01 (baseline asserts full year 2016).

## 5. Expected results (predicted from XRP)

- **Pure-age / transfers / balances metrics** (circulation, DAA, tx-volume, dormant, age-consumed, creation-timestamps, network-growth): **reproduce** (deterministic from raw).
- **Price-weighted metrics** (realized-cap, MVRV, MRP, NPL, supply-in-profit, price-consumed, price-derived volatility/rsi): likely show a **fossil gap** if ETH's served history predates the 2022-03 acq-price methodology change.
- The `_guard`-vs-served diff quantifies this split per-metric.

## 6. Current state / achieved (2026-07-07)

- Harness + DAG built, validated, and pushed (both PRs open on `metricsQA`).
- **Write-surface validated:** per-job dry-run of all 55 ETH jobs → 100% writes to the 7 `*_guard` tables. Nothing hits a served table.
- **Operator:** created the 7 `*_guard` tables (`ON CLUSTER`, after dropping legacy CODECs that the current CH rejects); seeded daily prices for ETH (asset 1681, dt < 2018-01-01, 8 metrics).
- **ETH recompute COMPLETE** (rc=0, 55 jobs) — single `main.py` pass into `*_guard`, genesis → 2017-01-01 (reduced from 2018 for nightly infra load; recompute warms from genesis 2015, baseline asserts the clean full year 2016). Produced **119 intraday + 420 daily = 539 on-chain metrics** for asset 1681. Prod-write mechanism = `clickhouse_driver` writable user (same as the XRP daily run), bypassing the readonly wrapper.
- **Baseline = full 539-metric set** (expanded from the initial 11-metric sample — the sample under-reported fossils), recorded from `*_guard` (HEAD-truth), asserting full year 2016. Self-check vs `*_guard` PASSES. See §10 for the fossil-gap result.

## 7. Open items / next steps

1. **On recompute completion:** coverage check (per-metric row counts in `intraday_metrics_guard`/`daily_metrics_v2_guard`, continuous 2015→2017, asset 1681 only).
2. **Re-record the ETH baseline from `*_guard`** (`record … _guard`) → replaces the served-prod seed with HEAD-computed truth (years 2015–2017).
3. **Diff `*_guard` vs served** → the ETH fossil-gap measurement (the §1 diagnostic).
4. **Merge #2274 + #1714 → deploy the nightly DAG** (rebuilds `master` image with the guard code + blessed baseline).
5. **Roll out** to more chains (add a `FIXTURES` entry + record its baseline; BTC = the UTXO stack path).
6. **Long-age tail:** age-threshold metrics (`dormant_Ny`, long age bands) are 0 until coins reach age N, so a short nightly window can't guard them; rolling-window metrics (MVRV_5y etc.) *are* covered in year 1 (they saturate to all-available history). Consider a **weekly deep/full-history run** for the long-age tail — keeps nightly load light.
7. **Window:** currently genesis → 2018-01-01 (~2.5y, chosen to bound nightly infra load; reducible to a shorter window with a one-line fixture change + re-record). ETH volume grows ~6×/yr in this era (6.5M→44M→254M→582M stacks rows), so each added year costs disproportionately more.

## 8. Key design decisions (with rationale)

- **Baseline = committed hardcoded values**, not vs-served or vs-previous-recompute. vs-served is chronically nonzero (fossils) → not a clean pass/fail; vs-previous silently normalizes a bad commit into the baseline. Committed values are a human-blessed fixed point that only moves in a reviewed PR.
- **Name-keyed JSON** — ids are an internal framework concern; names keep the file reviewable.
- **Scope by exclusion** — "which metrics to recompute" is the chain's jobs minus out-of-scope source families (stable, coarse); "which metrics to assert" is the baseline (fine). Avoids maintaining a parallel per-metric list.
- **Write-surface gate is mandatory** — makes an open-ended recompute safe: a missing redirect fails the dry-run before any prod write.
- **Prices in scope** (added after initial transfers/balances/stacks) — fundamental for MVRV/MRP/realized-cap; the daily price bundle is seeded (external source, not DMF-computed).
- **`master` image** — guard the latest code, not the deployed tag.
- **Alerting = red Airflow task** for v1 (Prometheus later).

---

## 9. Session log — 2026-07-07

- Framed the problem (long-lived-pipeline drift; how mature shops handle it: log-as-truth/Kappa, immutable+time-travel table formats, bitemporal, provenance/lineage, shadow-diff-promote, golden/regression fixtures). Reconciled with fast-shipping constraints: pin ≠ freeze (content-address + stamp); provenance is per-run not per-row; detection is CI/sampling, not full recompute.
- Built the baseline harness + write-surface gate + price seeder + guard-table DDL + nightly DAG; opened PRs #2274 and #1714.
- Iterated scope with operator: on-chain only → **+prices**; exclusion list corrected twice (dropped price-derived exclusions when prices came in scope; added staking/fees).
- Validated write surface (per-job dry-run, all 55 ETH jobs → 100% `_guard`).
- Fixed `ON CLUSTER` (3-host cluster, manual create) + moved DDL out of deprecated `db/`; dropped legacy CODECs (Gorilla-on-DateTime rejected by CH 25.3).
- Operator created the 7 guard tables + seeded ETH daily prices (dt < 2018-01-01).
- Window: first ran genesis → 2018-01-01 but it was heavy (2.5y incl. 2017's 254M stacks rows; ~3 jobs/13min) — killed and restarted at genesis → 2017-01-01 (recompute warms from genesis 2015, baseline asserts clean full year 2016). ETH volume grows ~6×/yr (6.5M→44M→254M→582M stacks rows/yr), so each added year costs disproportionately.
- **ETH recompute completed** (rc=0, 55 jobs, 539 metrics into `*_guard`); expanded the baseline from the 11-metric sample to all 539 on-chain metrics; ran the fossil-gap diff (§10).

---

## 10. ETH run + fossil-gap result (2026-07-07)

**Recompute:** `main.py`, 55 on-chain jobs, genesis → 2017-01-01, into `*_guard`. rc=0. Coverage: 119 intraday + 420 daily = **539 metrics** for asset 1681, continuous. Fast for the ~1y window (vs hours for the abandoned 2.5y run) — the concrete nightly-cost data point.

**Baseline:** expanded from the 11-metric sample to the **full 539-metric on-chain set** (all resolve to names in `metric_metadata`; 0 unresolvable). Asserts the **clean full year 2016** — recompute warms from genesis 2015 for cumsum correctness, but 2015 is a partial genesis year whose pre-genesis days get zero-filled by the jobs; asserting 2016 (fully populated both sides) avoids the zero-fill/boundary artifact. Self-check vs `*_guard` PASSES.

**Fossil-gap (HEAD baseline vs SERVED, 2016, 539 metrics):** `sum` gaps tiered —
- **17 real (rel > 1%), ALL price/profit-weighted:** `transaction_volume_profit`/`_loss`/`_ratio` and `network_profit_loss` (+1d/7d/30d changes) — **severe (rel 0.5 → ∞)**, the NPL family (same methodology + identity/regime story as XRP §12); `stack_mean_age_dollar_days_*` (price×age) rel 0.11–0.44; `stack_realized_cap_usd_delta_7d/30d`, `mean_realized_price_usd_7d`, `mvrv_usd_7d` ~1%.
- **81 moderate (1e-4 … 1e-2)** — minor drift.
- **39 float-noise (< 1e-4)** + **~520 metrics reproduce** (all pure-age / transfers / balances, plus short-window price like `mvrv_1d`).

**Conclusion:** ETH's served history is **mostly reproducible** by HEAD code; fossils are **concentrated in the price/profit family** (acquisition-price methodology change + NPL identity/regime) — the same signature as XRP, now confirmed chain-consistent. **Full coverage was essential:** the 11-metric sample hit `realized_cap`/`MVRV`/`MRP` at 1d (which reproduce) and missed the entire NPL family + `_7d`/`_30d`/dollar-days variants (which don't). The guard is validated end-to-end with full 539-metric coverage; baseline committed (`f1189aed`).

**Next:** merge #2274 + #1714 → deploy nightly DAG; optionally drill the `transaction_volume_profit_loss` ∞ divergence to confirm the XRP identity story; roll out to more chains.

---

## 11. Session log — 2026-07-08 — baseline audit before blessing (⚠ BASELINE NOT BLESSABLE AS-IS)

Slowed down before merging to verify the committed baseline values are the ones we want to bless. Two findings; the first **blocks the merge**.

### 11.1 "Two dropped metrics" (539 vs 537) — benign

Journal §6/§10 said 539 (119 intraday + 420 daily); the committed file has **537** (119 + 418). Reconciled against `*_guard`:
- Guard produces (2016): 119 intraday + **426** daily = 545 distinct.
- The 8 daily absent from the baseline = **exactly the `guard_seed_prices.py` seed set** (`daily_{closing,opening,high,low,avg}_price_usd`, `daily_{avg,closing}_marketcap_usd`, `daily_trading_volume_usd`) — seed **inputs**, correctly not asserted (circular). `record_baseline` only records names in its supplied list (`metric_baselines.py:307`), which never included the seeds.
- **Conclusion:** 537 computed metrics, baseline asserts 100% of them. Zero computed metrics dropped. "539" was a miscount → fix to 537.

### 11.2 The profit/loss "∞ fossils" are a GUARD-HARNESS ORDERING ARTIFACT, not fossils — ⚠ BLOCKER

§10 listed `transaction_volume_profit`/`_loss`/`_ratio` as "severe fossils (rel 0.5→∞)". They are **not fossils** — the baseline recorded **spurious zeros** from a job-ordering/seed race:

- `transaction_volume_profit` baseline = **0.0** (all 366 days of 2016); served = 327M. But **re-running the exact profit query now against the guard tables yields ~510k profit for a single day** (nonzero). So the stored 0 is not what HEAD computes from the finished inputs.
- **Timeline (computed_at, guard):** seam `age_distribution_5min_delta` 14:43–14:59 → `transaction_volume_profit` **15:00** → intraday `price_usd` bulk-loaded into `intraday_metrics_guard` at **15:08:12** (single timestamp, exactly 105 408 rows = 366×288, **1 row/dt** vs served's 2/dt → a manual `INSERT…SELECT`, not a job). NPL ran 15:23 (after price → real value).
- **Root cause:** `transaction_volume_profit_loss_job` (and NPL) read `price_usd` from `intraday_metrics` (= `intraday_metrics_guard` under the guard config; job lines 65–75). Intraday `price_usd` for ETH is an **external input** — the `intraday-prices` DMF job is named `intraday-prices` (no chain prefix → not matched by the guard's `startswith("eth-")` filter) **and** its assetSelector explicitly excludes eth/btc/erc20/xrp/… So no in-scope job produces it; it must be **seeded**, like daily prices. But **`guard_seed_prices.py` seeds only DAILY prices, not intraday `price_usd`** → gap. In the ETH run the operator bulk-loaded intraday price manually at 15:08, *after* `main.py` had started (14:59) and already run the profit jobs at 15:00 → their INNER JOIN against an empty price table matched nothing → only the `UNION ALL` zero-fill landed → all-zero.
- **Blast radius:** exactly the 3 metrics that INNER-JOIN intraday `price_usd` **and** ran in the 15:00–15:08 gap: `transaction_volume_profit`, `transaction_volume_loss`, `transaction_volume_profit_loss_ratio`. All other price consumers ran after 15:08 (NPL 15:23, realized_cap/mvrv read the seam not intraday price). `stack_price_consumed` (73.8B) and `whale_transaction_count` (677) ran before 15:08 yet **match served exactly** — they read the seam's stored `acquisition_price`, not intraday price.
- **Why this blocks the merge:** the baseline asserts 0 for these 3 (a known-false value); once intraday price is seeded properly, HEAD produces nonzero → the guard would go **red on its first correct run** (or stay green-but-wrong if the race recurs). Blessing 0 blesses a harness bug.

**Fix (proposed):** extend the seed step to also seed **intraday `price_usd`** into `intraday_metrics_guard` before the recompute — either in `guard_seed_prices.py` or a sibling `guard_seed_intraday_prices.py`, run in the DAG's `seed-prices` step **before** `recompute`. Then re-run the recompute (correct input-presence) and **re-record** the baseline. Also consider an **input-presence gate** (sibling to the write-surface gate): assert every external input a job reads (intraday price) is present for the window before running — the topo sort orders *computed* jobs but cannot order an external input (see §11.3).

### 11.3 Does the framework guard job ordering? YES for computed jobs; NO for external inputs

- The DMF **does** topologically sort: `main.py` → `fetch_jobs` → `job_factory.sort_factories` builds an `igraph` dependency graph from metric `depends_on` and runs jobs in `topological_sorting()` order. So ordering **among jobs the run computes** is guarded — the profit-vs-seam ordering was fine (seam 14:43 < profit 15:00).
- The failure is **outside** the topo sort's reach: intraday `price_usd` is produced by **no in-set job** (external input). Such deps become an **"unspecified-job"** vertex (`job_factory.py` `factory_graph`) — assumed to already exist. Nothing in the guard pipeline gates that these external inputs are present before the run; the write-surface gate checks *outputs*, not *input presence*. That's the missing guard.

### 11.4 The GENUINE fossil (NPL) = the ETH GENESIS/PREMINE cohort — caveat on "bless HEAD-truth"

**What NPL is:** realized network profit/loss. For coins that move in a 5-min bucket, `sum((current_price − acquisition_price)·−amount)` — i.e. amount × (price now − price when the coins were last acquired), summed over movers. Needs each cohort's *acquisition price*, stored in the seam (`distribution_deltas_5min`), filled by a `LEFT ASOF JOIN` of the cohort's acquisitionTime against the intraday price grid (`age_distribution_intraday_job.py:126`).

NPL ran after the price seed (15:23), so its guard value is real HEAD output: guard **559.6M** vs served **281.1M** (2016). Decomposed on the guard seam:
- NPL from `acquisition_price > 0` rows = **281.06M** — *exactly* served NPL.
- NPL from `acquisition_price = 0` rows = **+278.53M** → guard total 559.59M.
- **The `acq_price = 0` rows are ALL the ETH genesis cohort:** every one has acquisitionTime `2015-07-30 15:26:13` (Ethereum genesis; the ~72M presale/premine ETH). ETH had **no market price at genesis** — earliest price in `intraday_metrics_historic_optimization` is `2015-08-07 14:45:00` ($2.83), and there are **0** price rows at/before genesis. So the `LEFT ASOF JOIN` finds no acquisition price for this cohort.
- **Served vs guard for this cohort:** served stores its acquisition price as **NULL** (57 053 rows, acq-year 2015) → `(current−NULL)` = NULL → `sum()` **skips** them → genesis cohort **excluded** from NPL. The guard seam stores **0** (41 037 rows, all genesis) → `(current−0)·−amount` books the cohort's **full 2016 market value as profit** → +278M. (Both flag the *same* pre-price cohort — confirmed: served's genesis-time rows are 100% NULL.)
- **Economic reading:** the fossil is entirely "how do you value profit on premined/genesis coins that were never bought at a market price?" Served = exclude (defensible). HEAD = treat as acquired at $0, so 100% of current value is "profit" (an over-count, ~doubles NPL). **Served (281M) is arguably the more-correct number.**
- **Caveat for the design (§8 "bless HEAD-truth"):** here HEAD-truth is a *worse* number than served — blessing it locks in the genesis-at-$0 over-count.

**RESOLVED — the NULL→0 is a side-effect of the ASOF-join fix, PR #2132 (git archaeology, 2026-07-08):**
- INNER JOIN era (data loss): `84ec2023` "Add acquisition price to distribution deltas" (**2025-03-19**) introduced acquisition_price via **`INNER JOIN`** → genesis cohort (no price) **dropped** (journal §0 window 2025-03-19 → 2026-03-02).
- The ASOF fix = **PR #2132** "Use LEFT ASOF join instead of INNER" (branch `ageDistributionRowsDrop`, author YordanPavlov, commit `d678ad9a`, merged **2026-03-02**) — changed `age_distribution_batches_intraday_job.py` `INNER JOIN` → **`LEFT ASOF JOIN`** (match to closest *previous* price). Sibling `055dd3d7` "Fix age distribution job" (**2026-03-03**) applied the same to the non-batches `age_distribution_intraday_job.py`.
- **Mechanism:** LEFT ASOF recovers cohorts whose acquisition bucket had no *exact* price by matching the closest previous price — but the ETH genesis/premine cohort was acquired **before any price exists at all** (genesis 2015-07-30; first price 2015-08-07), so it stays **unmatched**. `join_use_nulls` is **not set anywhere** in `daily_metrics/` (grep empty) → ClickHouse default `0` → a LEFT/LEFT-ASOF join fills unmatched rows with the **type default (`0` for Float64), not NULL** (fill happens in the join, before storage — the `Nullable` column type is irrelevant). So #2132 correctly fixed the *data loss* but introduced a *valuation error* for the truly-pre-price cohort: it books at acquisition_price 0 → 100% of current value counted as profit → NPL 281M → 559M.
- **This is genuine current-HEAD behavior, NOT a guard artifact** — a clean re-run still yields 559M. So "bless HEAD-truth" would enshrine the over-count. **Correct resolution is a CODE FIX**, not a bless: e.g. `join_use_nulls=1` on that join (unmatched → NULL → excluded, matching served's 281M), or explicitly drop `acquisition_price IS NULL / = 0` genesis rows from the profit sum. Pending: read the FIX PR (055dd3d7) diff + review to confirm whether the 0-fill was a known trade-off; get its PR number from operator.
- **Guard implication:** NPL (and the dollar-days / realized-cap price-weighted family) should be treated as **fix-first, then bless** — do not bless 559M. The guard correctly *surfaced* this as a divergence; that's the guard working as intended (it caught a real, if pre-existing, defect).

**Net:** baseline is **not blessable as-is** (3 spurious-zero metrics from the intraday-price seed gap; NPL-family bless-vs-fix question open). Do NOT merge until intraday price is seeded, the recompute re-run, and the baseline re-recorded + re-reviewed.
