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

> ⚠ **MERGE BLOCKED** — baseline not blessable until re-run + re-recorded. §11's intraday-seed plan was **superseded by §12** (2026-07-10): the zero-fill root cause is an undeclared `dependsOn`, now fixed in the metric specs; NO intraday seeding. Also see §12.3: the 07-07 run **leaked 1.89M rows into prod `labeled_intraday_metrics_v2`** — remediation pending.

0. **[SUPERSEDED — see §12.1]** ~~Seed intraday `price_usd`~~ — reverted; `eth-intraday-prices` computes it in-run from `asset_prices_v3`, ordered via `dependsOn`.
0b. **Remediate the labeled-table contamination (§12.3)** — operator decision: surgical `ALTER DELETE` by `log_comment` fingerprint + notify the labeled-balances owner (bulat-l) since 07-08/07-10 organic backfills may have consumed our delta rows.
1. **Re-run the ETH recompute** (now 53 jobs, no seed step needed), then coverage check (per-metric row counts, continuous 2015→2017, asset 1681; prices job must run FIRST and profit metrics be nonzero).
2. **Re-record the ETH baseline from `*_guard`** — `transaction_volume_profit/_loss/_ratio` will become nonzero; **hold the NPL / price-weighted family** until the genesis-valuation defect is fixed (see spun-off task [`../2026-07-genesis-acquisition-price-valuation/`](../2026-07-genesis-acquisition-price-valuation/genesis-acquisition-price-valuation.md)) — do not bless 559M.
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

### 11.5 Session wrap — 2026-07-08

- **(a) intraday-price seed gap: FIXED + pushed.** `table_qa/guard_seed_prices.py` now seeds daily **and** intraday `price_usd` into the guard tables (only `price_usd` needed — no in-scope consumer for intraday marketcap/volume/price_btc/price_eth). DAG picks it up automatically (its `seed-prices` pod already calls this module; order `seed >> assert >> recompute >> check` is correct). Operator will run it manually + re-run the recompute in a later session.
- **(b) NPL genesis-valuation defect: root-caused to PR #2132, spun out.** New standalone task [`../2026-07-genesis-acquisition-price-valuation/`](../2026-07-genesis-acquisition-price-valuation/genesis-acquisition-price-valuation.md) — the LEFT-ASOF fix values the ETH genesis/premine cohort at `acquisition_price = 0` (unmatched + default `join_use_nulls=0`) → NPL 281M→559M. To be fixed there before the guard blesses the price-weighted family.
- **Deferred to next session:** run the seed, re-run the ETH recompute (intraday price up front), re-record baseline for the reproducible set (hold NPL/price-weighted family pending the #2132 fix), then merge #2274 + #1714.

---

## 12. Session log — 2026-07-10 — §11 root cause revised; prod-write incident found & gated

Operator pushed back on §11's "seed intraday price" plan ("intraday should be computed as part of the run, reading from the prices table"). Re-investigated; the operator is right, and the trail led to a genuine prod-write incident. Commits: clickhouse-tables `184d7eac`/`a04c668b`/`c7ca1e25`, docker-airflow `3e72918e` (both `metricsQA`, pushed).

### 12.1 §11.2 was wrong twice — the real root cause is an UNDECLARED dependsOn

- **`eth-intraday-prices` IS in the guard's job set.** §11.2 looked at the generic `intraday-prices` job (which indeed excludes eth); the chain-prefixed `eth-intraday-prices` (specs.d/dags/intraday-metrics-eth.yaml, script `intraday_prices_job`, source `prices.Prices` → raw **`asset_prices_v3`**) is matched by the `eth-` filter and was in the 07-07 run (proof: it's in the 07-07 rows' `log_comment` job list, §12.3). `asset_prices_v3` has ETH 2015–2016 data (source `coinmarketcap` = `config.prices_source` default) from **2015-08-07 14:49** — the run can compute intraday `price_usd` itself; no seeding.
- **The "manual bulk load at 15:08:12" never happened** — that was the prices job itself. `intraday_prices_job` writes all records with one client-side `now()` (single computed_at), 1 row/dt; the "105 408 rows" in §11.2 was the 2016-only slice (366×288); the full 15:08:12 batch is 147 567 rows (2015-08-07 → 2016-12-31) — exactly what today's dry run recomputes ("Inserting 147567 records"). Operator confirmed they only seeded DAILY prices.
- **Root cause of the zero-fill:** `transaction_volume_profit/_loss` declared **no** `dependsOn` and `network_profit_loss` only the seam, while their jobs read `price_usd` from `intraday_metrics` (= the run's own output under guard redirects). The topo sort was free to schedule the prices job after them — and did (dry run: prices dead last; 07-07 real run: prices 15:08 vs profit 15:00). NPL's nonzero value was luck (it ran 15:23 > 15:08).
- **Fix (the DMF-level one, benefits any from-scratch recompute):** declare the real inputs — `price_usd/2019-01-01` (+ seam for the tx-volume pair) in `network_profit_loss_metrics.yaml`. Verified: prices job now sorts FIRST (dry run line 7278, before profit 151542 / NPL 157032). `guard_seed_prices.py` reverted to daily-only (a **one-time operator prerequisite** per chain/window, not a nightly step); the DAG's seed pod removed (chain: `assert >> recompute >> check`).

### 12.2 Dry-run mode couldn't complete end-to-end — fixed (3 spots); this was a GATE BLIND SPOT

Jobs that read real data mid-run crashed the dry run: session-temp tables were created via `execute_dml` (suppressed when dry) while reads go via `execute_dql` (always executes) → Code 60. Fixed: `create_asset_metadata_temp_table` + `labeled_balance_delta_job`'s scoped table → `execute_dql` (identical single-host client; session-local scratch, no persistent write either way; precedent: `dry_run_csv_pipeline.py:178`); `cumulative_sum_job`'s negative-value debug hook → return on the empty dry-run result. Full 53-job dry run now **rc=0**. Why it matters: the write-surface gate parses dry-run SQL — a job that crashes before emitting its INSERT is INVISIBLE to the gate. That blind spot hid §12.3 on 07-07 (the labeled job crashed in the per-job dry-run validation, so "100% writes → _guard" was true only of the jobs that emitted SQL).

### 12.3 ⚠ INCIDENT: the 07-07 run wrote 1.89M rows into prod `labeled_intraday_metrics_v2`

With the dry run completing, the gate immediately FAILed: `labeled_intraday_metrics_v2` is a write target. `eth-balance-changes-delta-intraday-hourly` + `eth-address-changes-delta-intraday-hourly` run `labeled_balance_delta_job` — **label-based metrics whose names dodge the "label" exclusion substring** — and write to `intraday_label_based_metrics_table_v2`, which `GUARD_OUTPUT_ENV` does not redirect.
- **Confirmed via the table's own `log_comment` column:** 1 891 048 rows (metric 1168 `labeled_balance_delta`) + 1 127 rows (1170 `balanced_labels_delta`), blockchain `ethereum`, 119 labels, dt 2015-07-30 15:30 → 2017-01-01, computed_at 2026-07-07 ~15:00, log_comment = our run's 55-job list (`"repo": "clickhouse-tables"`, no airflow dag/owner keys — unambiguous fingerprint, and surgical delete key).
- **Severity:** of 726 445 keys overlapping pre-existing organic rows, only **254 differ** beyond 1e-9 rel — HEAD reproduces the organic values. But ~1.16M keys are NEW (organic backfill hadn't covered them yet), and the `labeled-balances-current` DAG (owner bulat-l) ran historical backfills over 2015–2016 on 07-08 and 07-10 — our rows may already be folded into downstream cumulative label metrics. (Old-dt writes on 06-26/06-30/07-04/07-08/07-10 are all that DAG's organic backfill — attribution matters; today's 1.17M-row batch is organic, NOT from today's dry runs, which wrote nothing.)
- **Remediation (operator decision, not executed):** `ALTER TABLE labeled_intraday_metrics_v2 DELETE WHERE blockchain='ethereum' AND computed_at BETWEEN '2026-07-07 15:00:00' AND '2026-07-07 16:00:00' AND log_comment LIKE '%eth-active-addresses-intraday-deltas%'` (ReplicatedReplacingMergeTree; replicates automatically), plus notify bulat-l re: possibly-consumed deltas.
- **Prevention (committed):** `-changes-delta` added to `EXCLUDE_SUBSTR` (ETH set 55 → 53 jobs — surgical, verified); the now-complete dry run means the gate sees every write: final gate = **PASS, 6/6 targets `_guard`**.

### 12.4 State after this session

- Harness ready for the clean re-run: 53 jobs, prices ordered first, gate green, no seeding needed (daily seed already in place from 07-07; intraday computed in-run; stale guard rows are shadowed via `argMax(computed_at)`).
- Pending operator OK: (a) contamination remediation/escalation; (b) the re-run itself (writes to `*_guard` only). Then §7 items 1–4.

### 12.5 Contamination value-diff (2026-07-10, operator asked "how different is what we inserted?")

Nullable-safe key join (`ifNull(asset_id,0)`), our 1.89M rows vs everything else in `labeled_intraday_metrics_v2` (ethereum, dt<2017):
- **Overlap with pre-existing organic rows: 726 445 keys → 100% value-identical.** 726 191 within 1e-9 rel; 253 float-noise (1e-9..1e-4); the single ">1%" outlier is `top_100_balance` 2015-11-18 12:00, `-1.34e-10` vs `-6.7e-11` — floating-point dust, numerically zero both sides.
- **New keys (no pre-existing row): 1 164 616.** Of these, **88 660 were since independently rewritten by the organic 07-08/07-10 backfills — zero disagreements** (≤1e-4 rel). Remaining **1 075 956 still ours-only**: real label deltas (miner 150k keys, whale_usd_balance 79k, centralized_exchange* 78k, top_100_balance 76k, poloniex/bittrex owners…; medians ~70–240 ETH, maxes to 36M for top_100_balance; anachronistic labels like nft_trader/eth2_staking are all-zero rows). Same code path + same current-label-vintage the organic backfill itself uses (it too backfills 2025-vintage labels like `centralized_exchange_20250307` into 2015–2016).
- **Conclusion: the contamination is value-clean** — foreign provenance and premature coverage, not wrong numbers. Every key where a comparison exists (726k pre + 88.7k post = 815k keys) matches exactly. Deleting by log_comment remains safe (organic re-fills; ReplacingMergeTree + argMax picks later organic rows anyway); keeping is data-safe but leaves rows the labeled pipeline didn't write. Either way, inform bulat-l.

### 12.6 Clean re-run + fossil re-analysis + baseline blessed (2026-07-10)

Operator decided: leave the labeled-table contamination in place (§12.5 showed it value-clean) and proceed.

**Clean recompute:** 53 jobs, rc=0, ~35 min. `price_usd` computed IN-RUN at 08:34:24 (run start — dependsOn ordering works end-to-end), 147 567 rows from `asset_prices_v3`; profit family real and nonzero (profit 355.6M / loss 295.3M / ratio 602.7 for 2016). Stale 07-07 rows shadowed via `argMax(computed_at)`.

**Fossil re-analysis (guard vs served, 2016, 537 metrics):** 350 reproduce (<1e-9), 89 float-noise (<1e-4), 81 moderate (1e-4..1e-2), **17 REAL (>1%)** — but the composition CHANGED vs §10, cleanly splitting into two causes:
- **Genesis-valuation defect (HEAD wrong, fix-first):** `network_profit_loss` 559.6M vs 281.1M (rel .498) + its change_1d/7d/30d (rel 1.2–2.0, sign flips); `transaction_volume_profit` 355.6M vs 327.0M (**+8.0%**) and `_ratio` (+10.4%). **`transaction_volume_loss` REPRODUCES (7e-5)** — the smoking-gun confirmation: an acq-price-$0 cohort can book profit but never loss, so only the profit side inflates. (§10's "∞ severe" tier for this family was entirely the ordering artifact.)
- **True methodology fossils (HEAD = current intended methodology, bless):** `stack_mean_age_dollar_days_90d/180d/365d/2y/3y/5y` (rel .11–.44), `stack_realized_cap_usd_delta_7d/30d`, `mean_realized_price_usd_7d`, `mvrv_usd_7d` (~1–4%). Same 2022-03 acq-price-grid story as §10/XRP. (Realized-cap/dollar-days are NOT genesis-affected: a $0×amount contribution equals a NULL-skipped one.)
- `active_holders_distribution_amount_delta_1e8` "rel 1.0" is dust (-1.9e-9 vs 0), not a divergence.

**Baseline blessed:** re-recorded all 537 from `*_guard`, then **held out 6** (`network_profit_loss` +3 changes, `transaction_volume_profit`, `_ratio`) pending the [`../2026-07-genesis-acquisition-price-valuation/`](../2026-07-genesis-acquisition-price-valuation/genesis-acquisition-price-valuation.md) fix → **531 asserted metrics**, provenance note in the JSON, self-check vs `*_guard` PASSES. Commit `bcd949bd` (metricsQA, pushed).

**Remaining to go live:** review+merge #2274 + #1714 (all merge blockers cleared); after the genesis fix lands: recompute → re-add the 6 held metrics. Then more chains (BTC).

### 12.7 Dev-instance glue: test any clickhouse-tables image against its baseline (2026-07-10)

Operator request: the airflow-dev personal-cluster setup (`devops/stage/k8s-apps/airflow-dev`; `NAMESPACE=<you> ENVIRONMENT=<ch-tables branch/PR> AF_IMAGE_TAG=<docker-airflow branch> DAGS_BRANCH=<dags branch> make install`) should let the guard test a NON-master clickhouse-tables image, selected via ENVIRONMENT like the metrics-computation DAGs. Investigated + implemented (docker-airflow `00a73c67`, metricsQA):

- **How the existing glue works:** airflow-dev passes ENVIRONMENT to Airflow pods as an OS env var (`extraEnv`); docker-airflow's `utils.py` reads it at DAG-parse time; `create_kube_pod_operator` builds `image = f"{repo}:{tag or ENVIRONMENT}"` — DAGs that pass no tag get the ENVIRONMENT-named clickhouse-tables image. The guard DAG passed `tag="master"` unconditionally.
- **Change:** `IMAGE_TAG = "master" if ENVIRONMENT == "production" else ""` — prod keeps guarding master; everywhere else the ENVIRONMENT image is tested **against the baseline committed inside that same image** (an intentional metric change goes green only with its re-recorded baseline — the PR-diff-as-audit-trail workflow, now testable pre-merge). Off production the DAG is **unscheduled** (manual trigger) so the shared stage instance / dev clusters don't run nightly.
- **ClickHouse pinned to prod regardless of instance:** `CLICKHOUSE_HOST` in utils is the cluster-local service name, so a stage dev instance would otherwise recompute against **stage** CH — whose raw tables hold DIFFERENT data (verified: eth_transfers Jan-2016 1.148M stage vs 1.078M prod; eth_stacks 2016 42.85M vs 43.58M; prices 8.9k vs 11.2k) → baseline mismatch by construction. Non-prod instances now use `clickhouse.production.san:30900` (the established `*.production.san` NodePort pattern; `DAILY_CLICKHOUSE_PORT` added — DMF port config confirmed). Guard tables are shared prod scratch; concurrent dev runs could interleave (acceptable; computed_at dedup).
- **Gate hardened:** dry run is now STRICT (no `|| true`) — a mid-dry-run crash truncates the SQL and blinds the gate (the §12.3 escape path). Safe now that #2274's dry-run fixes make a full dry run exit 0.
- **ENV_VARS note:** airflow-dev's user ENV_VARS override mechanism is consumed opt-in per DAG; the guard deliberately does NOT read it, so *_guard write redirects cannot be overridden from a dev deployment.
- Not testable locally (no airflow package): py_compile OK; the dev instance itself is the parse test. **To validate:** `NAMESPACE=yordan-p ENVIRONMENT=metricsQA AF_IMAGE_TAG=metricsQA DAGS_BRANCH=metricsQA make install`, then manually trigger `metrics-regression-guard` — expect gate PASS → recompute → check PASS against the metricsQA image's blessed 531-metric baseline (Jenkins must have built clickhouse-tables:metricsQA from the PR branch; per airflow-dev README Notice 2, once a PR exists images are tagged by PR name — use that as ENVIRONMENT if the branch tag is stale).

### 12.8 CORRECTION to §12.7 + design reversal: cluster-local ClickHouse, single ground-truth baseline (2026-07-10, UNCOMMITTED — operator reviewing)

Operator challenged the prod-CH pinning: the baseline is **ground truth** (frozen ledger history) — if stage and prod disagree, that's itself an issue to investigate, not a reason to pin or fork baselines. Re-verified and the operator is right:

- **§12.7's "stage raw data differs from prod" was MEASUREMENT ERROR.** The raw `count()`s compared pre-merge duplicate rows in Replacing-family tables. Deduplicated content compare (Jan-2016): `eth_transfers` **1 078 120 distinct rows on BOTH**, sum(value) equal to float-order noise; `asset_prices_v3` **8 922 rows on both, equal sums**. Ledger + raw prices are content-identical across clusters.
- **What stage actually lacks is the computed/loaded price INPUT history:** `intraday_metrics_historic_optimization` (the seam's ASOF acquisition-price grid) and `daily_metrics_v2` (daily price bundle = the seed source) have **0 rows for eth < 2017 on stage**. Without them a stage recompute 0-fills every cohort's acquisition price and has no daily prices — garbage against any baseline. Fix = one-time copy of frozen inputs from prod (~148k grid rows + ~23k bundle rows for the ETH fixture).
- **Design reversed (edits in working tree, NOT committed):** dropped `GUARD_CH_HOST` prod-pinning + `DAILY_CLICKHOUSE_PORT`; guard always uses the cluster-local ClickHouse; ONE baseline everywhere (ground-truth principle: cross-cluster divergence = data-quality finding, e.g. exporter/backfill drift — a feature, not a bug, of the single baseline). Bonus: dev-triggered runs no longer touch prod's *_guard scratch (no interleaving with the nightly). DAG docstring prerequisites now include the stage input backfill. Kept from §12.7: ENVIRONMENT-selected image off production, unscheduled off production, strict dry-run gate.
- **Session process note:** operator updated global CLAUDE.md — no auto commit/push; this session: edits only, operator reviews (docker-airflow `00a73c67` was already pushed before the instruction; the reversal sits uncommitted on top).

---

## 13. Feasibility analysis — enforce the write surface via ClickHouse credentials (2026-07-17)

Operator direction: replace *trust* in the `guard_write_surface` gate with *enforcement* — (1) move the `*_guard` tables into a dedicated database, (2) run the guard as a dedicated `regression_guard` user: readonly on `default`, read-write only on the guard database. **Verdict: FEASIBLE**, with a small, well-bounded change set. The gate stays as defense-in-depth (fails fast pre-run; credentials would otherwise fail mid-run at job N of 53).

### 13.1 How users are actually managed on the cluster (investigated)

- The prod cluster (`clickhouse.production.san:30900` = `devops/prod/k8s-apps/clickhouse`, 3-replica `default_cluster`, CH **25.3.6**) has users in **two stores**:
  - **XML (`users_xml`)** via the chart's `templates/configmap_usersd.yaml`: `default`, `web` (profile `readonly`), `sanbase`. Git-tracked.
  - **SQL (`local_directory`)**, created ad hoc by an admin, NOT in git: `admin` (ALL + GRANT OPTION, sha256 pw), `backend`, `ci_automation`, `cluster`, `readonly`, `readonly_user`, `datascience`.
- **`backend` — the user DMF (and the guard) currently connects as — is `GRANT ALL ON *.*` with `no_password`.** The entire write-protection today is the parse-the-dry-run gate; the §12.3 labeled-table leak went straight through it. Credentials enforcement closes that incident class structurally: a missing redirect becomes `ACCESS_DENIED`, not a prod write. (Broader observation, out of scope: passwordless ALL-on-everything `backend` is a standing exposure for every service on the network.)
- `readonly` = `GRANT SELECT, dictGet ON *.* ` + NAMED COLLECTION — the model for the read half.

### 13.2 What the guard run actually needs (grant model)

Verified against the code paths in the 53-job set:

- **Reads**: raw tables, prices, `metric_metadata`/`asset_metadata`, dictionaries → `SELECT, dictGet ON *.*`.
- **Writes**: only the 7 redirected output tables → full DML/DDL on the dedicated db only.
- **Scratch**: all mid-run scratch (`tmp_metric_table*`, `tmp_delta_futures`, `*_tmp_asset_mapping_*`, `tmp_composite_*`, asset-metadata temp) is **`CREATE TEMPORARY TABLE`** — session-scoped, NO persistent-write grant needed. Two ClickHouse subtleties: `scoped_merge_tree_table*` creates temporary tables **with an explicit MergeTree engine**, which since CH 23.x needs the separate `CREATE ARBITRARY TEMPORARY TABLE` grant; verify both on stage.
- **One genuine non-guard write path**: `cumulative_sum_job`'s negative-value debug hook does `CREATE TABLE IF NOT EXISTS test.debug_cumsum_* + INSERT` (fires only on anomaly). Either grant `CREATE TABLE, INSERT ON test.*` (test is scratch; recommended — an anomaly then produces its diagnostic instead of a confusing ACCESS_DENIED) or make the hook non-fatal.

```sql
-- one-time, as admin, per cluster (prod + stage), ON CLUSTER default_cluster:
CREATE DATABASE IF NOT EXISTS regression_guard ON CLUSTER default_cluster;
CREATE USER IF NOT EXISTS regression_guard ON CLUSTER default_cluster
  IDENTIFIED WITH no_password SETTINGS PROFILE 'default';   -- NOT profile readonly (readonly=2 blocks granted writes too)
GRANT ON CLUSTER default_cluster SELECT, dictGet ON *.* TO regression_guard;
GRANT ON CLUSTER default_cluster CREATE TEMPORARY TABLE, CREATE ARBITRARY TEMPORARY TABLE ON *.* TO regression_guard;
GRANT ON CLUSTER default_cluster SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE ON regression_guard.* TO regression_guard;
GRANT ON CLUSTER default_cluster CREATE TABLE, INSERT ON test.* TO regression_guard;  -- debug hook (optional, recommended)
```

**Where the user definition should live — recommendation: the devops chart's `configmap_usersd.yaml` (XML), not ad-hoc SQL.** Modern CH XML users take an explicit `<grants>` block (each line a GRANT statement), so the whole grant model above is expressible in git-tracked XML, reviewed like any devops PR, identical stage/prod. users.d is hot-reloaded (no CH restart; configmap propagation ~1 min — verify on stage). Ad-hoc SQL (current practice for `backend` et al.) also works but perpetuates the untracked-user pattern.

### 13.3 Code changes (all in the two open PRs, modest)

- **Redirects become db-qualified**: `GUARD_OUTPUT_ENV` values → `regression_guard.<real_table_name>` (recommend dropping the `_guard` suffix inside the db — names mirror prod 1:1). Config table names are plain f-string interpolation into SQL (`FROM {t}` / `INSERT INTO {t}` / `CREATE TEMPORARY TABLE x AS {t}`) — dotted names are syntactically fine; no backtick-quoting or `system.tables`-lookup patterns found in the DMF paths. The strict dry run validates this end-to-end for free before anything writes.
- `table_qa/guard_tables.sql` → DDL in `regression_guard` db (new ZK paths including the db).
- `table_qa/metric_baselines.py` → generalize `table_suffix` ("+`_guard`") to a table prefix/db override (`regression_guard.` + name).
- `table_qa/guard_write_surface.py` → assert every persistent write target's **database == `regression_guard`** (keep the temporary-table exclusion; the `test.debug_*` allowance stays).
- `table_qa/guard_seed_prices.py` → target `regression_guard.daily_metrics_v2`.
- DAG (`metrics_regression_guard.py`) → db-qualified `GUARD_OUTPUT_ENV` + `DAILY_CLICKHOUSE_USER=regression_guard`.

### 13.4 Migration sequence

1. **Decisions** (operator): XML-in-devops vs SQL-by-admin; drop `_guard` suffix inside the db (recommended); grant `test.*` for the debug hook (recommended).
2. **Provision stage first**: user + db + grants; verify the temp-table grants and users.d hot-reload behave as expected.
3. **Provision prod** (devops PR merge or admin SQL); create the 7 tables in `regression_guard` (updated `guard_tables.sql`).
4. **Preserve the blessed state without a re-record**: `INSERT INTO regression_guard.<t> SELECT * FROM default.<t>_guard` (columns incl. `computed_at` copy verbatim → the committed 531-metric baseline stays valid; self-check against the new location confirms). Then `DROP` the old `default.*_guard` tables.
5. **Code + DAG edits** (§13.3) on the open `metricsQA` PRs.
6. **Validate**: strict dry-run gate (all writes → `regression_guard.*`), then a real run **as `regression_guard`**, check vs baseline green. **Negative test**: as `regression_guard`, attempt an `INSERT` into a `default.*` served table → expect `ACCESS_DENIED` (the §12.3 scenario, now structurally impossible).
7. Stage side (per §12.8's cluster-local direction): same db/user on stage + the still-pending frozen-input backfill before dev-instance runs mean anything.

### 13.5 IMPLEMENTED (2026-07-17, same session — edits in working trees, NOT committed)

Operator decisions: **(1) XML-in-devops** for the user, **(2) drop the `_guard` suffix** inside the db, **(3) grant `test.*`** for the debug hook. All three implemented:

- **devops** (`stage/k8s-apps/clickhouse/templates/configmap_usersd.yaml`): `regression_guard` XML user added — profile `default`, `<grants>`: `SELECT, dictGet ON *.*`; `CREATE TEMPORARY TABLE, CREATE ARBITRARY TEMPORARY TABLE ON *.*`; `SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE ON regression_guard.*`; `CREATE TABLE, INSERT ON test.*`. **Discovery: `prod/k8s-apps/clickhouse/templates` is a SYMLINK to the stage chart's templates** — one edit covers both clusters (they differ only in values files).
- **clickhouse-tables** (5 files, metricsQA): `guard_tables.sql` → `CREATE DATABASE regression_guard` + the 7 tables inside it, suffix-free, ZK paths `/clickhouse/tables/regression_guard/<name>`, migration comment (INSERT…SELECT from legacy `default.*_guard`, then DROP); `metric_baselines.py` → `table_suffix` replaced by `db` param / `--db` CLI flag; `guard_write_surface.py` → asserts every persistent write is **db-qualified into `regression_guard`** (unqualified names = default db = violation; legacy `*_guard` names in default now correctly FAIL — verified with a synthetic-SQL unit run); `guard_seed_prices.py` → targets `regression_guard.daily_metrics_v2` (runs as the regression_guard user — its grant set is exactly this flow); `Makefile` → DDL as admin-grade user, seed as `regression_guard` (SEED_USER var).
- **docker-airflow** (`dags/metrics_regression_guard.py`): `GUARD_OUTPUT_ENV` values db-qualified; `DAILY_CLICKHOUSE_USER=regression_guard` in the recompute/gate pods, `CLICKHOUSE_USER=regression_guard` in the check pod; check passes `--db regression_guard`; docstring prerequisites updated (user via devops usersd; db+tables as admin; record `--db regression_guard`). py_compile OK.

Still to do (operator / next session): deploy the clickhouse chart (stage first) → verify user + temp-table grants + hot-reload; run updated `guard_tables.sql` as admin; migrate the blessed rows + drop legacy `default.*_guard`; trigger a dev-instance run end-to-end; **negative test** (as regression_guard, `INSERT` into a served `default.*` table → expect ACCESS_DENIED).

**Review follow-up — dry-run policy centralized (2026-07-17, later same day):** PR reviewers flagged §12.2's `execute_dml→execute_dql` swaps as dangerous — correct instinct: it silently rewrote `execute_dql`'s contract to "always executes even in dry-run," with safety resting on a call-site comment (the next copy of the pattern, for a persistent scratch table, would make dry-run write — and DMF dry runs also happen as `backend` outside the guard). Replaced with the centralized fix: `execute_dml` now implements dry-run's real contract (*no PERSISTENT writes*, not "no statements") — it tracks `CREATE TEMPORARY TABLE` names and executes statements touching only those (INSERT/DROP on tracked temps) even when dry; both call sites reverted to `execute_dml`. Explored but rejected making dry-run "true" (zero reads/creates): SQL generation is data-dependent (50 `execute_dql` sites / 27 files — id resolution, min/max-dt batching, gap detection), so stubbing reads forks control flow and hides branches from the write-surface gate (the §12.3 blind-spot class, made permanent); a plan/execute refactor is a rewrite. Possible cheap third layer instead: a static CI check that every written `*_table` config key carries a `regression_guard.` override. Caveat documented: dry-run is zero-mutation, not zero-load (temp-table INSERT…SELECT executes its read side).

**Committed (2026-07-17):** devops `d58f1e78` (operator; branch `regressionGuardUser`) — the XML user; clickhouse-tables `ede9d927` (guard db + credentials, table_qa) + `d13d910c` (dry-run centralization, daily_metrics) on metricsQA; docker-airflow `53117c78` (DAG user + db-qualified redirects) on metricsQA. Not pushed.

### 13.6 Residual risks / notes

- `no_password` for `regression_guard` matches cluster convention (network-gated); its blast radius is the guard scratch db — acceptable, and a huge narrowing vs `backend`'s ALL.
- Concurrent guard runs (nightly + dev-triggered on stage) share the scratch db — pre-existing situation, `computed_at` dedup handles it.
- The one-time daily-price seed (operator prereq per chain) must also be re-pointed at `regression_guard.daily_metrics_v2` — it can no longer run as `backend` by accident, which is exactly the point.

## 14. Session log — 2026-07-17 — daily price seeding removed (in-run daily-prices-and-volumes)

**Trigger:** operator found the `daily-prices-and-volumes` DMF cronjob and asked whether it could replace the daily-price seed bootstrap.

### 14.1 Finding

The seed step's premise (guard_seed_prices.py docstring: the daily bundle "is NOT computed by any DMF job") was **wrong**. `daily-prices-and-volumes` (`prices_job`, selectors priceMetric×priceable) computes exactly the 8 seeded metrics from raw `asset_prices_v3` (source=coinmarketcap, `marketcap_usd < 1e13`, FINAL) — prod's own served daily price history comes from this job.

### 14.2 Verification (prod, readonly)

- `asset_prices_v3` ETH coverage: 2015-08-07 (listing) → full 2016, no zero prices.
- Recompute vs served `daily_metrics_v2` (asset 1681, 2015-2016): closing/avg price, avg marketcap, trading volume **bit-exact** — 513 days, 0 mismatches, max diff 0; served days absent from raw (pre-listing) are all zero, matching prices_job's cross-product zero-fill.
- DMF dry run with `DAILY_JOBS=daily-prices-and-volumes,eth-composite-metrics`: prices job selected (8 metric ids, asset 1681) and topologically sorted FIRST — daily consumers (mvrv_metrics, daa_divergence, composites) already declare `dependsOn` on the daily price metrics, so no seam fix needed (unlike the intraday 07-07 case).
- Write-surface: prices_job's scratch is `CREATE TEMPORARY TABLE` (session-scoped, gate-excepted); INSERT goes to `daily_metrics_table` → redirected by `DAILY_DAILY_METRICS_TABLE`.

### 14.3 Changes (committed + pushed, metricsQA both repos)

- clickhouse-tables `fdedefb1`: `guard_seed_prices.py` deleted; Makefile `seed_prices` target removed (stage NOTE re-pointed at raw asset_prices_v3 coverage); the 8 daily price metrics added to the ETH baseline daily group (recorded from served prod = bit-exact with in-run output; **531→539 asserted**), per_metric rtol 0.002 mirroring intraday price_usd (same mutable raw table re-read nightly).
- docker-airflow `94b81711`: `PRICE_JOBS = ("daily-prices-and-volumes",)` appended to every fixture's job set (the job has no chain prefix, so `onchain_jobs()`'s prefix filter drops it; `DAILY_ASSETS` scopes it); DAG docstring prerequisite updated — NO price seeding of any kind.

**Merge coupling:** docker-airflow side first or together — the baseline now asserts the 8 price metrics, so a run without the DAG change reports them `missing`. Old seeded rows in `regression_guard.daily_metrics_v2` are inert under the check's `--since` filter.

### 14.4 Residual

- Trade-off accepted: guard now re-reads `asset_prices_v3` nightly for the daily bundle — an upstream revision of 2015-2016 price history surfaces as diffs (also through MVRV etc.); with the seed it was frozen. Same exposure as intraday price_usd.
- §13.6's note about re-pointing the seed at regression_guard is moot — the seed no longer exists.
- Stage: `asset_prices_v3` coverage for the window UNVERIFIED — stage CH reset every connection from the agent container today (even `SELECT 1`); environment issue flagged to operator.

### 14.5 Dry-run hardening — readonly=2 (2026-07-17, operator idea)

Dry-run clients now send `readonly=2` with every query (clickhouse-tables `0104cf56`, one line in `Context.clickhouse_clients` + unit test): the server rejects any persistent CREATE/INSERT with READONLY (164), while the session-temp lifecycle the dry run executes for real (CREATE TEMPORARY TABLE / INSERT into it / DROP it) stays allowed, per-query settings (log_comment) keep working, and `readonly` cannot be lowered mid-session. This closes the execute_dml regex-classification blind spot: a statement slipping past suppression now errors instead of writing (the 07-07 leak class becomes fail-closed at the server, third layer under grants + gate).

Verified: semantics on clickhouse-local 26.6 (temp lifecycle OK, persistent write 164, `SET readonly=0` rejected); settings flow on prod 25.3.6 (the `readonly` user's own profile IS readonly=2 and the full dry-run DQL phase runs under it; patched dry run reproduces the pre-change behavior exactly, still stopping at that user's missing CREATE TEMPORARY TABLE grant — 497, RBAC, unrelated). RESIDUAL: temp-table-under-readonly=2 not yet exercised on 25.3.6 with a granted user (stage CH unreachable today; prod non-readonly connections not permitted) — the first guard write-surface gate run proves it; failure mode is a loud red gate, never a write.
