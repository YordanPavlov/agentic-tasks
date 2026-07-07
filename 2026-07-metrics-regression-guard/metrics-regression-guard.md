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
- `dags/metrics_regression_guard.py` — nightly DAG, per chain: **seed-prices → assert-write-surface → recompute → check**, on `clickhouse-tables:master`. Jobs derived from the job graph minus `EXCLUDE_SUBSTR`. ETH fixture: `ethereum/2019-01-01` (asset_id 1681), window genesis → 2018-01-01.

## 5. Expected results (predicted from XRP)

- **Pure-age / transfers / balances metrics** (circulation, DAA, tx-volume, dormant, age-consumed, creation-timestamps, network-growth): **reproduce** (deterministic from raw).
- **Price-weighted metrics** (realized-cap, MVRV, MRP, NPL, supply-in-profit, price-consumed, price-derived volatility/rsi): likely show a **fossil gap** if ETH's served history predates the 2022-03 acq-price methodology change.
- The `_guard`-vs-served diff quantifies this split per-metric.

## 6. Current state / achieved (2026-07-07)

- Harness + DAG built, validated, and pushed (both PRs open on `metricsQA`).
- **Write-surface validated:** per-job dry-run of all 55 ETH jobs → 100% writes to the 7 `*_guard` tables. Nothing hits a served table.
- **Operator:** created the 7 `*_guard` tables (`ON CLUSTER`, after dropping legacy CODECs that the current CH rejects); seeded daily prices for ETH (asset 1681, dt < 2018-01-01, 8 metrics).
- **ETH recompute RUNNING** — single `main.py` pass, 55 jobs, genesis → 2018-01-01, into `*_guard` (writes validated `_guard`-only). Prod-write mechanism = `clickhouse_driver` writable user (same as the XRP daily run), bypassing the readonly wrapper.

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
- Reduced window to genesis → 2018-01-01 (nightly infra load).
- **Launched the ETH recompute into `*_guard`** (in progress at end of session).
