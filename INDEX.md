# Task index

One line per task, newest first. Scan this file to find prior work; each task
directory is a self-contained journal (runbooks, session logs, analysis
scripts). Durable residue is distilled OUT on close-out: decisions → in-repo
ADRs, reusable procedures → agent skills, prod defects → tracker issues.

| task | status | dates | outcome |
|---|---|---|---|
| [2026-07-genesis-acquisition-price-valuation](2026-07-genesis-acquisition-price-valuation/genesis-acquisition-price-valuation.md) | not started | 2026-07-08 → | Prod defect spun out of the regression guard. PR **#2132** (LEFT ASOF acquisition-price fix, merged 2026-03-02) values cohorts acquired **before any price exists** (ETH genesis/premine, acqTime 2015-07-30) at `acquisition_price = 0` — unmatched ASOF + default `join_use_nulls=0` fills `0` not NULL — so profit-weighted metrics book 100% of their value as profit. ETH `network_profit_loss` 2016: **559M** (HEAD) vs **281M** (served); the +278M is entirely genesis. Fixed the data-loss but introduced a valuation bug. Fix options: `join_use_nulls=1` / explicit exclude / impute genesis price (product call). Confirm blast radius across premine/pre-price chains (BTC, XRP). |
| [2026-07-metrics-regression-guard](2026-07-metrics-regression-guard/metrics-regression-guard.md) | **active** | 2026-07-07 → | Nightly guard: recompute a fixture with HEAD code, diff vs a committed baseline → catch commits that silently change historical metric computation (the next night, not years later). Scoped to transfers/balances/stacks/prices; scope-by-exclusion; write-surface gate. Harness + nightly DAG built (PRs clickhouse-tables#2274, docker-airflow#1714, branch `metricsQA`). ETH recompute of **537** on-chain metrics into `*_guard`. **2026-07-08 pre-merge audit found baseline NOT blessable yet:** (1) `transaction_volume_profit`/`_loss`/`_ratio` were spurious **0** from an intraday-`price_usd` seed gap — **FIXED**: `guard_seed_prices.py` now seeds intraday price too; operator to re-seed + re-run + re-record. (2) NPL 559M-vs-served-281M is a genuine defect → spun out to [genesis-acquisition-price-valuation](2026-07-genesis-acquisition-price-valuation/genesis-acquisition-price-valuation.md); hold the price-weighted family until fixed. Framework topo-sorts *computed* jobs but not *external inputs* (the seed-gap root). **Do not merge until re-recorded.** |
| [2026-06-odt-bucketing-xrp](2026-06-odt-bucketing-xrp/verifying-the-batched-odt-migration.md) | **active** | 2026-06-29 → | Hourly `odt` bucketing in the Flink stacks job, XRP first. Layer 1 (source equivalence) + Layer 2 (recomputed metrics) both PASSED — bucket cleared for next chains. Savings: stacks −28% rows, seam −49%, futures −56%. Pending: Flink savepoint/RocksDB-gauge state comparison, prod-defect escalations, NPL product call. ADR: `etherbi-flink/docs/decisions/configurable-odt-bucketing.md`. |
| [2026-06-flink-job-build-harness](2026-06-flink-job-build-harness/flink-testability-job-build-harness.md) | not started (plan) | 2026-06-29 → | Step #3 of the test-hardening sequence from the XRP `InvalidTypesException` incident: a `buildsCleanly` harness calling `job.build(config)` offline, a parametrized all-jobs test (15 jobs), and a CI gate so a Flink graph that won't assemble can't merge. Step #2 (lambda → `MapFunction` in `XRPStacks`) already done. Main open question: enumerate jobs by list, classpath scan, or real deploy configs. |
| [2026-06-ltc-stacks-deprecation](2026-06-ltc-stacks-deprecation/ltc-stacks-deprecation.md) | parked (Phase 1 done) | 2026-06 | Replace LTC stacks with balances-derived metrics. Phase 1 (cumsum-safe set) executed + validated; windowed metrics (Phase 2 dip-scan) sketched, awaiting product decisions on the windowed term structure. Origin of `compare_ltc_experimental.py` (template for re-run diffs). |
| [2026-06-onchain-metrics-source-map](2026-06-onchain-metrics-source-map/onchain-metrics-source-map.md) | done | 2026-06-03 | Dependency map coloring every on-chain metric by its raw source (`*_transfers` / `*_balances` / `*_stacks`) — the blast radius of dropping `_stacks`. Key result: all blue metrics root at the single `age_distribution_5min_delta` pivot, and `total_supply` is stacks-dependent only by wiring. Basis for the LTC stacks deprecation. Regen: `extract_source_map.py` (from specs) + `build_metrics_map.py` (curated Graphviz). |

## Conventions

- Directory per task: `YYYY-MM-<kebab-slug>/`, dated by task start.
- Keep the journal append-only during the task (session logs with dates); the
  top of the main doc says what the task IS, session logs say what happened.
- Update the status column here on every significant milestone. A pre-commit
  hook (`.githooks/`, enabled via `core.hooksPath`) rejects commits that touch
  a task directory without touching this file.
- Keep the outcome cell to one or two sentences — current state and where to
  look. Detail belongs in the task's own journal.
