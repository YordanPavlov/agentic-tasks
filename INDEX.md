# Task index

One line per task, newest first. Scan this file to find prior work; each task
directory is a self-contained journal (runbooks, session logs, analysis
scripts). Durable residue is distilled OUT on close-out: decisions → in-repo
ADRs, reusable procedures → agent skills, prod defects → tracker issues.

| task | status | dates | outcome |
|---|---|---|---|
| [2026-07-metrics-regression-guard](2026-07-metrics-regression-guard/metrics-regression-guard.md) | **active** | 2026-07-07 → | Nightly guard: recompute a fixture with HEAD code, diff vs a committed baseline → catch commits that silently change historical metric computation (the next night, not years later). Scoped to transfers/balances/stacks/prices; scope-by-exclusion; write-surface gate. Motivated by the XRP fossils (2022-03 acq-price methodology change + PR #2132 data loss). Harness + nightly DAG built (PRs clickhouse-tables#2274, docker-airflow#1714, branch `metricsQA`). ETH recompute of **537** on-chain metrics into `*_guard`, baseline recorded (asserts clean 2016). **2026-07-08 pre-merge audit: baseline NOT blessable as-is** — `transaction_volume_profit`/`_loss`/`_ratio` recorded as spurious **0** (intraday `price_usd` seed gap: `guard_seed_prices.py` seeds only daily prices, so the profit jobs ran before intraday price was manually bulk-loaded mid-run; the framework topo-sorts *computed* jobs but not *external inputs*). Fix: seed intraday price before recompute + re-run + re-record. Genuine fossil = NPL (guard 559M vs served 281M) caused by NULL→0 acquisition-price handling — served is arguably more-correct, so "bless HEAD" is in question. **Do not merge yet.** |
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
