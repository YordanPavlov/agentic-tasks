# Task index

One line per task, newest first. Scan this file to find prior work; each task
directory is a self-contained journal (runbooks, session logs, analysis
scripts). Durable residue is distilled OUT on close-out: decisions → in-repo
ADRs, reusable procedures → agent skills, prod defects → tracker issues.

| task | status | dates | outcome |
|---|---|---|---|
| [2026-07-metrics-regression-guard](2026-07-metrics-regression-guard/metrics-regression-guard.md) | **active** | 2026-07-07 → | Nightly guard: recompute a fixture with HEAD code, diff vs a committed baseline → catch commits that silently change historical metric computation (the next night, not years later). Scoped to transfers/balances/stacks/prices; scope-by-exclusion; write-surface gate. Motivated by the XRP fossils (2022-03 acq-price methodology change + PR #2132 data loss). Harness + nightly DAG built (PRs clickhouse-tables#2274, docker-airflow#1714, branch `metricsQA`). ETH validated end-to-end: recompute of all **539** on-chain metrics into `*_guard`, baseline recorded (asserts clean year 2016). Fossil-gap vs served: ~520 reproduce; fossils concentrated in the **price/profit family** (NPL severe, dollar-days/realized-cap moderate) — same signature as XRP. Next: merge #2274+#1714 → deploy nightly DAG; roll out chains. |
| [2026-06-odt-bucketing-xrp](2026-06-odt-bucketing-xrp/verifying-the-batched-odt-migration.md) | **active** | 2026-06-29 → | Hourly `odt` bucketing in the Flink stacks job, XRP first. Layer 1 (source equivalence) + Layer 2 (recomputed metrics) both PASSED — bucket cleared for next chains. Savings: stacks −28% rows, seam −49%, futures −56%. Pending: Flink savepoint/RocksDB-gauge state comparison, prod-defect escalations, NPL product call. ADR: `etherbi-flink/docs/decisions/configurable-odt-bucketing.md`. |
| [2026-06-ltc-stacks-deprecation](2026-06-ltc-stacks-deprecation/ltc-stacks-deprecation.md) | parked (Phase 1 done) | 2026-06 | Replace LTC stacks with balances-derived metrics. Phase 1 (cumsum-safe set) executed + validated; windowed metrics (Phase 2 dip-scan) sketched, awaiting product decisions on the windowed term structure. Origin of `compare_ltc_experimental.py` (template for re-run diffs). |

## Conventions

- Directory per task: `YYYY-MM-<kebab-slug>/`, dated by task start.
- Keep the journal append-only during the task (session logs with dates); the
  top of the main doc says what the task IS, session logs say what happened.
- Root-level symlinks exist for pre-repo absolute paths (older docs/agent
  memories reference `~/santiment/tasks/<old-name>`); don't add new ones for
  new tasks.
- Update the status column here on every significant milestone. A pre-commit
  hook (`.githooks/`, enabled via `core.hooksPath`) rejects commits that touch
  a task directory without touching this file.
- Keep the outcome cell to one or two sentences — current state and where to
  look. Detail belongs in the task's own journal.
