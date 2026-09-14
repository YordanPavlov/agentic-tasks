# Historic metrics release process

**Started:** 2026-09-14
**Status:** problem definition — base for brainstorming
**Repo:** `clickhouse-tables` (DMF + Airflow DAGs in `airflow/`)

## Problem statement

When source data (transfers / balances / stacks) is corrected, we have a
clean release mechanism: build a new versioned table, QA it at leisure,
then re-point the Distributed table — an atomic, seconds-long switch,
reversible by pointing back.

The **metrics layer has no equivalent**. Recomputing historic metrics on
top of the corrected source means clearing the (single) historical DAG
and letting it re-run, which has two defects:

- **(a) No inspection window.** New values go live the instant each job
  inserts them — there is no stage where the new metric version can be
  compared against the old before users see it.
- **(b) Incremental switch.** A full historical run takes ~days; during
  that window served data is a mix of new (early intervals) and old
  (not-yet-recomputed intervals). Time-series consumers see an
  inconsistent hybrid until the run completes.

Implicit third defect: **(c) no rollback.** Once merges collapse the
ReplacingMergeTree, the old version's rows are gone.

## Why it behaves this way (mechanics)

- Metrics land in shared tables `daily_metrics_v2` / `intraday_metrics`:
  `ReplicatedReplacingMergeTree(computed_at)`, `ORDER BY (asset_id,
  metric_id, dt)`, partitioned by `metric_id`. **Replicated, not
  sharded — there is no Distributed layer to switch** (unlike sources).
- The canonical read is `argMax(value, computed_at)` — latest write wins
  immediately. "Release" is a side effect of insertion, not a step.
- Recompute = clear runs of the per-asset historical DAGs
  (`intraday-metrics-{ticker}-historical`, `daily-metrics-historical-{ticker}`,
  monthly intervals, catchup). Destination tables are config knobs
  (`DAILY_DAILY_METRICS_TABLE` / `DAILY_INTRADAY_METRICS_TABLE` env vars
  exist) but all DAGs point at the live tables.
- Downstream fan-out complicates any table swap: MVs
  `*_to_metrics_store_mv` → `metrics_store` (filtered subset),
  `intraday_metrics_dt_optimization_mv`, `intraday_metrics_historic_optimization`;
  plus replication of facing tables to the client-facing cluster.
- Metric-level versioning exists only in metadata
  (`metric_metadata_versioned`, `status: production`, version label on
  specs) — it versions *definitions*, giving each version a new
  `metric_id`; it is not designed for re-releasing the *same* definition
  over corrected sources (id churn for every affected metric per
  incident would be its degenerate use).

## Existing assets / prior art

- `daily_metrics_v2_experimental` / `intraday_metrics_experimental`
  tables on prod + `*-experimental` bespoke DAG copies (used ad hoc for
  code experiments via image-tag pinning; DAG copies are hand-maintained
  and slated for removal).
- `*_guard` tables + nightly metrics-regression-guard: recomputes
  baseline metrics into a side table and diffs vs served — already the
  "compute aside and compare" pattern, but for detection, not release.
- `intraday_metrics_pit` (plain MergeTree) — point-in-time capture.
- Source-side switch runbook (opt-balances dt-collapse, avax v3): proven
  QA-then-switch flow that this task wants to replicate one layer up.

## Goal

A release process for recomputed historic metrics with the same
properties the source layer already has:

1. Full recompute lands somewhere **not visible to consumers**.
2. **Inspection window**: arbitrary old-vs-new comparison before release
   (table_qa-style gates, guard-style diffs, manual analysis).
3. **Atomic go-live** (seconds, all intervals at once), ideally scoped
   (per asset / per metric set affected by the source fix).
4. **Rollback** for some grace period.
5. Realtime DAGs keep writing throughout (the tip must not go stale
   during the days-long recompute, and the switch must not lose the
   tip).

## Constraints

- Consumers (sanbase, metrics_store subscribers, facing cluster) must
  not need query changes — the read pattern and table names they use
  should keep working, or any change must be coordinated/transparent.
- Metric tables are shared across all assets/metrics; a fix is usually
  scoped to one chain's metrics — full-table swaps are too blunt unless
  combined with selective copy-back.
- MV fan-out fires on INSERT into the live tables; writes landing
  elsewhere bypass it (must be replayed at switch), writes landing live
  are irreversible (guard leak of 07-07 was exactly this class).
- Airflow side: one historical DAG per asset, no mechanism to run two
  versions of it concurrently against different destinations.

## Candidate directions (to brainstorm)

Seeds only — not evaluated yet:

1. **Side-table recompute + merge-back release**: point historical DAG
   (via the existing table-name env vars) at a scratch table; on
   approval, INSERT-SELECT into live with fresh `computed_at`
   (release = one insert, minutes not days; rollback = re-insert old
   snapshot; needs old-version snapshot + MV/tip handling).
2. **Introduce a switchable layer over metric tables** (Distributed or
   proxy/view per metric table, like sources): heavyweight — renames
   under consumers, MV re-pointing, facing replication.
3. **Partition-level surgery**: `PARTITION BY metric_id` means
   `REPLACE PARTITION` from scratch table to live is atomic per metric —
   possibly the cheapest atomic switch, if computed_at/tip semantics
   work out.
4. **Version dimension in the data** (e.g. version column + served-version
   pointer, or exploit `metric_metadata_versioned` ids): clean model,
   but touches consumers/read pattern.
5. **Process-only improvement**: keep instant-live but sequence re-runs
   newest-first / freeze-and-blitz to shrink the hybrid window — cheap,
   doesn't solve (a).

## Open questions

- How much of the guard/table_qa tooling can be reused as the
  "inspection" step verbatim?
- Does `REPLACE PARTITION` interact safely with the realtime DAG writing
  into the same partition during the switch?
- Scope unit of a release: per chain? per (metric set × date range)?
- Do we need rollback beyond "keep the old snapshot table for N days"?
- What happens to `metrics_store` / dt-optimization / facing during and
  after a swap — replay, recompute, or MV-on-scratch-table?
