# Historic metrics release process

**Started:** 2026-09-14
**Status:** direction selected (2026-09-14 brainstorm) — experimental-tables test runs
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

## Mechanics (verified on prod 2026-09-14)

- Metrics land in shared tables `daily_metrics_v2` / `intraday_metrics`:
  `ReplicatedReplacingMergeTree(computed_at)`, `ORDER BY (asset_id,
  metric_id, dt)`. **Partitioning: `daily_metrics_v2` is UNPARTITIONED;
  `intraday_metrics` is `PARTITION BY toYYYYMM(dt)`** (the earlier
  "partitioned by metric_id" claim was wrong — kills per-metric
  REPLACE PARTITION ideas). Replicated, not sharded — no Distributed
  layer to switch.
- The canonical read is `argMax(value, computed_at)` — **write = release**.
  This one decision produces all three defects.
- MV fan-out on INSERT into the live tables (bypassed by ATTACH/swap;
  fires on any insert, including unwanted ones):
  `daily_metrics_v2_to_metrics_store_mv` / `intraday_metrics_to_metrics_store_mv`
  → `metrics_store` (filtered via `filter_metric_ids`/`filter_asset_ids`),
  `intraday_metrics_dt_optimization_mv`, `intraday_metrics_historic_optimization`,
  `available_intraday_metrics_mv`, sql_exporter health MVs. Inbound:
  `mv_intraday_metrics_pit_transfer` (`intraday_metrics_pit` →
  `intraday_metrics`) — part of the intraday write path.
- Destination tables are config knobs (`daily_metrics/config.py`:
  `daily_metrics_table`, `intraday_metrics_table`,
  `sink_intraday_metrics_table`, `delta_futures_table`,
  `address_profit_table`, … — env-settable with `DAILY_` prefix). The
  knob list is effectively the manifest of every table a run touches.
- **All intermediates are also `ReplicatedReplacingMergeTree(computed_at)`**
  (delta futures ×2, `distribution_deltas_5min`, `address_profit`,
  label-based tables, availability, job progress) — uniform semantics
  across the whole table set.

## Selected design: experimental-tables test runs (simple, partial goals)

Reuse the existing always-on `*_experimental` clone tables as a shared
staging environment. Flow per repair:

1. **Test run**: historical DAG(s) run with (i) optionally a pinned
   custom `clickhouse-tables` image and (ii) a switch that redirects
   **all** output-table knobs to the `_experimental` set. Data lands
   invisibly; realtime untouched.
2. **QA at leisure**: diff experimental vs live (guard machinery /
   `compare_metrics_with_reference.py` / table_qa-style gates).
3. **If approved**: snapshot the scope's served values from live (cheap
   insurance), then clear the main historic DAG and re-run to live as
   today. **Double compute, very simple mental model.**

Scorecard: fixes (a) fully (inspection window), keeps realtime writing
(goal 5). Deliberately gives up atomic go-live (b) and real rollback (c)
— the live re-run is still incremental & irreversible. Accepted trade.

### Build items

- **DAG factory flag, not cloned DAG files**: parameterize the existing
  historical DAG factory with image override + experimental-tables
  switch. The switch must flip the knobs **as an all-or-nothing set** —
  a partial flip is exactly how pollution happens. Deletes the
  hand-maintained `*-experimental` DAG copies.
- **Grants over convention**: dedicated CH user for experimental mode
  with write grants only on `%_experimental` — a missed knob fails
  loudly instead of writing to prod (kills the 07-07 leak class).
  Highest-value hardening; insist on it.
- **Fill the table-set gaps** (verified 2026-09-14): experimental
  variants exist for finals, delta futures ×2, distribution_deltas,
  label-based ×3, dt_optimization, available_metrics, metric_metadata,
  metrics_by_name. **Missing: `address_profit`** (P/L state — real
  pollution risk), `available_signals`, historic_optimization inner
  table, `intraday_metrics_pit` (if sink path used). Generated DDL;
  mind explicit ZooKeeper paths (`/clickhouse/tables/global/…`) —
  clones need fresh ZK paths.
- **Truncate step** at test-run start — stale rows from prior
  experiments poison the QA diff and availability. One experiment per
  chain at a time (different chains coexist fine).
- **Snapshot before clearing live**: dated `INSERT SELECT` of the
  scope's served values → rollback = re-insert with fresh computed_at.
- **Same-image discipline**: QA'd run and live re-run must use the same
  image and unchanged sources, else what ships ≠ what was approved.

## Explored & set aside (decision trail)

1. **Side-table recompute + merge-back INSERT** — works, no consumer
   changes; release is minutes not seconds; superseded by (2).
2. **Side tables + `ATTACH PARTITION FROM` promotion** — the best full
   solution found: attach copies parts via hardlink (source table keeps
   its data), seconds-atomic for the unpartitioned daily table
   (`PARTITION tuple()`), per-month for intraday; no consumer/schema
   changes. Evolved into *ephemeral per-incident clone DBs*: full table
   set (incl. intermediates) deployed per re-run, grants-isolated,
   genesis-only re-runs (kills state seeding), promotion-as-PR
   (snapshot → attach → scoped deletes → MV replay), then DAG + tables
   dropped. Fully specified; set aside for complexity, **not** dead —
   bolts onto the selected design later (experimental tables are the
   clone set; promotion replaces the second live re-run) if defects
   (b)/(c) start hurting.
   Carried obligations of any attach design: attach doesn't fire MVs
   (needs downstream replay), can't delete live-only keys (needs scoped
   `ALTER DELETE`; worst on delta_futures — stale future contributions
   keep firing), freeze discipline between QA and attach.
3. **run_id version dimension in main tables** — requires run_id
   appended to ORDER BY (doable in-place: `ADD COLUMN … MODIFY ORDER BY`)
   plus read-side resolution. Resolution options all rejected: view
   under the name (ruled out), row policies (complexity moved, not
   removed), consumer filter `run_id IN released_runs` (feasible,
   no-flag-day migration, but DMF-internal reads are a big surface and
   the unfiltered long tail is risky). Key insight kept: with a static
   released-runs set, argMax(computed_at) resolves the rest — no
   per-scope pointer needed.
4. **Distributed/proxy switch layer, per-metric partition surgery,
   process-only sequencing** — rejected (heavyweight / wrong
   partitioning / doesn't give inspection).

## Greenfield view (strategic notes)

Root cause of all pain: **write = release** — storage, version
resolution and serving conflated in ReplacingMergeTree+argMax. The
greenfield shape (implementable on our stack): immutable per-run
datasets, a catalog mapping (metric, asset) → served version, version
resolution in the **sanbase registry** (the one indirection point that
touches no end consumer), explicit history/tip split, Airflow Datasets
for data-aware orchestration, derived stores rebuilt at release instead
of insert-time MVs. Usage decisions that block it today, cheap→dear:
single namespace/omnipotent writer; non-hermetic state; task-cron
Airflow with no source-version lineage; insert-time MV fan-out;
consumers bound to physical names; shared mega-tables; write=release.
Strategic successor task if ever funded: **version resolution in the
sanbase metric registry** — after that, true pointer-flip releases
become possible and the MV-replay problem dissolves.

## Next steps

- [ ] DAG factory: image override + all-or-nothing experimental switch
- [ ] Create missing experimental tables (address_profit, available_signals,
      historic_optimization inner, pit) with fresh ZK paths
- [ ] Experimental-mode CH user + grants (`%_experimental` writes only)
- [ ] Truncate + snapshot steps; QA diff runbook (guard reuse)
- [ ] Retire hand-maintained `*-experimental` DAG copies
