# Custom-historical experimental runs

**Started:** 2026-09-21
**Status:** design agreed, build not started
**Repo:** `clickhouse-tables` (custom-historical DAG + DMF)
**Related:** `2026-09-historic-metrics-release-process` (release/recompute
side; shares the experimental-tables + grants build items)

## Goal

Extend the custom-historical DAG into a tool that computes one or more
*testing* metrics for one or more assets so an operator can review the
output before release. Analysis of the output is out of scope.

## Key decisions (2026-09-21)

1. **Two Airflow Variables extend the DAG**: `custom_dag_image_tag`
   (branch image — CI already pushes `:branch` + `:sha` on every push;
   `create_kube_pod_operator` already accepts a `tag` kwarg, pull policy
   Always) and `custom_dag_experimental` (redirects the *data*-table env
   knobs to the `*_experimental` set, all-or-nothing).
2. **`metric_metadata_versioned.status` is the metadata backbone.**
   Enum `development|testing|production`; the `metric_metadata` View
   (→ `metrics_by_name` dict → backend) filters `status='production'`,
   while the runtime ID lookup and the computation path ignore status.
   So a `status: development` metric computes normally but is invisible
   to users. Live pattern already (V31 social: 753 dev rows on prod).
   IDs are permanent across dev→production flips (populate re-inserts
   under the saved id). **No metadata-table overrides needed anywhere**;
   `metric_metadata_experimental` is obsolete for this flow.
3. **Populate runs from master only — merge first to register.**
   `populate_clickhouse_metadata.py` is a full-state reconciler: run
   from a stale/partial branch it rolls back specs changed on master
   since fork AND auto-deprecates assets missing from the local spec set
   (`populate_clickhouse_metadata.py:138-156`; the `DAILY_SPECS_PATH`
   subset variant is catastrophic). Rather than build scoped
   registration, we keep the trunk-only contract: new metrics merge with
   `status: development` (dark), new assets merge their yaml (no status
   gate — realtime computation starts immediately). Enforce with a
   guard in `populate-metadata.yml` refusing `real=true` off master.
4. **Rejected**: dry-run CSV pipeline variant (output must land in
   tables the backend can use); patching `metric_metadata_experimental`
   (wrong table — runtime resolves IDs from `metric_metadata_versioned`).

## Use-case flows

- **1. Recompute existing metric/asset (fix validation)** → branch
  image + experimental switch ON. No registration (same name+version =
  same id, prod-consistent). Rule: run must be *chain-complete* (every
  metric read from a metrics table is also computed in-run) or inputs
  pre-seeded into the experimental table. Review = UNION ALL diff vs
  prod. Ship = merge fix + normal live re-run; experimental rows
  disposable.
- **2. New metric, existing asset** → merge spec+code to master with
  `status: development` (auto-registered; realtime computes it dark
  from then on). Backfill history via custom-historical from the
  *master* image into the **real** tables (dark launch, chunked
  intervals + path-dependent metrics work since sink=source). Iterate
  with a branch image — no re-registration unless (name, version)
  changes. Release = flip label to `production`; backfill already in
  place, nothing recomputed.
- **3. Existing metrics, new asset** → merge asset yaml first
  (permanent asset_id via trunk populate; realtime starts computing
  into real tables under production metric ids — invisibility rests on
  sanbase having no project for the slug). Historic backfill under test
  goes to experimental for review; after approval run the classic
  new-asset stitch to live.

## Known hazards / caveats

- Data-table knobs not env-overridable (`intraday_nft_metrics`,
  `ecosystem_aggregated_metrics`, `xrp_assets`, labels, …) — exclude
  those jobs in experimental mode; grants-limited CH user is the
  backstop (shared item with release-process task).
- `dictGet('metrics_by_name', …)` is blind to dev metrics (dict sources
  the filtered view; missing key → 0 → silently empty query). Framework
  path (versioned table) unaffected.
- `precompute_available_metrics` scans data tables by id, no status
  filter → dev ids appear in `available_metrics`. Presumed harmless
  (backend addresses by name); verify on sanbase side.
- Cleanup of abandoned dev metrics: rows under a real id in
  unpartitioned `daily_metrics_v2`; reruns overwrite same keys, ranges
  not re-covered need mutations.
- One experiment at a time per DAG (global Variables); consider a
  dedicated `custom-historical-experimental` clone so operational
  new-asset stitching stays unblocked.

## Build items

- [ ] `custom_dag_image_tag` Variable → `tag` kwarg in custom DAG(s)
- [ ] `custom_dag_experimental` switch: curated all-or-nothing env set;
      document the chain-complete-or-seed contract
- [ ] `populate-metadata.yml`: refuse `real=true` when ref != master
- [ ] Missing experimental tables (address_profit, available_signals,
      historic_optimization inner, pit) — shared w/ release-process task
- [ ] Experimental-mode CH user, `%_experimental` writes only — shared
- [ ] Verify sanbase: dev-metric invisibility via the view;
      `available_metrics` dev-id noise harmless
- [ ] Decide: dedicated experimental DAG clone vs mode switch on
      existing custom-historical
