# Custom-historical experimental runs

**Started:** 2026-09-21
**Status:** build started — branch `custom-historical-experimental` (a46b13b0)
pushed with the DAG knobs; PR not yet created; stage test pending
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
3. **REVERSED 2026-09-21: branch populate runs on stage are a feature,
   not a hazard — no guard.** Deeper reading of the reconciler showed
   the trunk-only rationale was overstated:
   - **No removal path for metrics**: branch-new registrations are
     permanent (wanted — that's pre-merge stage registration; the
     framework resolves `(name,version)→metric_id` from
     `metric_metadata_versioned` at runtime, so an unregistered metric
     can't compute at all). Assets DO get a `deprecated:true` patch.
   - **Rollbacks are content-only and transient**: computation reads
     specs from the image's YAML, not the DB; the DB `specification`/
     `status` rollback heals ≤30 min via the `ch-metadata` task in the
     realtime `daily-metrics` DAG (stage+prod, from env image). Impact
     is view/dict visibility blips (+ niche: jobs reading the
     `metric_metadata` view at runtime, e.g. composite_labeled_holders).
   - **The workflow is stage-only by construction**: `runs-on:
     san-runner` = ARC scale set in the stage cluster (prod's is
     `san-runner-prod`), default cluster-local CH hosts, no prod creds.
     Prod's registry writer is `ch-metadata` from the `:production`
     image → registration reaches prod only on release.
   Contract: rebase before dispatching (stale checkout rolls back
   others' content until the next tick) and ALWAYS label new metrics
   `status: development` — the default is `production` = instantly
   user-visible, and registrations can't be auto-undone. Candidate
   follow-up: populate-side validation refusing new metrics without an
   explicit status label. (The `DAILY_SPECS_PATH` subset variant
   remains catastrophic — full reconcile from a partial spec set.)
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

- [x] `custom_dag_image_tag` Variable → `tag` kwarg (custom DAG 1 only)
- [x] `custom_dag_experimental` switch: `EXPERIMENTAL_TABLE_ENV_VARS`
      in `utils.py` (14 knobs, all-or-nothing; the 3 missing
      experimental tables are redirected anyway → loud pod failure, no
      prod-table leak); contract documented in `airflow/README.md`
- [x] ~~`populate-metadata.yml` guard~~ — REVERSED, see decision 3
- [x] Decide clone-vs-switch: knobs on `custom-historical-metrics`
      only; `custom-historical-metrics-2` stays vanilla for
      operational runs
- [ ] Stage test via airflow-dev (below), then PR
- [ ] Missing experimental tables (address_profit, available_signals,
      historic_optimization inner, pit) — shared w/ release-process
      task; exist on neither stage nor prod (both otherwise have the
      same 10 `*_experimental` tables)
- [ ] Experimental-mode CH user, `%_experimental` writes only — shared
- [ ] Verify sanbase: dev-metric invisibility via the view;
      `available_metrics` dev-id noise harmless
- [ ] Candidate: populate validation — refuse NEW metrics lacking an
      explicit `status` label (production-default footgun)

## Testing on stage: airflow-dev cluster

Personal Airflow at `devops/stage/k8s-apps/airflow-dev`:

```bash
NAMESPACE=yordan-p CH_BRANCH=custom-historical-experimental ENVIRONMENT=stage make install
```

(`make config` = `envsubst < values.yaml.template > values.yaml` under
the hood.) `CH_BRANCH` picks the docker-airflow image with the branch's
DAG code baked in (CI pushes it + syncs graph to
`s3://airflow-meta-stage/graph/<branch>`); keep `ENVIRONMENT=stage` so
job pods default to `clickhouse-tables:stage` — then
`custom_dag_image_tag=custom-historical-experimental` proving the
override is non-vacuous. Job pods hit stage CH (cluster-local hosts).
Set the `custom_dag_*` Variables in the UI (Admin→Variables), incl. the
two new knobs; trigger `custom-historical-metrics`; verify pod image +
`DAILY_*_TABLE=*_experimental` env vars (`kubectl get pod -o yaml`) and
rows in `daily_metrics_v2_experimental`. Negative test: unset both →
`:stage` image + real tables. Known flake: Variables not loading on
first start → delete the scheduler pod. Redeploy after new commits:
`make restart` (DAGs are baked into the image, not synced).
