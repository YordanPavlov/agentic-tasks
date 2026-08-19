# CI metric-registration gap — new metrics break the airflow image build

**Started:** 2026-08-19
**Repo:** `clickhouse-tables`
**Trigger:** PR [#2308](https://github.com/santiment/clickhouse-tables/pull/2308)
(`stackCirculation100`) — [failing build job](https://github.com/santiment/clickhouse-tables/actions/runs/32266479646/job/96112663359)
**Related task:** [stack-circulation-100y](../2026-07-stack-circulation-100y/stack-circulation-100y.md)
(the PR that surfaced this; blocked by it)

## What this task IS

Since the August 2026 CI refactor, **any PR that adds a new metric spec fails
the `build and push` job of `build-airflow.yml`**, and CI offers no path to
unblock itself: the only step that registers a metric in ClickHouse runs
`--dry-run` on branches and PRs. This task documents the failure, the two
master commits that created it, and sketches fixes. Scope is the CI/build
architecture — not the metric in #2308.

## The failure

`airflow/Dockerfile` stage `generator`, step `[generator 5/5]` (Dockerfile:32),
first `python3 export_dependency_graph.py`:

```
Traceback (most recent call last):
  File "/build/daily_metrics/export_dependency_graph.py", line 116, in main
    scripts = list(fetch_scripts(context, filter_by_dag=True))
  File "/build/daily_metrics/job_factory.py", line 149, in fetch_scripts
    raise Exception(
Exception: Job eth-cumulative-sums has metrics ['stack_circulation_100y/2019-01-01']
           which are not imported in ClickHouse.
```

`fetch_scripts` resolves every matched metric spec to a numeric `metric_id`
from `metric_metadata_versioned` and hard-raises on any spec without one
(`daily_metrics/job_factory.py:141-151`, a check in place since 2023). Confirmed
against both clusters on 2026-08-19: neither `stack_circulation_100y` nor
`stack_circulation_delta_100y` exists in `metric_metadata_versioned` on prod or
stage (the `_20y`, `_10y`, `_inf` siblings all do). So the exception is correct
about the state of the world — what changed is that this state now blocks a
build.

## Root cause: two master commits

1. **`88d1b26e` "Bake airflow artifacts into image" (2026-08-12)** — moved
   graph / asset-CSV / `airflow-dags-config.yaml` generation *into the CI docker
   build*, replacing the runtime S3 sync (`dags-s3-clone` / `dags-s3-sync`
   sidecars). `git log -S export_dependency_graph -- .github/ Jenkinsfile
   airflow/Dockerfile` returns **only this commit**: before it, no CI step ever
   ran `export_dependency_graph.py`. Graphs were produced inside the cluster,
   *after* the DAG's `ch-metadata` task
   (`populate_clickhouse_metadata.py`, wired at `airflow/dags/utils.py:744`) had
   inserted the new metric rows. A metric missing from CH therefore never broke
   a build — it self-healed on the next DAG run.
2. **`a90d40ce` "ci: populate CH metadata; dry-run on PR/branch, real on
   tags/release" (2026-08-13)** — added `.github/workflows/populate-metadata.yml`.
   It resolves `MODE=--dry-run` for `pull_request` and branch pushes; a real
   apply happens only on a **tag push, a release, or `workflow_dispatch` with
   `real=true`**.

Together: the build now *requires* metrics to be registered in ClickHouse, while
the only automation that registers them is gated off for exactly the branches
that introduce them. The two workflows are also independent — even when
`populate-metadata` does apply for real, nothing orders it before
`build-airflow`.

Neither commit is wrong on its own; the gap is in the seam. Baking artifacts is
a real improvement (deterministic images, no runtime sidecar), and gating writes
to shared metadata behind tags is a real safety property.

## Blast radius

- Every future PR adding a metric spec hits this, with an error message that
  names the symptom but not the remedy.
- The image build now has a **hard runtime dependency on ClickHouse**
  availability and on the metadata contents of whichever cluster the runner
  lands in — a CH outage or a stale metadata state breaks image builds
  repo-wide, not just metric PRs.
- Same class of failure exists for **assets**: `job_factory.py:139` asserts
  `asset_metadata != {}` per job. A PR adding only new assets to a brand-new
  job would fail the same way.

## Fix ideas

Not yet decided; roughly in order of increasing invasiveness.

### A. Document + manual dispatch (immediate, no code)
Add to `daily_metrics/README.md` / `agent_docs/architecture.md` and the PR
template: *"new metric or asset spec → dispatch `populate clickhouse metadata`
with `real=true` on your branch before the build runs."* Also improve the
exception text in `job_factory.py:149` to name the fix command. Cheap; leaves a
manual step on every metric PR.

### B. Register-before-build in CI (probable real fix)
Run the populate step for real, ahead of the image build, on branch pushes.
Guardrails that make this defensible:
- The metric path of `populate_clickhouse_metadata.py` is **insert-only for new
  names** (`INSERT INTO metric_metadata_versioned (metric_id, name, version,
  specification, status)`); a new row is inert until a job writes data under
  that `metric_id`. Registering early is close to harmless — unlike the asset
  path, which also updates existing rows.
- Consider a `--metrics-only --new-only` mode so a branch can never mutate
  existing metadata.
- Ordering needs the two workflows merged, or `populate-metadata` converted to a
  reusable `workflow_call` that `build-airflow` declares in `needs:`.

Open question: **which cluster** does the runner's default
`clickhouse-{0,1,2}.default.svc.cluster.local` resolve to (`config.py:141`), and
is it the same one the `generator` stage queries? The fix is only coherent if
registration and build see the same CH. Verify before implementing.

### C. Build-side tolerance flag
Give `export_dependency_graph.py` an explicit
`--allow-unregistered-metrics` that downgrades the `job_factory.py:149` raise to
a warning and drops unregistered metrics from the graph. Keeps branch builds
independent of CH writes. Risks: the baked graph silently lacks the new metric's
edges, so the image is wrong-but-green, and a later registration doesn't
retroactively fix the image. If taken, the flag must be build-only — the runtime
path (`main.py`) must keep the hard failure — and the build should print a loud
summary of what it dropped.

### D. Decouple ids from CH (structural, large)
Assign metric ids deterministically from the specs (or carry them in the yaml)
so graph generation is a pure function of the repo. Removes CH from the build
critical path entirely and fixes the outage-breaks-builds problem, but it is a
significant change to how `metric_metadata_versioned` is owned.

### E. Pre-flight lint job (complements any of the above)
A fast PR job that diffs metric/asset specs against CH metadata and comments
with the exact registration command. Turns a 3-minute docker build failure with
a buried traceback into an immediate, actionable check.

**Leaning:** A now (unblocks #2308 today), B as the real fix, E as the ergonomic
wrapper. C only if we decide branch builds must never write to shared metadata.

---

## Session log

### 2026-08-19 — diagnosis

Traced PR #2308's red `build and push` from the raw job log (the
`gh run view --log-failed` output truncates before the traceback; use
`gh api .../jobs/<id>/logs --allow-escape-sequences`). Confirmed the missing
metrics against prod and stage `metric_metadata_versioned`. Bisected the CI
history to `88d1b26e` + `a90d40ce` as the cause. No fix implemented yet — task
opened to hold the options above; #2308 still blocked pending a decision on A/B.
