# BCH sequenced-metrics revert

**Started:** 2026-08-27
**Repo:** `clickhouse-tables` (branch `bch-sequenced-revert`)
**PR:** [#2341](https://github.com/santiment/clickhouse-tables/pull/2341) — spec flip; [#2342](https://github.com/santiment/clickhouse-tables/pull/2342) — icrc-payment-count dup removal (side find)

## What this task IS

BCH was the pilot chain for the "sequenced" (logical-clocks) metric
computation: jobs ignore the Airflow run interval and instead advance their
own cursor (max finalized `block_number`/`seq_num` per metric×asset read back
from the destination table), tagging every row with `seq_num`,
`block_number`, `is_finalized`. The experiment is being **reverted** to the
standard interval-based approach used by every other chain, because:

- **Slow compute** — each run does several cursor/gap/block↔time scans over
  both source and destination tables (with full-scan fallbacks), serialized
  by `max_active_runs=1` + `depends_on_past`.
- **No historic recompute** — rewriting the past conflicts with seq_num
  monotonicity; the sequenced jobs are excluded from all historical DAGs.

## Scope (verified: BCH is the only user of `sequenced_job`)

| Cronjob | Sequenced script | Revert to | Spec file |
|---|---|---|---|
| bch-active-addresses-intraday-deltas | active_addresses_sequenced_job | active_addresses_job | dags/intraday-metrics-bch-immutable.yaml |
| bch-cumulative-sums-intraday | cumulative_sum_sequenced_job | cumulative_sum_job (`intraday: true`) | dags/intraday-metrics-bch-immutable.yaml |
| bch-transaction-volume-intraday | transaction_volume_intraday_sequenced_job | transaction_volume_intraday_job | dags/intraday-metrics-bch-immutable.yaml |
| bch-age-distribution-intraday-deltas | age_distribution_intraday_sequenced_job | age_distribution_intraday_job | dags/intraday-metrics-bch-immutable.yaml |
| bch-stack-age-consumed-intraday | stack_age_consumed_intraday_sequenced_job | stack_age_consumed_intraday_job | dags/intraday-metrics-bch-immutable.yaml |
| bch-payment-count | payment_count_job_sequenced | payment_count_job | cronjobs/payment_count_cronjobs.yaml |
| bch-transaction-volume | transaction_volume_job_sequenced | transaction_volume_job | cronjobs/transaction_volume_cronjobs.yaml |

The 5 intraday jobs run in a dedicated `intraday-metrics-bch-immutable`
Airflow DAG (built in `airflow/dags/intraday_metrics_bch.py`); after the
revert they fold into the regular `intraday-metrics-bch` DAG + its
`-historical` sibling (standard per-chain pattern, cf.
`intraday_metrics_ltc.py`).

Out of scope: the SOL Snowflake pipeline (`airflow/dags/sf_intraday_*.py`)
also writes `seq_num`/`is_finalized` — separate experiment; because of it the
seq columns in the shared tables stay.

## Validated findings (prod, 2026-08-27, read-only)

- Consumers read `argMax(value, computed_at)` and ignore seq columns; columns
  have defaults (`seq_num 0`, `block_number 0`, `is_finalized true`) ⇒ revert
  is data-compatible, no migration; interval recompute overwrites via newer
  `computed_at`.
- `intraday_metrics`, asset_id 2484 (bitcoin-cash), rows with `seq_num > 0`:
  - active_addresses_delta_{1h,24h,7d,30d}: since 2024-08-07, 106k rows each
  - active_addresses_{1h,24h,7d,30d} (cumulative sums): since 2025-01-10, 77k rows each
  - transaction_volume_5min: since 2024-07-30, 115k rows
  - stack_age_consumed_5min: since 2024-08-22, 93k rows
  - pre-pilot interval-based rows (~1.7M per metric) sit under the sequenced era
- `daily_metrics_v2`: payment_count fully recomputed under sequencing
  (2009-01-06 → today, 6450 rows); transaction_volume sequenced since
  2024-07-25.
- Age-distribution sequenced rows go to `distribution_deltas_5min`;
  active-addresses delta-cancels to `intraday_delta_futures`.
- Cross-link: the known BCH `stack_circulation_*` zero-delta defect window
  starts 2024-08 — coincides with the sequenced takeover of age-distribution
  (2024-08-22). Full recompute may fix it; check after backfill
  (see ../2026-07-stack-circulation-100y/).

## Decisions (operator, 2026-08-27)

- Recompute **all** BCH history after the flip (not just the sequenced era);
  runs alongside realtime — computed_at versioning makes that safe.
- Standard per-chain DAG structure: immutable DAG removed, jobs fold into
  `intraday-metrics-bch` (+ historical).
- Sequenced framework code is deleted — but only **after** the no-sequence
  backfill is done and verified (keep it diagnosable meanwhile).

## Plan

1. **Spec-flip PR** (this branch): move 5 cronjobs into
   `intraday-metrics-bch.yaml` with plain scripts; delete
   `intraday-metrics-bch-immutable.yaml`; flip the 2 daily cronjob scripts;
   reduce `airflow/dags/intraday_metrics_bch.py` to the standard pattern;
   update `agent_docs/architecture.md` example.
2. Stage deploy → run flipped jobs on a small window; values + default seq
   columns.
3. Prod deploy → realtime healthy.
4. Backfill full BCH history: clear the new tasks across
   `intraday-metrics-bch-historical` runs (monthly since 2009-01-09, ~212
   runs; knobs `intraday_metrics_historical_max_active_runs=10`,
   concurrency 12) + daily historical for the 2 daily metrics.
5. Verify: seam continuity, sequenced-era diff, stack_circulation zero-delta
   window, regression-guard baseline membership (re-record if any of these
   BCH metrics are among the recorded 539).
6. **Cleanup PR**: remove the 7 `*_sequenced` modules, `sequenced_job()`
   (jobs/__init__.py), `sequence_number.py` + tests, sequenced insert helpers
   (`compute_multiple_metrics_sequenced`, seq params of
   `compute_multiple_delta_futures`). Table columns stay (SOL).

## Session log

### 2026-08-27 — analysis + step 1

- Analyzed sequenced machinery (`jobs/__init__.py:97`, `sequence_number.py`),
  confirmed BCH-only usage, queried prod data state (above).
- Operator approved plan; implemented spec-flip on branch
  `bch-sequenced-revert` (uncommitted, review pending):
  - 5 immutable cronjobs moved into `intraday-metrics-bch.yaml` (same names +
    selectors, plain scripts, BTC-style args); immutable yaml deleted.
  - `bch-payment-count` / `bch-transaction-volume` flipped to plain scripts.
  - `airflow/dags/intraday_metrics_bch.py` reduced to the standard per-chain
    pattern (== ltc file); `agent_docs/architecture.md` example updated.
- Validation: 238/238 daily_metrics tests pass; ruff clean on the DAG file;
  `export_dependency_graph.py --dag intraday-metrics-bch` against prod
  metadata (readonly user) → 25 nodes / 22 edges, all 5 moved jobs present,
  immutable graph skipped (no factories). Bonus: dependency edges
  age-distribution → {realized-cap, circulation, network-profit-loss,
  stack-age/price-consumed} and active-addresses-deltas → cumulative-sums
  now wire *within* one DAG — under the immutable split these crossed DAG
  boundaries unenforced.
- Gotchas hit: `.env.dev` sets `DAILY_JOBS`/`DAILY_ASSETS`, silently
  filtering the exporter to "no factories" — unset when exporting. Stage
  metadata also lacks these specs (skips even for ltc), so export must run
  against prod (readonly, DQL-only).
- Pre-existing, out of scope: duplicate cronjob name `icrc-payment-count`
  (twice in payment_count_cronjobs.yaml, also on master).

### 2026-08-27 — PRs opened

- Operator reviewed the diff; approved commit+push.
- [#2341](https://github.com/santiment/clickhouse-tables/pull/2341) — the
  spec-flip (branch `bch-sequenced-revert`).
- [#2342](https://github.com/santiment/clickhouse-tables/pull/2342) — removes
  the pre-existing byte-identical duplicate `icrc-payment-count` doc (branch
  `fix-icrc-payment-count-duplicate`, off master; no conflict with #2341 —
  different hunks of payment_count_cronjobs.yaml).
- Next (after merge + deploy): stage check, then full BCH history backfill
  (plan steps 2–4).

### 2026-08-27 — stage validation (step 2) PASSED

Operator deployed to a test Airflow cluster writing to stage CH and enabled
`intraday-metrics-bch-historical` (catchup from 2009). Inspected stage
(asset_id 381; metric ids differ from prod — resolve by name per cluster).

- DAG shape confirmed: immutable DAG gone; intraday historical has 24 tasks
  incl. the 5 moved ones (`bch-address-changes-delta-intraday-hourly` absent
  by design: `exportable: false`); daily-metrics-historical-bch has the 2
  flipped daily jobs.
- All 5 moved jobs write: deltas, cumulative sums, tx volume,
  age-distribution (distribution_deltas_5min), delta-cancels
  (intraday_delta_futures, 4.3M rows); backfill reached ~2010-12;
  seq_num=0 / is_finalized=true defaults everywhere.
- Values vs prod (monthly aggregates 2009-02..07):
  - transaction_volume_5min, stack_age_consumed_5min: **bit-identical**.
  - active_addresses_delta_24h: identical Mar–Jul; Feb-only cold-start diff.
  - active_addresses_24h: exactly **prod + 2599** — cumulative continuation
    correctly seeded from a stale stage row (2009-01-31 23:55, value 2599,
    computed_at 2026-06-14, old stage experiment). Not a code defect; on
    prod the seed is genuine pre-pilot data.
- Stage sources (bch transfers/stacks) proved fine for early history despite
  operator concern; only destination tables carried stale junk.
- **Gap found**: `@monthly` + start_date 2009-01-09 ⇒ first data interval is
  2009-02-01; 2009-01-09..31 never covered by catchup. For prod: either one
  manual run for Jan 2009 or accept (prod Jan data is valid and seeds the
  cumulative continuation). Recommended: accept.
- Next: prod deploy (step 3), then full backfill (step 4).

### 2026-08-27 — daily historical validation PASSED

Operator triggered `daily-metrics-historical-bch` on the test cluster.
Frontier at validation time: pc through ~2011-11, tv through ~2013-07;
starts correctly at 2009-01-09 (no first-month gap on the daily DAG).

- payment_count: **bit-identical to prod on every backfilled day** (0/3120
  mismatches; 2009+2010 full-year sums identical).
- transaction_volume: 67/3120 days differ only in the 6th decimal,
  max relative diff 6.7e-12 — float summation-order noise. Effectively
  identical.
- Stray fresh rows at dt 2026-05-21 with seq_num 460/11933,
  is_finalized=false: written by the **regular stage Airflow** still running
  the old `:stage` image (PR #2341 unmerged) — its realtime daily-metrics DAG
  keeps recomputing the tip of stage's stale source (ends 2026-05-21). Not
  from the test cluster; resolves on merge+deploy.

### 2026-08-28 — #2341 merged; stage cutover confirmed

- Merged 09:09 UTC. Stage auto-sync: clickhouse-tables:stage image flipped
  job scripts (same cronjob names ⇒ effective immediately, even before the
  Airflow restart); airflow rollout landed ~09:25 (09:12 restart was node
  churn; scheduler RS hash unchanged — deploy restarts are pod replacements
  on mutable :master tag).
- Confirmed: last sequenced write on stage 08:19 UTC (pre-merge), none
  after; job pods running under dag_id=intraday-metrics-bch (balance-changes,
  circulation, realized-cap seen live), zero under
  intraday-metrics-bch-immutable over a 18-min watch.
- Env notes: kubectl exec blocked via proxy ("Upgrade request required") on
  stage; stage airflow web 502/unauth — DAG-bag verified via pod dag_id
  labels + CH write silence instead.
- Next: publish GitHub Draft Release for prod cutover, then full backfill
  (same day preferred; cumulative BCH metrics ride the switchover seam until
  backfilled).
