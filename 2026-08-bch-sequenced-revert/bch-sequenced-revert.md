# BCH sequenced-metrics revert

**Started:** 2026-08-27
**Repo:** `clickhouse-tables` (branch `bch-sequenced-revert`)
**PR:** — (not yet)

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
- Operator approved plan; implementing spec-flip on branch
  `bch-sequenced-revert`. No commit/push yet (review pending).
