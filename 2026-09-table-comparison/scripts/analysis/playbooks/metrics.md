# Playbook: metrics

`daily_metrics_v2` / `intraday_metrics` (and `_experimental`): one row per `(asset_id, metric_id, dt)`, `value`.
Fully replicated, grouped by `(asset_id, metric_id)`; `common_groups_only` leaves metrics the experimental run did
not compute to `coverage.json`. Experimental tables hold only the re-run assets.

**Code:** `clickhouse-tables`: metric specs in `daily_metrics/specs.d/metrics/*.yaml` (sources, `dependsOn`,
lookback), job functions in `daily_metrics/job_functions/`, the dependency graph via
`daily_metrics/export_dependency_graph.py`. Runs are Airflow DAGs (`santiment-airflow-runs-analysis` skill).
Translate ids with `metric_metadata` / `asset_metadata` (`santiment-clickhouse-metrics-navigation` skill).

**First, sort the failing groups by metric family**, using the spec's source table: transfer-based, balance-based,
stack-based, price-based, derived (ratios, z-scores, MVRV). A cause usually hits a family, not single metrics; a
family that is equal while another fails points at the shared source of the failing one.

**Third references:**
- the source tables the metric is computed from (transfers, balances, stacks, prices of the chain), recomputed for
  one asset and day;
- the metric's definition: a value that is impossible (negative ages, ratios > 1, percent > 100) is wrong on that
  side whatever the other says;
- the price tables (`intraday_metrics` `price_usd`, experimental or not, as the run used them).

**Typical shapes:**
- `missing_in_*` with `zero` ≈ `keys` before an asset's chain data exists → zero padding.
- every group, 1–3 days, then a 1y/30d/… tail on lookback metrics → source gap; match the tail to the lookback.
- a few assets `large` on every day up to a fixed date → contract switch.
- an asset failing on everything → asset reference mismatch.
- a USD/P&L family `large` and one-directional, or missing in launch months → job ordering; compare the write times
  of the dependency and the consumer (part `modification_time`, Airflow task end times).

**Cautions:**
- A metric is only as right as its inputs: before blaming a metric job, check that its source tables agree.
- Check which tables the experimental run read (experimental or live sources, e.g. prices); the config of the run
  is in the DAG run's conf.
- One group-day file can be large; read `groups` with `jq`, not whole.
