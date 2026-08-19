# stack_circulation_100y — cancel-free total supply

**Started:** 2026-07-27
**Repo:** `clickhouse-tables` (branch `stackCirculation100`)
**PR:** [#2308](https://github.com/santiment/clickhouse-tables/pull/2308) — introduce `stack_circulation_100y`, root `total_supply` on it

## What this task IS

`stack_circulation_20y` doubles as the total-supply proxy
(`total_supply = coalesce(custom_total_supply, stack_circulation_20y)`), but
`circulation_job.compute_delta_cancels` books an aging-out cancel at
`odt + period` into the delta-futures tables for every delta. In the supply
role those cancels are meant to never fire, yet all are persisted — and they
*do* eventually fire, breaking the proxy. This task introduces
`stack_circulation_100y` computed with **no future cancels at all**, re-roots
`total_supply` (and later the other supply-intent consumers) on it, and paves
the way to deprecating the 20y family.

Spun out of the parked [ltc-stacks-deprecation](../2026-06-ltc-stacks-deprecation/ltc-stacks-deprecation.md)
task (findings F4/F6 context only — none of that task's work is in scope here).

## Decisions

- **Balances-based total supply (`total_supply_from_balances`) DECLINED as a
  concept** (operator, 2026-07-27). Not worked on; supply stays in the stacks
  family.
- **No per-metric `futureCancels` flag** (operator: configurable = misuse
  surface). Instead a hard rule in `circulation_job`:
  `INFINITE_WINDOW_PERIOD_DAYS = 36500`; periods at/above it are an infinite
  window and are excluded from `compute_delta_cancels` entirely. The deltas
  side needs no change (`dt − odt < period` is always true at 100y; the
  futures read finds zero rows).
- `stack_circulation_delta_100y` = `period: 36525` with the standard
  `coinAgeMetric` + `circulationDelta` labels → slots into the existing
  per-chain circulation cronjobs; `stack_circulation_100y` (`sumMetric`) →
  picked up by the existing cumulative-sums cronjobs. Zero new cronjobs.

## Validated findings (prod, 2026-07-27)

- Code path: `circulation_job.py` writes `future_dt = toDate(odt + period)`
  into `daily_delta_futures`; same pattern copied in
  `circulation_intraday_job`, `realized_cap_job`, `realized_cap_intraday_job`
  (+ active-addresses/holders/creation-ts-intervals jobs, which are
  genuinely windowed).
- **Daily** `daily_delta_futures` (replicated, 5.8B rows / 36 GiB per broker):
  `stack_circulation_delta_20y` (32) = 136M rows, **99% future-dated** (to
  2046); `stack_realized_cap_usd_delta_20y` (60) = 141M rows, 99.1%.
- **Intraday** `intraday_delta_futures` (94.8B rows / 843 GiB per broker):
  `stack_circulation_intraday_delta_20y` (348) = 4.82B rows and
  `stack_realized_cap_usd_intraday_delta_20y` (311) = 4.56B rows — both
  **100% future-dated**, zero ever consumed; ~92 GiB per broker ≈ 285 GiB
  cluster-wide for the 20y pair alone.
- **Correctness cliff:** first 20y cancels land in-range **2029-01-09 for
  BTC and BCH** (LTC 2031-10, XRP 2032-12, DOGE 2033-12, ETH 2035-07). From
  that day `circ_20y` — and `total_supply`, marketcap, dominance, inflation,
  S2F, NVT — silently decays as old coins age out of the 20y window.
- Anomaly noted for the equality check: daily 20y partition holds cancels
  with `future_dt` as early as 2009 ⇒ some rows carry pre-genesis garbage
  `odt` (≈1989). Coins "older" than 20y from garbage odt are excluded by
  `circ_20y` but included by `circ_100y` — any equality diff is that
  pre-existing garbage, not methodology.
- Consumer inventory of `circ_20y`-as-supply (to re-root later):
  `total_supply`, `stack_mean_creation_timestamp`, `stack_total_age`,
  `standard_deviation_of_coin_age`, `percent_of_total_supply_in_profit`,
  `percent_of_total_supply_on_exchanges`, `non_exchange_token_supply`, `nvt`,
  `nvt_transaction_volume`, `stock_to_flow_ratio`, `annual_inflation_rate`
  (uses `delta_20y`), liquity/makerdao/maple supply+marketcap aliases.
  Entanglement: `mean_realized_price_20y = rc_20y/circ_20y` (daily and
  intraday), and `rc_20y` is the all-time denominator in ~10 hodl-band
  composites — deprecating `circ_20y` drags the whole 20y family
  (circulation + realized cap, daily + intraday).

## Plan

1. **PR #2308 (done, unmerged):** infinite-window rule + 100y metric specs +
   `total_supply` → `circ_100y`. **Merge gate:** register metadata
   (`populate_clickhouse_metadata.py`), genesis backfill of
   delta_100y/circ_100y on all stacks chains, equality check vs `circ_20y`
   (expect bit-equal today).
2. Re-root the remaining supply-intent consumers (separate PR, value-neutral
   today; includes moving `annual_inflation_rate`/`stock_to_flow_ratio` to
   `delta_100y` — `delta_20y` starts including firing cancels in 2029).
3. Same rule + 100y twins for `realized_cap_job` and the intraday jobs.
4. Deprecate the 20y family: remove specs, drop futures partitions (daily 32
   & 60; intraday `(348,*)` & `(311,*)`) → ~285 GiB reclaim. **Prereq:**
   check sanbase for direct API exposure of the 20y metric names (outside
   clickhouse-tables).

## Confirmation queries (prod, `-u readonly`)

```sql
-- daily (seconds)
SELECT m.name, count() AS rows, countIf(f.future_dt > today()) AS future_dated,
       round(future_dated / rows * 100, 1) AS pct, max(f.future_dt) AS max_fdt
FROM daily_delta_futures f
LEFT JOIN metric_metadata m ON m.metric_id = f.metric_id
WHERE f.metric_id IN (32, 60) GROUP BY m.name;

-- intraday: same on intraday_delta_futures, metric_id IN (348, 311) (~4 min)

-- the 2029 cliff
SELECT a.name, min(f.future_dt) AS first_cancel_fires
FROM daily_delta_futures f LEFT JOIN asset_metadata a ON a.asset_id = f.asset_id
WHERE f.metric_id = 32 AND f.future_dt > today()
GROUP BY a.name ORDER BY first_cancel_fires LIMIT 5;
```

---

## Session log

### 2026-07-27 — validation, PR #2308, plan

Re-validated F4 against prod (numbers above; intraday 20y pair turned out
100% future-dated, sharper than the parent doc's estimate). Operator declined
both the balances-based supply concept and a configurable no-cancel flag →
hard `INFINITE_WINDOW_PERIOD_DAYS` rule. Shipped PR #2308 (circulation_job
rule + `stack_circulation_[delta_]100y` specs + `total_supply` re-wire;
+36/−3). Not merged — backfill gate documented in the PR body. Next: metadata
registration + stage backfill/equality run, then consumer re-root PR.

### 2026-08-19 — PR #2308 CI blocked (not a defect in this change)

The `build and push` job fails because the two new metrics are not yet in
`metric_metadata_versioned` and the August CI refactor now generates the
dependency graphs *during* the image build. Metadata registration — already the
next step here — has become a hard prerequisite for a green build, and the CI
path that would do it runs dry-run on branches. Analysis and fix options:
[ci-metric-registration-gap](../2026-08-ci-metric-registration-gap/ci-metric-registration-gap.md).
