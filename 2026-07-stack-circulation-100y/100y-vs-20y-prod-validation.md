# 100y vs 20y prod equality validation — BTC ✅, BCH ❌

**Date:** 2026-08-25
**Context:** the 100y family is deployed on prod with genesis backfill done for
BTC and BCH. Expectation: today (before the 2029 cancel cliff) every 100y
series should equal its 20y counterpart. Parent doc:
[stack-circulation-100y](stack-circulation-100y.md).

## Metrics under test (8 pairs)

| Pair | 20y id | 100y id | Table |
|---|---|---|---|
| stack_circulation | 20 | 2626 | daily_metrics_v2 |
| stack_circulation_delta | 32 | 2627 | daily_metrics_v2 |
| stack_realized_cap_usd | 48 | 2628 | daily_metrics_v2 |
| stack_realized_cap_usd_delta | 60 | 2629 | daily_metrics_v2 |
| stack_circulation_intraday | 336 | 2622 | intraday_metrics |
| stack_circulation_intraday_delta | 348 | 2623 | intraday_metrics |
| stack_realized_cap_usd_intraday | 299 | 2624 | intraday_metrics |
| stack_realized_cap_usd_intraday_delta | 311 | 2625 | intraday_metrics |

## Test method (prod, `-u readonly`)

Per pair, per `dt`, canonical values via `argMax(value, computed_at)`;
then in one aggregate: coverage per side, shared points, bit-equal count,
first/last differing dt, max abs and max relative diff. Template:

```sql
WITH pairs AS (
  SELECT dt,
    argMaxIf(value, computed_at, metric_id = {OLD}) AS v20,
    argMaxIf(value, computed_at, metric_id = {NEW}) AS v100,
    countIf(metric_id = {OLD}) AS n20, countIf(metric_id = {NEW}) AS n100
  FROM {TABLE} WHERE asset_id = {ASSET} AND metric_id IN ({OLD},{NEW})
  GROUP BY dt
)
SELECT countIf(n20>0) AS pts20, countIf(n100>0) AS pts100,
  countIf(n20>0 AND n100>0) AS both,
  countIf(n20>0 AND n100>0 AND v20=v100) AS bit_equal,
  minIf(dt, n20>0 AND n100>0 AND v20!=v100) AS first_diff,
  maxIf(dt, n20>0 AND n100>0 AND v20!=v100) AS last_diff,
  maxIf(abs(v100-v20), n20>0 AND n100>0) AS max_abs,
  maxIf(abs(v100-v20)/greatest(abs(v20),1e-9), n20>0 AND n100>0) AS max_rel
FROM pairs
```

Follow-ups where diffs appeared: yearly gap profile
(`argMax(v100-v20, dt)` per year), step detection on the cumulative gap
(`gap - lagInFrame(gap) OVER (ORDER BY dt)`, filter `abs(step) > 100`), and
raw delta sampling on suspect days.

## BTC (asset_id 1452) — PASS

All 8 pairs equal within Float64 summation-order noise; **no real
discrepancy, the known master bugfix does not surface on BTC.**

| Pair (20y → 100y) | Shared pts | Max abs diff | Max rel diff |
|---|---|---|---|
| circ daily cum | 6,438 | 2.1e-7 BTC | 1.1e-14 |
| circ daily delta | 6,438 | 1.4e-8 BTC | 1.3e-11 |
| rc daily cum | 6,438 | $0.0023 | 5.6e-15 |
| rc daily delta | 6,438 | $0.0003 | 4.2e-11 |
| circ intraday cum | 1,853,949 | 5.2e-8 BTC | 2.6e-15 |
| circ intraday delta | 1,853,949 | 6.1e-10 BTC | 6.1e-11 |
| rc intraday cum | 1,853,949 | $0.0027 | 7.3e-15 |
| rc intraday delta | 1,853,949 | $1.6e-5 | 3.4e-8 (on a $3.38 delta — small-denominator artifact) |

Noise diagnosis confirmed two ways: cumulative gap grows smoothly
1e-9 (2010) → 2e-7 (2026) — a rounding random walk, no step change; worst
relative deltas all sit on tiny denominators. Every 100y series has 8 extra
all-zero days at the front (2009-01-01..08 = backfill start vs first
spendable transfer; intraday: exactly 2,304 extra 5-min points). The task
doc's "bit-equal" expectation is literally false (different summation order)
but holds to ~1 satoshi / sub-cent.

## BCH (asset_id 2484) — FAIL

Real discrepancies in **all 8 pairs**: daily circulation gap reaches
**−997,573.6 BCH (~5% of supply)** at HEAD; realized cap up to **$11.7B
(6.4% relative)**. RC diffs start 2017-07-23 (fork/pricing start), circ
diffs on day one (2009-01-09). Entirely a backfill-history phenomenon:
live deltas are bit-identical again from ~2026-06-01 onward, so the gap is
frozen since 2026-05-09.

The cumulative circ gap (100y − 20y) decomposes into three distinct events:

1. **2009-01-09 → 2009-02-02:** 100y deltas ~5–6k/day lower, accumulating to
   exactly **−139,200**, then frozen for 12 years. Reads as the 20y history
   overcounting early pre-fork blocks, absent from the fresh recompute.
2. **2021 steps:** **−219,184 (01-14), −38,634 (01-31), −288,335 (07-30)** +
   minor ones → gap −689k. Day-specific recompute differences — candidate
   for the known master bugfix (unconfirmed which one).
3. **2024-08-16 → 2026-05-09: `delta_100y` = literal 0** nearly every day
   while `delta_20y` shows normal ~450 BCH/day issuance (clean multiples of
   the 3.125 reward until 2025-04-08, messier after). A few catch-up days
   inside the window (+42k 2025-09-02/03, +31k 2026-02-10, +13k 2025-07-28)
   partially offset; net ~−308k. **Looks like the genesis backfill produced
   no deltas for this ~21-month window** (backfill defect or source-data
   hole), not like a bugfix.

Level sanity: `circ_20y` = 20.71M BCH at HEAD — above any plausible BCH
supply (~19.9M); `circ_100y` = 19.71M — slightly below. So components 1–2
plausibly *correct* old 20y overcounting, while component 3 likely makes the
100y series **undercount by ~300k**. Interpretation confidence: high on the
data structure, medium on which side is right per component.

## Open / next

- Map the operator's known master bugfix onto components 1/2 (which days
  does it predict?).
- Investigate the 2024-08→2026-05 zero-delta window: do `bch_stacks` source
  rows exist for those dates? Separates backfill bug vs data hole. Until
  resolved, **BCH `total_supply` rooted on `circ_100y` would undercount**.
- Repeat the sweep for the remaining stacks chains (LTC, DOGE, XRP, ETH) as
  their backfills land.
