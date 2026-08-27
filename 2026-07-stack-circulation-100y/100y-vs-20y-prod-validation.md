# 100y vs 20y prod equality validation — BTC ✅, LTC ✅, BCH ❌, XRP ❌, DOGE ⚠️

**Date:** 2026-08-25 (BTC/BCH), 2026-08-27 (LTC/XRP/DOGE)
**Context:** the 100y family is deployed on prod with genesis backfill done for
BTC and BCH (then LTC/XRP/DOGE). Expectation: today (before the 2029 cancel
cliff) every 100y series should equal its 20y counterpart. Parent doc:
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

## LTC (asset_id 2462) — PASS

All 8 pairs equal at BTC-like noise levels. Daily: max abs 4.4e-6 LTC /
$1.4e-4 (rel ≤ 1e-9, cum rel ~1e-14). Intraday deltas: max 3.5e-8 LTC.
Head padding differs harmlessly: 20y daily has 22 extra all-zero days
(2011-09-16..10-07, pre-genesis; LTC genesis 2011-10-08), intraday 20y has
3,456 extra pre-genesis points (2011-09-26..10-07).

One caveat, and it's a **20y-side defect**: since **2026-05-31 10:15** the
intraday cum pair carries a constant **+6.25 LTC (one block reward)** offset
(100y higher), persisting to HEAD (~25.3k points, rel 8e-8). At the
transition bucket both delta series agree (+25.0) but the 20y cum advanced
only +18.75 while the 100y cum advanced +25 — i.e. the **20y intraday cum is
internally inconsistent with its own delta stream**; the 100y series is
self-consistent. Daily pairs never show it (gap frozen at −2.3e-6). Verdict:
100y LTC is good; the 20y intraday cum silently dropped one block.

## XRP (asset_id 2053) — FAIL (catastrophic, 100y unusable)

All 8 pairs wildly different. At HEAD: `circ_20y` = **100.53B** XRP vs
`circ_100y` = **0.532B** — the 100y series is missing ~99.99B, i.e. the
entire genesis allocation (gap −99.994B, rel ~1.0). RC: $94.76B (20y) vs
$25.89B (100y). Structure:

- 100y early values are **tiny negatives** (−0.0002 → −0.0036 over
  2013-01-02..10) — impossible for a circulation metric; the fresh recompute
  spends from genesis-era accounts it never credited.
- The 20y side absorbs the genesis-era allocations as big steps the 100y side
  lacks: **+85.81B on 2013-01-26**, +6.0B 02-06, +1.5B 02-07, +2.0B 08-01,
  +1.99B 12-19; gap essentially saturated at −99.5B by 2018, −99.99B now.
- 100y has 28 zero rows 2011-11-05..12-02 (pre-ledger padding, harmless) and
  ~360 missing days mid-series (not yet profiled).
- Level sanity: `circ_20y` = 100.53B slightly *exceeds* the 100B max supply,
  so the 20y side overcounts ~0.5B too — but the 100y side is the unusable
  one.

### Root cause (2026-08-27, CONFIRMED): epoch-0 odt sentinel vs 100y window

Circulation reads `distribution_deltas_5min` (metric 162,
`value`=odt, `measure`=amount) via `circulation_job.py` with filter
**`WHERE dt - odt < period`**. Genesis/unknown-origin coins carry
**`odt = 0` (1970-01-01)** — a sentinel meaning "older than history"
(XRP ledgers before 32570 are lost; balances pre-exist). When such coins
move, the pipeline emits `+amount @ odt=dt` and `−amount @ odt=epoch0`.

- **20y**: `dt − 1970 ≈ 43y+ ≥ 20y` → epoch-0 negative row excluded →
  the move counts as old coins entering circulation. Correct by accident.
- **100y**: `43y < 100y` → epoch-0 negative row **included** → cancels the
  positive row → **the move nets to 0 forever** (until 2070). The tiny
  negative values are float residue of the cancelling pairs.

Proof: on 2013-01-26 the distribution layer holds +85,810,081,001 at fresh
odt vs −85,810,000,500 at epoch-0; recomputing monthly deltas from the
current source with the 20y filter reproduces the stored 20y series exactly,
with the 100y filter gives ~0. **Yearly epoch-0 outflow matches the circ gap
year-by-year to float noise** (2013 −98.656B … 2026 cum −99.9946B ≡ gap).
Not missing source data; not a backfill defect; ongoing live (~kXRP/day).
Fix is semantic: treat `odt = 0` as infinitely old (out-of-window for ANY
period) in circulation/realized-cap jobs (+ intraday variants), or re-assign
genesis odt upstream — ties into the open XRP genesis product call.

### Root cause for XRP realized cap: NULL acquisition_price (shared w/ DOGE)

The epoch-0 rows contribute **$0** to rc (their `acquisition_price` is
NULL). The rc gap is instead the NULL-price hole below: XRP
`acquisition_price` is **all-NULL every day 2012 → ~2025-04**; rc_100y
pre-2025 history is zero, the $25.9B at HEAD accumulated purely since the
2025-04 cutover.

## DOGE (asset_id 2695) — circulation PASS, realized cap FAIL

Circulation (daily + intraday, cum + delta): matches to noise — max abs 23.7
DOGE on a ~150B supply (rel 1.5e-10); worst delta diff 7.47 DOGE (rel 5.5e-7,
small denominators). Fine.

Realized cap: real divergence, up to **$25.1B / 116% relative**, starting
2013-12-15. Since circulation matches, this is a *valuation-of-history*
difference (price applied at move time), not coin movement. Gap profile
(100y − 20y): drifts −8.5M (2013) → −353M (2018), then explodes during the
2021 DOGE mania: −$2.6B (04-16), −$1.08B (04-17), −$1.46B (05-04), −$1.59B
(05-07), **−$9.03B (10-29)**; partial reversals +$2.97B/+$3.44B (2022-07-19/21);
more steps −$1.17B (2024-11-12), −$1.11B (2024-12-16). **Frozen at −$21.45B
since 2025-07-02**; live rc deltas match since.

### Root cause (2026-08-27, CONFIRMED): NULL acquisition_price pre-cutover

`distribution_deltas_5min.acquisition_price` is **NULL on every row of every
day** of DOGE history until the stacks-pipeline cutover: first non-NULL
price = **2025-04-08** (the same cutover date seen in the BCH analysis).
`realized_cap_job.py` computes `sum(amount * acquisition_price)` →
`amount × NULL` = NULL → sum skips it → the 100y genesis backfill wrote
**rc_delta_100y = 0 on all 4,132 days before 2025-04** (verified: zero
nonzero days). The frozen 20y history was computed years ago from the old,
price-bearing table state and is the correct side. The "steps" in the gap
are just mirror images of rc_20y's own daily deltas. Amounts (`measure`)
are intact → circulation unaffected. DOGE has 3.45M epoch-0 rows but they
net to −24 DOGE / $0 — no XRP-style problem.

Consequence: **any recompute of pre-2025-04 realized cap from the current
distribution table produces zeros** — this endangers not just 100y but any
future re-backfill of the 20y rc family too. Needs acquisition_price
backfilled into the distribution history (upstream fix), then rc_100y
re-backfill.

## Open / next

- Map the operator's known master bugfix onto BCH components 1/2 (which days
  does it predict?). Does it also predict the DOGE 2021 rc steps?
- Investigate the BCH 2024-08→2026-05 zero-delta window: do `bch_stacks`
  source rows exist for those dates? Separates backfill bug vs data hole.
  Until resolved, **BCH `total_supply` rooted on `circ_100y` would
  undercount**.
- **XRP circ: decide epoch-0 semantics** — treat `odt = 0` as infinitely old
  in the window filter (circulation_job, realized_cap_job + intraday
  variants), or re-assign genesis odt upstream; then re-backfill circ_100y.
  Part of the open genesis product call. Also profile the ~360 missing
  mid-series days.
- **XRP + DOGE rc: backfill acquisition_price** in `distribution_deltas_5min`
  pre-2025-04-08 history (all-NULL for both chains), then re-backfill
  rc_100y. Check remaining chains for the same hole (BTC/LTC rc passed, so
  their tables still carry prices; check BCH and future chains). Until then
  rc_100y for XRP/DOGE is unusable pre-2025-04 and any 20y rc recompute
  would be silently zeroed too.
- **LTC 20y intraday cum dropped one block at 2026-05-31 10:15** (internally
  inconsistent with its own deltas) — cosmetic (8e-8) but a live-pipeline
  defect worth a look; 100y is clean.
- Repeat the sweep for remaining stacks chains (ETH) as backfills land.
