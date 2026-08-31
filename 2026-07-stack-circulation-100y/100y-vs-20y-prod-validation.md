# 100y vs 20y prod equality validation — BTC ✅, LTC ✅, BCH ❌, XRP ❌, DOGE ⚠️

**Date:** 2026-08-25 (BTC/BCH), 2026-08-27 (LTC/XRP/DOGE + root causes)
**Context:** the 100y family is deployed on prod with genesis backfill done for
BTC and BCH (then LTC/XRP/DOGE). Expectation: today (before the 2029 cancel
cliff) every 100y series should equal its 20y counterpart. Parent doc:
[stack-circulation-100y](stack-circulation-100y.md).

## Status summary (as of 2026-08-27)

| Chain | Circulation | Realized cap | Root cause / blocker |
|---|---|---|---|
| BTC | ✅ pass (noise) | ✅ pass (noise) | distribution history re-populated 2026-05-20 post-fixes — clean |
| LTC | ✅ pass (noise) | ✅ pass (noise) | one *20y-side* defect: 20y intraday cum dropped a 6.25 block 2026-05-31 (cosmetic, unfixed) |
| BCH | 🔶 intraday deltas clean post-re-run (2026-08-31); Jan-2009 source hole −139.2k remains | 🔶 deltas bit-equal; cum broken by anchor bug | daily re-run still pending; see "BCH after re-run" below |
| XRP | ❌ fail (−99.99B) | ❌ fail | circ: epoch-0 sentinel — **fixed by PR #2344 (merged, verified working on ETH re-run)**, needs genesis re-backfill; rc: NULL prices (below) |
| DOGE | ✅ pass (noise) | ❌ fail (−$21.4B) | rc: NULL acquisition_price pre-2025-04-08 — needs age-distribution history re-run, then rc_100y re-backfill |
| ETH | 🔶 100y faithful to source; −170.5k gap vs stale 20y history | 🔶 same, −$215M | post-re-run 2026-08-31: #2344 confirmed working; residual gap = 2020-era source revisions the frozen 20y never saw — see "ETH after re-run" |

Fixes shipped: **PR #2344** (odt=0 treated as infinitely old — XRP circ, ETH
pre-emptively). Fixes pending elsewhere: acquisition_price history re-run
(XRP+DOGE rc), BCH (own task), XRP live +532M drift (unowned, in Flink/sink
layer), LTC 20y intraday one-block drop (unowned, cosmetic).

### ETH detail (2026-08-27)

- **Backfill state:** 100y genesis backfill is mid-flight — coverage
  2015-07-30 (genesis) → 2018-02-07, plus a harmless pre-genesis zero-pad
  block 2013-12-08 → 2014-06-21; live 100y rows only since 2026-08-20.
  Equality gate cannot run until the backfill completes.
- **Does ETH need the odt=0 exclusion? Yes.** ETH has 68,696 epoch-0
  liability rows (−0.32M ETH cumulative, 2016-07-20 DAO-era → 2021-03-22).
  Without #2344 the 100y window includes them → circ_100y would undershoot
  circ_20y by up to ~0.32M ETH (~0.27% of supply) — small next to XRP's
  99.99% but a hard gate failure, far above float noise. With #2344 both
  windows drop the sentinel rows identically → equality restored.
- **Timing problem:** the in-flight backfill runs pre-#2344 code and has
  already passed 2016-07-20 (it's at 2018-02) — it is baking the epoch-0
  gap into history right now. After #2344 deploys, re-backfill from
  ≥ 2016-07-20 (or restart entirely).
- **rc is NOT exposed to the DOGE NULL-price problem:** ETH's pre-cutover
  distribution history (2.95B rows, written 2020-07 → 2025-12) has only
  0.011% NULL acquisition_price (319,568 rows — the pre-price-era remnant
  incl. the epoch-0 rows themselves, which #2344 excludes anyway).

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

### Flink origin of odt=0 (2026-08-27): shared overdraft/liability fallback

Traced in etherbi-flink: XRP stacks use the SAME shared account-model
machinery as ETH (`XRPStacks` → `ComputeAccountStackChangesTimeWindow` →
`HandlerOneAccountChange`); there is no chain-specific construction logic
and **no genesis seeding step anywhere in the codebase**. `odt=0` comes from
the handler's deliberate overdraft fallback
(`HandlerOneAccountChange.scala:188-208`): when a debit pops the stack empty
with remainder (account spends coins the pipeline never tracked), it pushes
a **negative "liability" segment with `ots = 0`** and emits
`sign=1, ots=0, amount=−rem`. That matches the ClickHouse fingerprint
exactly (epoch-0 rows are negative-only). The segment is a debt marker, not
an acquisition — `ots=dt` would be wrong for it; `ots=0` is Flink's sentinel
for "origin unknown / older than tracking".

- XRP: tracked history starts at ledger 32570 with 100B already distributed
  and unseeded → the fallback absorbs the entire genesis supply on first
  spends (−99.99B).
- BTC/LTC/DOGE/BCH: UTXO pipeline, full replay from block 0, mints are real
  events (`+amount @ ots=ts`, verified for BTC coinbase) → fallback never
  fires.
- **ETH: same fallback fired 68,696 times, −0.32M ETH total, 2016-07-20
  (DAO-fork era) → 2021-03-22.** Genesis premine was seeded properly (else
  72M would show). Predict: ETH circ_100y will undershoot circ_20y by
  ~0.32M ETH when its backfill lands, unless the DMF fix ships first.

So the DMF-side reading is settled: the 20y window *accidentally* interprets
the sentinel correctly (1970 = out-of-window); the infinite-window rule must
do it *explicitly* — option (b) below matches Flink's intended semantics.

### Convention analysis (2026-08-27): odt=dt seed vs odt=0 sentinel

Our odt convention for coin *creation* is `odt = dt` — verified on BTC:
every coinbase mint appears in `distribution_deltas_5min` as
`+50 @ odt = dt` at mint time. The XRP construction violates it twice:
**no seed/creation rows were ever emitted** (all 294,612 epoch-0 rows are
negative-only, −99.994B total; the 100B initial balances exist only as
implicit stacks), and the implicit stacks carry `odt = 0` instead of the
first-tracked-ledger time. Note: just relabeling the −rows to `odt = dt`
would be wrong — they'd cancel their paired +rows in every window and break
20y too.

Two convention-correct fixes:
- **(a) Mint-at-genesis (BTC-like):** seed `+100B @ dt = odt = ledger-32570
  time` upstream, outflow −rows then carry that odt. circ_100y = 100B −
  burns from day one ≈ true total supply. But recomputed 20y history changes
  shape (100B at 2013-01-01 instead of the "distribution ramp"), breaking
  the frozen-history equality gate by design. Upstream data surgery.
- **(b) Epoch-0 = infinitely old:** job-side filter change
  (`odt=0 rows never in-window`), preserves "counts on first move"
  semantics, makes 100y ≡ 20y bit-for-bit today, only needs 100y
  re-backfill. Undercounts true supply only by never-moved coins — by now
  just ~5.4M XRP (100B − 99.9946B epoch-0 outflow − burns). Pragmatic pick.

### NEW defect (both series, live): unbalanced deltas since 2025-04 cutover

Net of ALL distribution deltas per year should be ≈ −burns (and is:
cumulative −14.26M ≈ XRP fee burns through 2024). From the 2025-04 cutover
the stream goes unbalanced-positive in bursts: +12.8M (2025-04), +22.5M
(05), +22.5M (06), +60.0M (07), ~0 (08-09), then +378.6M during 2026 —
cumulative **+532M**. This is exactly `circ_100y` at HEAD (0.532B) and
exactly the 20y overshoot above max supply (100.5267B = 99.9946B epoch-0
outflow + 0.532B drift). Coins are being created from nothing —
**both 20y and 100y are inflated by +532M and growing**; circ_20y > 100B
max supply is the smoking gun. Likely +rows emitted without matching −rows
(untracked source stacks post-cutover). Needs its own investigation.

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

### Provenance of the NULL prices (2026-08-27, git + computed_at forensics)

- acquisition_price was **added to the age-distribution writers on
  2025-03-19 (`84ec2023` "Add acquisition price to distribution deltas")**,
  deployed ~2025-04-08 (= the cutover date everywhere). Rows written before
  that are NULL by construction.
- Follow-up bugfix **2026-02-27 (`d678ad9a` "Use LEFT ASOF join instead of
  INNER — Using INNER JOIN we are loosing stack records")**: between
  2025-03 and 2026-02 the price join silently DROPPED stack rows with no
  matching price. Any distribution history (re)written in that window may be
  missing records entirely — **prime candidate for the operator's "known
  master bugfix" behind the BCH 2021 steps; check BCH's distribution
  computed_at vintage** (cross-lead for the bch-sequenced-revert task).
- Vintages (computed_at of pre-cutover rows, metric 162):
  **BTC re-populated 2026-05-20** with post-fix code, 0 NULLs of 787M rows —
  which is why BTC passed both gates. **XRP: 2020-08 → 2025-04, 99.4% NULL
  (2.07B rows). DOGE: 2024-02 → 2025-04-09, 100% NULL (301M rows).**
  Neither was re-run after the feature landed. Notably XRP's distribution
  history was NOT re-derived even by the 2026-07 odt migration (stacks only).
- Remedy: re-run the age-distribution historical backfill for XRP + DOGE
  (same operation as BTC's 2026-05-20 run, mind the raced-DELETE sequencing
  issue), then re-run the 100y genesis backfills (circ for XRP needs
  PR #2344 deployed first).

## Fix PR

**PR #2344 `odt-zero-out-of-window`** (2026-08-27): `WHERE dt - odt < period
AND odt!=0` at all 8 filter sites in circulation_job, realized_cap_job +
intraday variants. Verified: 20y outputs bit-identical old vs new filter;
100y filter now reproduces 20y on XRP genesis era exactly. After deploy:
XRP circ_100y genesis re-backfill (ETH's future backfill correct from
start).

## BCH after re-run (2026-08-31 sweep)

Intraday backfill fully re-written 2026-08-28→31 (all 8 metrics, from
**2009-02-01** = first row of the re-derived distribution source, reached
HEAD). **Daily tables NOT re-run yet** (only live writes) — daily numbers
still show the old gaps by design.

Intraday verdict:
- **rc_delta: perfect** — bit-equal on all 1.86M shared points.
- **circ_delta: bit-equal from 2009-02-01 onward.** All three old gap
  components (2021 steps −550k, zero-delta window −308k) are GONE from the
  delta streams except component 1.
- **Component 1 persists as a source hole:** the re-derived distribution
  has NO rows before 2009-02-01, while old 20y history has ~139.2k BCH of
  activity over 2009-01-09..31 (≈2,784 pre-fork blocks × 50 BTC — real
  early-BTC coins). Jan-2009 metric rows are un-rewritten legacy stubs (20y
  vintage 2021-01-25, 100y vintage 2026-08-25). circ_cum carries the
  constant −139,200 offset forever. Needs source-layer backfill of
  2009-01-09..31 (upstream replay start) or an accept-decision.
- **rc_cum: cumsum ANCHOR defect at chunk boundary 2017-08-01 00:00.**
  Deltas identical, cums equal through 2017-07-31 23:55 ($621.75M), then at
  00:00: **cum_100y resets to literal 0** (wipes $621.7M; the known
  "cumsum zeroing" bug family, cf. #2345) while **cum_20y jumps +397,818
  with zero delta** (imported stale old-history anchor). Net constant gap
  −$622.14M to HEAD. Both sides need their cum jobs re-run with fixed
  anchoring (post-#2345, correctly sequenced).

## ETH after re-run (2026-08-31 sweep)

100y genesis backfill complete (4,247 days; still has the harmless
pre-genesis zero-pad 2013-12-08→2014-06-21; 13 scattered days 20y-only).

- **PR #2344 verified working in production:** the epoch-0 profile
  (−70.9k 2016 / −110.9k 2018 / −131.9k 2020, cum −317k) does NOT appear in
  the gap (2016 gap is only −1,960) — sentinel rows are being excluded.
- Remaining gap: circ −170,533 ETH (0.14%), rc −$215M (0.19%), accumulated
  almost entirely in **2020** (daily steps −5..9k ETH Feb–Jul 2020, +25,237
  on Black Thursday 2020-03-12), frozen since 2021, live matches.
- **The 100y side is faithful: stored 100y deltas == recompute from today's
  distribution source to the decimal on every step day tested; the frozen
  20y history is what disagrees with today's source.** I.e. the source's
  2020 history was revised after the 20y series was computed (vintages
  2020-07→2025-12 support this; candidate mechanism incl. the d678ad9a
  INNER-ASOF record loss and later repairs). Which side is *true* is
  undetermined — resolving likely means re-running the ETH 20y history from
  the current source (then both sides should be bit-equal), or accepting
  the 100y as the better-sourced series.

Per-metric impact evaluation for the team:
[eth-20y-stale-history-evaluation](eth-20y-stale-history-evaluation.md) —
headline: served `stack_circulation_delta_20y` is **negative on Black
Thursday 2020-03-12** (impossible; 185% error), rc_delta off up to $112M/day,
and the dollar-age family carries a **permanent +8.2-day bias** at HEAD;
level metrics ≤0.19%.

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
- **ETH**: wait for #2344 deploy, re-run the genesis backfill from
  ≥ 2016-07-20 (in-flight run is baking in the epoch-0 gap), then run the
  equality sweep. rc history is fine (0.011% NULL prices).
- **XRP live +532M unbalanced-delta drift** (since 2025-04 cutover, bursty,
  pushes circ_20y above max supply): needs its own investigation in the
  Flink→Kafka→CH path.
