# ETH: stale 20y history vs current source — impact evaluation for the team

**Date:** 2026-08-31 (sweep of the completed ETH 100y re-run)
**Parent:** [100y-vs-20y-prod-validation](100y-vs-20y-prod-validation.md)

## What happened

The ETH 100y metrics were re-backfilled from today's `distribution_deltas_5min`
(with PR #2344's odt=0 exclusion — verified working: no epoch-0 signature in
the gaps). Comparing them against the frozen 20y history exposes that **the
distribution source's 2016–2021 history was revised after the 20y series was
computed**, most heavily in Feb–Jul 2020. On every diff day tested, the 100y
value equals a fresh recompute from today's source *to the decimal*; the
served 20y value is the outlier. The causal chain is now established from
`computed_at` forensics (see "Causality", 2026-08-31): the served values are
faithful 2020-12 snapshots of the then-source; the source was revised
wholesale by a 2025-12-05→07 historical re-population; that re-population ran
the buggy INNER price join later fixed by `d678ad9a`, which left a separate,
still-live scar (stale genesis-era keys) in today's source.

The discrepancy is **frozen**: live values agree since ~mid-2021; the
damage is confined to served history.

## Which served metrics are affected, ranked

| # | Metric (DMF name) | Damage | Worst case |
|---|---|---|---|
| 1 | `stack_circulation_delta_20y` | 299 days wrong in 2020, 168 in 2021 | **2020-03-12 (Black Thursday): served −11,606 ETH — negative, physically impossible; truth +13,631 (185% error, wrong sign)**; 13 more days off by 38–62% |
| 2 | `stack_realized_cap_usd_delta_20y` | same day-profile | 2021-04-15: off by $112M (21%); 2021-05-28: served $25.3M vs truth $2.9M (7.8×) |
| 3 | `stack_mean_age_dollar_days` family (sanbase "mean dollar invested age") | **permanent +8.2-day bias at HEAD** — the composite multiplies rc_delta by an epoch-day factor and cumulates, so the 2020–21 errors never wash out (Σ day×error = −3.13e12 dollar-days ÷ rc $381B) | bias visible as a level shift today, and as spikes in diff views on the 2020–21 error days |
| 4 | Level consumers of `stack_circulation_20y` (total_supply, NVT, velocity, mean_age denominators, maple/liquity/makerdao composites) | constant −170,533 ETH (served 0.14% too high) since 2021 | uniform, minor |
| 5 | Level consumers of `stack_realized_cap_usd_20y` (realized cap, realized price / MVRV inputs, `mean_realized_price_usd_20y`, percent-change formulas) | −$215M peak (served ≤0.19% too high) | uniform, minor |

(Ranked by user-visible severity: #1–2 are served history that is grossly
wrong on specific famous days; #3 is a live, ongoing bias; #4–5 are within
noise for most consumers.)

## Evidence highlights

- Worst circ_delta days (served vs fresh): 2020-03-12 −11,606 vs +13,631;
  2020-02-24 +21,986 vs +13,535; 2020-07-25 +20,231 vs +13,415; the "truth"
  side is a smooth ~13.4–13.7k/day issuance curve, the served side swings —
  consistent with the old source having had duplicated/dropped records
  around high-load days.
- Yearly circ gap (100y−20y): −1,960 (2016) → −4,111 (2019) → −167,341
  (2020), frozen at −170,533 since 2021.
- Affected days (|Δ| > 1 ETH): 13 (2016), 8 (2017), 23 (2018), 27 (2019),
  **299 (2020), 168 (2021)**, 0 since.

## Causality (2026-08-31): who wrote what, and where `d678ad9a` fits

All from prod `computed_at` forensics (queries below run against
`daily_metrics_v2`, `distribution_deltas_5min` metric 162,
`intraday_metrics_historic_optimization` price_usd, asset_id 1681).

### 1. The served wrong values are 2020-vintage — NOT produced by the join bug

The served Black Thursday row is a fossil of the original live-era backfill:

```
metric 32 (circ_delta_20y)  2020-03-12  −11,606.19   computed_at 2020-12-16 17:37
metric 60 (rc_delta_20y)    2020-03-12  −150.06M     computed_at 2020-12-16 17:35
```

That predates the acquisition-price join (`84ec2023`, 2025-03-19) by over
four years — the INNER-join code **cannot** have produced these values. They
are faithful snapshots of what `distribution_deltas_5min` contained in
Dec 2020. Vintage map of the served 20y delta series (dt 2020–2021): almost
all rows are 2020-12 → 2022-02 live vintages; a single re-run on
**2025-12-17** re-wrote only **dt 2021-06-28 → 2021-12-31** (187 days). That
boundary is exactly why "live agrees since ~mid-2021": days from 2021-06-28 on
were recomputed *after* the source revision below; days before it were never
recomputed at all. (The cum metrics 20/48 were re-written 2024-09 and 2026-02,
but cumsum re-runs re-root from the same stale deltas — no healing.)
Confidence: high (direct row evidence).

### 2. What revised the source: the 2025-12-05→07 re-population

For dt < 2022, `distribution_deltas_5min` today is:

- **1,500,676,419 rows written 2025-12-05 10:50 → 2025-12-07 12:31** — a full
  age-distribution historical re-population covering dt 2015-08-07 14:45 →
  2021-12-31; this is the revision that moved Black Thursday by +25,237;
- **~314k stale rows** (vintages 2020-07 → 2022-02) the re-population did
  not overwrite — see §3.

Served 20y (2020-12) predates this run; the 100y backfill and any fresh
recompute (2026-08) postdate it. On 2020-03-12 the arithmetic closes exactly:
fresh +13,631.19 = +17,391.89 (2025-vintage keys) − 3,845.50 (stale keys)
+ 84.79 (stale epoch-0 keys, excluded by the window filter). The gap the doc
reports is precisely "2025 re-derivation minus 2020 derivation" at
price-era keys. Confidence: high.

### 3. Where `d678ad9a` fits: the INNER join dropped un-priceable keys — a scar still in today's source

The 2025-12 run executed the `84ec2023`-era writer
(`age_distribution_batches_intraday_job.py`), whose price attachment was
`INNER JOIN prices USING (asset_id, acquisitionTime)` — an **exact-equality
match against the 5-min price grid**. Any (dt, odt) key whose
`toStartOfFiveMinute(odt)` bucket has **no price row** was silently dropped
from the write. `d678ad9a` (2026-02-27) replaced it with
`LEFT ASOF JOIN ... ON acquisitionTime >= prices.acquisitionTime`: price
resolution became "most recent price at-or-before acquisition", so a missing
grid point no longer drops the record (and un-priceable records survive with
NULL price instead of vanishing).

Why a dropped key ⇒ a *stale* key: the writer's self-union re-emits every
existing row of the interval at measure 0, so a re-population rewrites
**every** pre-existing key — the only code path that leaves a key
un-rewritten is the price join dropping it. And on this ReplacingMergeTree
(key `asset_id, metric_id, dt, value`), a key that isn't rewritten keeps
serving its **old vintage** measure and acquisition_price.

Fingerprints (all verified on prod):

- ETH's price grid starts **2015-08-07 14:45** — and the earliest dt written
  by the 2025-12 run is **exactly 2015-08-07 14:45:00**. The entire
  pre-price-era slice of history (dt 2015-07-30 → 08-07) was un-writable by
  the INNER code and survives only as 2020-07/2020-12 vintages.
- Every one of the 79 stale keys on 2020-03-12 sits in a bucket with **zero**
  price rows: 54 epoch-0 sentinels (odt=1970 — never has a price) and 25
  genesis-era odts (2015-07-30 15:26 = genesis block, → 08-04).
- Era-wide, the stale keys fall in **exactly** those two classes — epoch-0
  and pre-2015-08-07 odts; no third class, no mid-history price-gap drops:

| dt year | keys total | stale | of which epoch-0 | stale net, genesis-era odt (ETH) | stale net, epoch-0 (ETH) |
|---|---|---|---|---|---|
| 2016 | 23.1M | 57,054 | 1 | −28,604,027 | −70,919 |
| 2017 | 119.9M | 31,311 | 1,813 | −6,294,164 | −59 |
| 2018 | 320.2M | 33,109 | 23,687 | −2,372,625 | −110,926 |
| 2019 | 307.6M | 3,602 | 1,039 | −1,669,269 | −1,244 |
| 2020 | 317.9M | 50,809 | 48,974 | −645,124 | −131,920 |
| 2021 | 409.1M | 7,019 | 3,701 | −382,927 | −1,944 |

(The epoch-0 column reproduces the known −317k epoch-0 profile exactly —
i.e. **no** epoch-0 row was rewritten in 2025: the INNER join structurally
dropped all of them.) Confidence: high — the self-union argument plus the
two-class partition and the first-price-row boundary leave no alternative
mechanism.

**No ETH distribution write for dt < 2022 has happened since 2025-12-07**, so
the post-fix code has never been applied to this history. Consequences, live
today:

- The negative legs of genesis-coin movements (odt = genesis era) are frozen
  2020-vintage measures, while their positive legs (odt = move time, priced)
  were re-derived in 2025 — today's source mixes two derivations within the
  same movement pairs. On Black Thursday, −3,760 of the +13,631 "truth"
  (28%) rides on stale rows. In aggregate the two derivations agree well
  (2016 yearly gap −1,960 on −28.6M of stale genesis flow ≈ 0.007%), so the
  practical distortion is small — but stored 100y and every fresh recompute
  inherit it, and its true magnitude is unknowable until a post-fix re-run.
- All stale keys carry **NULL acquisition_price**, so genesis-era negative
  legs contribute $0 to rc. (Post-fix this stays NULL by design — no price
  exists at-or-before a pre-2015-08-07 acquisition — so rc semantics for
  genesis coins are "cost basis $0" either way; the fix changes record
  survival and pricing of *price-era* keys, not this.)

### 4. What the fix demonstrably fixed

Post-`d678ad9a` re-populations write complete key sets with ASOF-resolved
prices: BTC's 2026-05-20 full re-population produced 0 NULL prices across
787M rows and passed both 100y equality gates (see parent doc). That is the
"after" picture ETH's 2016–2021 history never got.

### Causal chain, one paragraph

2020-12: 20y metrics computed from the then-current source → those rows still
serve today (never recomputed for dt < 2021-06-28). 2025-12-05→07: the
age-distribution history was re-derived (the revision that exposes the 20y as
stale), but with the INNER equality price join, which silently dropped every
epoch-0 and genesis-era-odt key — leaving ~183k stale 2020-vintage,
NULL-priced keys inside today's source. 2026-02-27: `d678ad9a` fixed price
resolution (LEFT ASOF), ending record loss for all future writes — but ETH's
pre-2022 history hasn't been re-written since, so both the 20y staleness
(metric layer) and the INNER-join scar (source layer) are still being served.

## Options

1. **Re-run the ETH 20y history** from the current source (the standard
   historical DAG, post-#2344 code). Both families then agree bit-for-bit;
   all downstream composites (incl. dollar-age) heal on their next
   recompute. Cost: one full ETH intraday+daily historical run + composite
   recomputes. Risk: the same cumsum-anchor pitfalls seen on BCH — needs
   correct sequencing. **Amendment (per Causality §3): re-run the
   age-distribution history for dt < 2022 with the post-`d678ad9a` code
   FIRST.** A metric-only re-run would bake the ~183k stale INNER-join keys
   (frozen 2020 measures, NULL prices) into the "healed" series; a source
   re-run heals them, restores the missing dt < 2015-08-07 slice, and will
   also move the 100y series slightly — so re-root/re-run *both* families
   after it.
2. **Accept the 100y as canonical** and fold this into the #2308 switchover
   (retire 20y sooner for ETH). No extra compute, but the served 20y
   history stays wrong until the switch, and delta-based composites keep
   the baked-in errors until *their* deps are re-rooted and re-run.
3. **Do nothing** — served history keeps a sign-flipped Black Thursday
   circulation delta and a +8-day dollar-age bias. Not recommended.

Open question for the team: do we trust today's source as *more* correct
than the 2020 vintage? Mostly answered now:

- The served side is **provably wrong on sign**: in 2020 nothing can age out
  of a 20y window, so a daily circulation delta cannot be negative — yet
  2020-12-16's computation produced −11,606.
- The fresh side matches protocol issuance arithmetic: ~6,500 blocks/day ×
  2 ETH + uncle rewards ≈ 13.1–13.7k ETH/day, exactly the smooth curve the
  2025 re-derivation produced.
- Residual caveat (Causality §3): today's source is not a *pure* 2025
  re-derivation — genesis-era-odt keys still carry 2020-vintage measures the
  INNER join couldn't rewrite (small in aggregate, ~0.01% on genesis flows,
  but unquantifiable per-day until a post-fix source re-run).

An independent spot-check of one 2020 day against raw chain data would still
be a nice belt-and-braces confirmation, but the sign-impossibility argument
alone settles which side is broken.
