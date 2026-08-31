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
served 20y value is the outlier. Candidate mechanism: the acquisition-price
join era (`84ec2023`, 2025-03-19) and the `d678ad9a` fix (2026-02-27,
"INNER ASOF join was losing stack records"), plus re-population vintages
2020-07 → 2025-12.

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

## Options

1. **Re-run the ETH 20y history** from the current source (the standard
   historical DAG, post-#2344 code). Both families then agree bit-for-bit;
   all downstream composites (incl. dollar-age) heal on their next
   recompute. Cost: one full ETH intraday+daily historical run + composite
   recomputes. Risk: the same cumsum-anchor pitfalls seen on BCH — needs
   correct sequencing.
2. **Accept the 100y as canonical** and fold this into the #2308 switchover
   (retire 20y sooner for ETH). No extra compute, but the served 20y
   history stays wrong until the switch, and delta-based composites keep
   the baked-in errors until *their* deps are re-rooted and re-run.
3. **Do nothing** — served history keeps a sign-flipped Black Thursday
   circulation delta and a +8-day dollar-age bias. Not recommended.

Open question for the team: do we trust today's source as *more* correct
than the 2020 vintage? The smooth issuance curve on the fresh side strongly
suggests yes, but an independent spot-check of a 2020 day against raw chain
data (e.g. total ETH issued on 2020-03-12) would settle it.
