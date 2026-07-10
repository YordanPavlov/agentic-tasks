# Genesis / pre-price cohort valued at $0 — acquisition-price ASOF defect (PR #2132)

> After PR #2132 switched the age-distribution acquisition-price join from
> `INNER` to `LEFT ASOF`, cohorts acquired **before any price exists** (e.g. the
> ETH genesis/premine cohort) are no longer dropped — but they are filled with
> `acquisition_price = 0` instead of being excluded, so profit-weighted metrics
> book **100% of their current value as profit**. This inflates
> `network_profit_loss` and the profit/dollar-days/realized-cap family. A
> valuation bug hiding inside a data-loss fix.
>
> **Author:** Yordan + Claude · **Started:** 2026-07-08
> **Origin:** spun out of [`../2026-07-metrics-regression-guard/`](../2026-07-metrics-regression-guard/metrics-regression-guard.md) §11.4 — the regression guard surfaced it as a HEAD-vs-served divergence on ETH.
> **Status:** NOT STARTED on the fix (analysis hardened 2026-07-10 — see §8: mechanism proven, cohort refined, blast radius narrowed; guard validation hook in place)

---

## 0. TL;DR

- **Symptom:** ETH `network_profit_loss` for 2016 recomputes to **559.6M** under
  current code vs **281.1M** served. The entire +278.5M is one cohort.
- **The cohort:** every inflated row has acquisitionTime `2015-07-30 15:26:13` —
  the **Ethereum genesis block** (the ~72M presale/premine ETH). ETH had **no
  market price at genesis**; the earliest price on record is `2015-08-07 14:45:00`
  ($2.83), and there are **0** price rows at/before genesis.
- **Root cause:** `age_distribution_batches_intraday_job.py` (and its non-batches
  sibling) compute each cohort's `acquisition_price` via a `LEFT ASOF JOIN`
  against the historical price grid. The genesis cohort has **no previous price
  at all**, so ASOF leaves it **unmatched**. `join_use_nulls` is not set anywhere
  in `daily_metrics/`, so ClickHouse's default (`0`) fills unmatched join rows
  with the **type default `0`, not NULL** — the fill happens in the join, so the
  `Nullable(Float64)` column never gets NULL. `acquisition_price = 0` →
  `(current_price − 0)·−amount` books the full current value as profit.
- **Introduced by:** PR **#2132** "Use LEFT ASOF join instead of INNER" (branch
  `ageDistributionRowsDrop`, commit `d678ad9a`, merged **2026-03-02**), which
  fixed a genuine data-loss bug (INNER JOIN dropped cohorts whose acquisition
  5-min bucket had no *exact* price) but did not handle the "no price *ever*"
  edge case. Non-batches sibling fix: `055dd3d7` (2026-03-03). The INNER-JOIN
  acquisition-price mechanism itself was added earlier in `84ec2023`
  (2025-03-19).

## 1. Why this is a real defect (not just "the guard disagrees")

- `network_profit_loss` measures realized network PnL: `Σ amount·(price_now −
  acquisition_price)` over moving coins. Pricing a never-market-priced cohort at
  `0` asserts those coins were **acquired for free**, so *every dollar they are
  now worth* is counted as profit. For ETH the premine is ~72M coins — a large,
  permanent, spurious profit floor.
- Served history (**281M**, genesis excluded) is arguably the **more-correct**
  number; current code (**559M**) over-counts by ~2× in this era.
- Live in production since #2132 merged (**2026-03-02**) for any recompute of the
  affected metrics. Served values for old periods predate #2132 and still carry
  the old (excluded) methodology → this is *also* a source of non-reproducible
  "fossils" (the guard's motivating problem).

## 2. Decomposition (evidence, ETH asset_id 1681, year 2016)

Recomputing NPL on the guard seam, split by acquisition_price:

| partition | 2016 NPL sum |
|---|---|
| `acquisition_price > 0` | **281.06M** — matches served exactly |
| `acquisition_price = 0` (all genesis, acqTime 2015-07-30) | **+278.53M** |
| total (current HEAD) | **559.59M** |

- Guard seam `acquisition_price`: 0 NULLs / 41 037 zeros (all genesis).
- Served seam: 57 053 NULLs (acq-year 2015) / 0 zeros — same cohort, stored NULL,
  excluded by `sum()`.

## 3. Blast radius (to confirm)

- **Metrics:** everything reading `acquisition_price` from `distribution_deltas_5min`
  — `network_profit_loss` (+ `_change_1d/7d/30d`), `transaction_volume_profit` /
  `_loss` / `_ratio`, `stack_mean_age_dollar_days_*`, realized-cap / MRP / MVRV
  variants that are price-weighted. (The guard's §10 "moderate" tier is likely
  partly this too.)
- **Chains:** any chain with coins acquired before its first recorded price —
  i.e. **premine / genesis-allocation chains and any asset whose stack history
  predates its price history**. ETH (premine) confirmed. **XRP** is the original
  fossil case ([`../2026-06-odt-bucketing-xrp/`](../2026-06-odt-bucketing-xrp/verifying-the-batched-odt-migration.md)) — re-verify whether its price-family fossils share this exact mechanism. BTC (2009 genesis, first price much later) is a strong suspect. Enumerate per chain: count of rows with `acquisition_price = 0` whose acquisitionTime precedes the first price.

## 4. Proposed fix (options — needs product decision)

1. **`SETTINGS join_use_nulls = 1`** on the ASOF join in
   `age_distribution_batches_intraday_job.py` + `age_distribution_intraday_job.py`.
   Unmatched → NULL → excluded from downstream `sum()`s (NULL propagates). Cleanest;
   coincidentally reproduces served 281M for ETH. Verify no downstream code relies
   on `acquisition_price` being non-null (e.g. arithmetic that would turn NULL).
2. **Explicit exclusion** — `WHERE acquisition_price > 0` (or `IS NOT NULL`) in the
   profit-family consumers. More surgical but must be applied in every consumer.
3. **Impute a genesis price** — assign the presale price (~$0.30 for ETH) or the
   first market price to pre-price cohorts. A product/methodology choice, not a
   pure bug fix; changes the meaning of "profit on premine."

**The product call:** what *is* the realized profit on premined/genesis coins
that were never bought at a market price? Exclude (options 1/2) or impute
(option 3)? This determines the fix and whether historical values must change.

## 5. Validation plan

- Unit/local: recompute NPL for ETH 2016 with the fix; expect genesis cohort
  excluded → ~281M (matches served) under options 1/2.
- Cross-chain: run the §3 enumeration; recompute one premine chain + BTC.
- Regression guard: once fixed, the guard's HEAD recompute of NPL should match
  served for reproducible periods — i.e. this defect is exactly the kind of thing
  the guard is meant to keep from recurring. Re-record the guard baseline for the
  price-weighted family *after* this fix lands (see guard task §11.4).

## 6. Decisions / trade-offs to resolve

- Fix option (1 vs 2 vs 3) — product + eng.
- Backfill: do we recompute affected historical metrics after the fix (large), or
  only fix forward and accept the pre-fix window as documented fossils?
- Scope: ETH-only first, or sweep all premine/pre-price chains in one pass.

## 7. Key references

- Guard task journal §11.4 (full derivation): [`../2026-07-metrics-regression-guard/metrics-regression-guard.md`](../2026-07-metrics-regression-guard/metrics-regression-guard.md)
- PR #2132: https://github.com/santiment/clickhouse-tables/pull/2132 (`d678ad9a`, merged 2026-03-02)
- Commits: `84ec2023` (INNER introduced, 2025-03-19), `055dd3d7` (non-batches LEFT-ASOF fix, 2026-03-03)
- Code: `daily_metrics/jobs/age_distribution_batches_intraday_job.py`, `age_distribution_intraday_job.py` (the `LEFT ASOF JOIN` on `intraday_metrics_historic_optimization`); `network_profit_loss_job.py`, `transaction_volume_profit_loss_job.py` (consumers).
- XRP fossil context: [`../2026-06-odt-bucketing-xrp/`](../2026-06-odt-bucketing-xrp/verifying-the-batched-odt-migration.md)

---

## 8. Update 2026-07-10 — clean guard re-run: mechanism proven, cohort refined, blast radius narrowed

The guard task's clean ETH re-run (53 jobs, prices computed in-run; see guard journal §12.6) re-measured everything with the ordering artifact eliminated. Three consequences for THIS task:

### 8.1 The affected cohort is the whole PRE-FIRST-PRICE window, not only the genesis timestamp

Fresh seam decomposition (`distribution_deltas_5min_guard`, metric 162, movers in 2016):

| partition | rows | acquisitionTime range | ETH moved (2016) |
|---|---|---|---|
| `acquisition_price > 0` | 27 466 874 | 2015-08-07 14:45:18 → … | 1.347B |
| `acquisition_price = 0` | 64 258 | **2015-07-30 15:26:13 → 2015-08-07 14:44:57** | 57.2M |

The zero-cohort spans genesis THROUGH the last pre-price 5-min bucket — the boundary is exactly the first price tick (2015-08-07 14:45). §0/§2's "every row is the genesis timestamp" was the dominant subset, not the full set: week-one on-chain activity (moves/contract funding before any market price) is equally affected. Same mechanism (ASOF has no earlier price → unmatched → 0-fill); slightly larger backfill scope. The §3 per-chain enumeration predicate stays valid: `acquisition_price = 0 AND acquisitionTime < first_price_time`.

### 8.2 Mechanism PROVEN by profit/loss asymmetry (strongest single piece of evidence yet)

Clean-run fossil diff (guard vs served, 2016): `transaction_volume_profit` **+8.0%** (355.6M vs 327.0M), `_ratio` **+10.4%**, NPL **+99%** (559.6M vs 281.1M), NPL changes 1d/7d/30d sign-flip — while **`transaction_volume_loss` REPRODUCES served at 7e-5**. A $0-acquisition cohort can satisfy `current_price > acquisition_price` but never `current_price < acquisition_price` — only profit-side metrics inflate. No alternative explanation produces that one-sided signature.

### 8.3 Blast radius NARROWED: price×amount-weighted metrics are NOT affected

§3 listed `stack_mean_age_dollar_days_*` / realized-cap / MRP / MVRV as suspects. The clean diff shows their divergences are the **2022-03 hourly→5-min acquisition-grid methodology fossil**, not this defect: for sum-style metrics a `0 × amount` contribution is numerically identical to a NULL-skipped row, so the 0-fill is invisible there. **Defect scope = comparison-style profit metrics only**: `network_profit_loss` (+`_change_1d/7d/30d`), `transaction_volume_profit`, `transaction_volume_profit_loss_ratio`. (`transaction_volume_loss` structurally immune, above.) This shrinks both the fix's consumer list for option 2 and the backfill scope.

### 8.4 Post-fix validation hook is now in place

The guard's ETH baseline was blessed 2026-07-10 (clickhouse-tables `bcd949bd`, 531 metrics) with exactly these **6 metrics HELD OUT**: `network_profit_loss`, `network_profit_loss_change_1d/7d/30d`, `transaction_volume_profit`, `transaction_volume_profit_loss_ratio`. Post-fix procedure: guard recompute (prices now topo-ordered in-run via the dependsOn fix, commit `184d7eac`) → NPL should land ~281M (matching served, options 1/2) → re-record those 6 from `*_guard` → re-add to the baseline. The guard then permanently pins the fixed semantics.
