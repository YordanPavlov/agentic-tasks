# Investigation log: xrp_stacks, 2026-10-13

Probes ran against the Distributed tables `default.xrp_stacks` (→ `_v8`) and `test.xrp_stacks_test` (→ `_v9`) with
`FINAL` (applied per shard; a key's rows sit on one shard). Balances: `default.xrp_balances` (→ `_v8`),
`test.xrp_balances_test` (→ `_v10`). Scratch outputs are in the session scratchpad and not kept.

## Orientation
- `_v8`: ReplacingMergeTree, `ORDER BY (contractAddress, address, sign, dt, nonce)`, from Kafka
  `xrp_stacks_v6.clickhouse-{host_index}`. `_v9`: `ORDER BY (contractAddress, address, sign, blockNumber, nonce)`, from
  `xrp_stacks_v7.clickhouse-{host_index}`. Both MVs have the same `SELECT`.
- Writer: `etherbi-flink` `net/santiment/xrp/job/XRPStacks.scala`. Its input is `KafkaSources2.getXRPBalancesStream`
  (the balances job's output; amount = `balance − oldBalance`). Its sink partitions rows over the topics by
  `blockNumber`.
- `system.parts`: both sides' last data is 2026-10-10 ~21:20. `_v9` has 0 level-0 parts; `_v8` has 45–46 per shard.
- The run was still going when the analysis started (2025-05 was written at 14:36 local). I waited for it to exit
  before running long queries. It wrote 2013-01 .. 2025-06 and stopped (early stop: 2.58M failing keys).

## Time line
| date | onset | event |
|---|---|---|
| 2013-01 .. 2025-06-16 | all equal | — |
| 2025-06-17 | first failing day; `missing_in_old` 54,630 vs `missing_in_new` 4,539 | 1 (A, B), 2 starts |
| 2025-06-18 .. 25 | paired missing, `differs` 35–114k/day; `new_gt_old` share drops from ~13% to ~3% on 06-23 | 2 (fewer B addresses active: 102 → 33) |
| 2025-06-26 | `missing_in_old` 72,367 vs 18,438; `differs` 65k → 271k | 1 (C, D); 2 grows (D addresses) |
| 2025-06-27 | `differs` 466k (peak) | 2 |
| 2025-06-30 | `missing_in_old` 31,440 vs 2,635 | 1 (E) |

I did not investigate the 06-23 direction change beyond the address count. Direction depends on the sign of each
address's missed flow, and the set of active addresses changed that day.

## Event 1: lost output
Ledgers on one side only, June 2025:
```sql
SELECT toDate(dt) d, blockNumber b, countIf(s=0) o, countIf(s=1) n FROM (
  SELECT 0 s, dt, blockNumber FROM default.xrp_stacks FINAL WHERE dt >= '2025-06-01' AND dt < '2025-07-01'
  UNION ALL
  SELECT 1 s, dt, blockNumber FROM test.xrp_stacks_test FINAL WHERE dt >= '2025-06-01' AND dt < '2025-07-01')
GROUP BY d, b HAVING o = 0 OR n = 0 ORDER BY b
```
This gives 484 ledgers, all new-only, which merge into 5 ranges (A–E in `analysis.md`) holding 135,501 rows. There
are no old-only ledgers.
- **Refutation check** ("the extra rows must add up to the gap"): the net row difference is +135,131 =
  135,501 − 370. The 370 are extra rows in old within 4,479 shared ledgers whose row counts differ, all from 06-17
  on (none before). They are stack-split differences of event 2. I did not verify per address that these all
  belong to B/D addresses.
- Source: in both balances tables, A/C/E have identical row counts (50,182 / 51,765 / 32,747). B/D have 426 / 2,436
  rows in `_v10` and 0 in `_v8`.
- Old's state saw A/C/E. In July, spends of stacks with `odt` inside the range windows were A 529/528, C 528/462,
  E 1,211/1,211 (old/new). The 11 addresses whose A/C spends differ are all active in D (event 2).
- Keys on the gap days: among the failing keys whose new rows all lie in lost ledgers, there are 50,453 + 54,444 +
  28,901 = 133,798 `missing_in_old`. The other failing keys on those days are event 2.
- Size: XRP born in A/B/C/D/E = 269.0M / 0.19M / 194.3M / 9.93M / 68.6M, with net ≈ 0 per range (fees go to
  `FEE_COLLECTOR`). Daily XRP spent volume of old vs new is short by 0.898% on 06-17, 0.761% on 06-26 and 0.208% on
  06-30, and differs by −2e-6 on 06-20.
- Zero-amount keys on the gap days (3,579 / 3,753 / 1,764) are lost rows with amount 0. Zero-amount stacks are routine
  (~0.2–0.6M rows/day in June on both sides).
- Why the output was lost (Flink sink transaction, Kafka, CH Kafka engine) is not determinable from the tables. All
  3 topics lost whole time windows (the sink spreads ledgers over the topics), so the loss is not specific to one
  partition. Kafka from June 2025 is likely past retention (not checked).

## Event 2: wrong state
Failing keys per (contract, address, category) per day, 06-17..30, one query per day (2–5 s each):
```sql
SELECT contractAddress, address, category, count() keys, sum(old_value), sum(new_value) FROM (
  SELECT contractAddress, address, sign, assetRefId, dt, odt, countIf(s=0) o_rows, countIf(s=1) n_rows,
         sumIf(sign*a, s=0) old_value, sumIf(sign*a, s=1) new_value,
         if(o_rows=0 OR n_rows=0 OR old_value=new_value, 0, abs(old_value-new_value)/greatest(abs(old_value),abs(new_value))) diff,
         multiIf(n_rows=0,'missing_in_new', o_rows=0,'missing_in_old', diff<=1e-9,'equal','differs') category
  FROM (SELECT 0 s, contractAddress, address, sign, assetRefId, dt, odt, toFloat64(amount) a FROM default.xrp_stacks FINAL WHERE <day>
        UNION ALL SELECT 1 s, … FROM test.xrp_stacks_test FINAL WHERE <day>)
  GROUP BY contractAddress, address, sign, assetRefId, dt, odt)
WHERE category != 'equal' GROUP BY contractAddress, address, category
```
- The per-day category counts are **identical** to the month file.
- Against the addresses active in the lost ledgers (from new): 8,656 addresses only in A/C/E fail only as
  `missing_in_old` on the gap days. Every other failing key on every day belongs to one of the 779 addresses active
  in B/D. No failing address is outside the lost ledgers.
- First version of the address list had a bug: the range filter included the 3 shared ledgers 97066619–621 and
  labelled them E. Fixed. The corrected list sums to 135,501 rows.
- Balance checks for 5 heavy addresses, flow 06-17..30, stacks vs `_v10` balance change:
  `rKi8uT8…` new −36.646608 = balance −36.646608, old −36.636492; `rTLdxcB…` new 133.883671 = balance, old 133.553827;
  `rogue5H…`, `rBTwLga…`, `r3rhWeE…` new = balance within 3e-5 (Float64 sums), old off by the lost flow.
- **Refutation check (all addresses):** if old's error were anything other than the missed ledgers, old's June net
  flow would deviate from `new − (address's flow in the lost ledgers)`. With exact sums
  (`toDecimal128(round(sign*amount, 6), 6)`) over 6,375 XRP addresses, 6,372 match within 3 µXRP and 3 are off by
  5–16 µXRP. This held.
- Sample of a paired missing pair: `rKw9bcac…` XMEME on 06-19 08:19:50 (tx `C5C99B01…`). Old has `odt` 1970-01-01 with
  amounts −1,373.04 / −1,528.79; new has `odt` 2025-06-16 00:09:21 with 2,676.59 / 2,520.85. The address got +4,052.26
  XMEME in B, which old never saw.
- Negative / epoch rows (`amount < 0`, `odt < 1971`) are the same rows on both sides 06-10..16 (1.6–2.5k/day, ~1k
  addresses). From 06-17, old has 1,056 more of them over 14 days.
- Raw rows (with and without `FINAL`) of the example key `rKi8uT8…` 2025-06-20 00:00:01: one row per side per sign,
  no unmerged copies. `nonce` differs by 1 (old 106879/106880, new 106880/106881). `nonce` is not compared.

## Unexplained
None.

## Catalog candidates
- **Lost output on one side** (split from *Lost input on one side*):
  - Signature: `missing_in_<side>` on the gap day only, on addresses that never fail again.
  - Confirm: the upstream table (balances) has the ledgers, and the side later spends stacks born in them (`odt`
    inside the range) as often as the other side.
  - Impact: the rows and the address's cumulative holdings only; the state is fine.
  - Seen in: `xrp_stacks` 2025-06-17/26/30, ranges A/C/E.
- **Wrong state after lost input**, additions to the signature:
  - The `differs` include many `float`-class keys (≤1e-6). These are drop-sized errors on large stacks, not float
    noise.
  - When the missed flow was an inflow, the side writes extra deficit stacks with negative amounts and
    `odt = 1970-01-01`.
  - Confirm with the per-address identity `side = other − flow in the lost ledgers`, using exact decimal sums.
- **Correction to "Lost input" seen-in:** `_v8` lacks 135,501 rows in 5 ranges (482 ledgers); the net row difference
  is 135,131 because old has 370 more rows from different stack splits in shared ledgers.

## Notes
- Before the incident (06-01..16), 866 of 5,407 XRP addresses (16%) have a stack net flow that differs from
  Σ(`balance − oldBalance`) in `_v10` by a few drops (median −4 drops, max ~0.01 XRP), on both sides alike. This is
  either a shared stacks-vs-balances discrepancy or an artefact of the probe (per-row `oldBalance` deltas). Not
  investigated.
- Float64 sums over an address's whole history are unstable. For `rBTwLga…` (40M stacks, ~2.2e11 XRP gross), old and
  new pre-06-17 sums differ by 0.049 XRP, while exact decimal sums are identical (17432.084007). This matters to
  consumers that sum raw amounts.
- Cost: ~30 queries, the longest ~30 s; nothing ran during `compare.py` except metadata probes.

# Continuation, 2026-10-13 (afternoon): 2025-07 .. 2026-09-02

Two more `compare.py` runs wrote 2025-07 .. 2026-06 (12:11–12:35 UTC) and 2026-07 .. 2026-09 (13:05–13:08); no
run was going during this analysis. Both tables still end 2026-10-10 ~21:20 (`max(dt)`: old 21:21:21, new 21:18:51);
`_v9`'s recent `modification_time`s are level-3 merges of old partitions, not new data. Scratch SQL and outputs are in
the session scratchpad and not kept.

## Predictions from the first pass, and checks
- *Event 2 persists on the 779 B/D addresses, fading.* Held: per-day failing keys by (contract, address), all 429
  days, one query per day (15–60 s each, 2 at a time):
  ```sql
  SELECT {d:Date} day, contractAddress, address, category, diff_class, new_gt_old,
    (contractAddress, address) IN (<the 779 B/D pairs>) in_bd, count() keys, …
  FROM (<per-key categories as in compare.sql, over default.xrp_stacks FINAL ∪ test.xrp_stacks_test FINAL, one day>)
  WHERE category != 'equal' GROUP BY contractAddress, address, category, diff_class, new_gt_old
  ```
  Per-day category counts equal the month files on 428 of 429 days (2025-08-09: see Notes). Every failing key is on
  a B/D address except 4,843 `missing_in_old` on 2025-12-22 (event 3). Monthly: 4.44M keys (529 addresses) in
  2025-07, 1.99M (243) 2025-08, 144k (144) 2025-09, then 42k–169k (42–200 addresses) per month, 2.8k on 2026-09-01..02.
- *A/C/E addresses (8,656) don't fail again.* Held through the cutoff.
- *The earlier "2026-05-31 sweep of 2.44M dust stacks" and "lopsided 2025-12-22".* The sweep does not show with the
  consumers' key: 2026-05-31 has 1,580 failing keys. 2025-12-22 is event 3.

## Time line (new onsets after 2025-06)
| date | onset | went to |
|---|---|---|
| 2025-07-04..13 | paired missing up to 42k/day, `large` 10–20k/day | 2 (`rhkpQH…`, `rUkcK6…`, `rK9xKh…`, `rSLpmD…`, …) |
| 2025-09-06 | `differs` drops below 5k/day | 2, fading |
| 2025-10-04/05 | `float` burst 4–10k | 2 (`rDscU1…`) |
| 2025-10-24/25, 10-31..11-11 | `large`, new > old, ~5k/day | 2 (`rP4xfx…`) |
| 2025-11-03..05 | paired missing 1.4–5k | 2 (`rP4xfx…` on 11-04) |
| 2025-12-22 | 7,119 `missing_in_old`, 0 `missing_in_new` | **3** |
| 2025-12-29 | `small` 600 → 3.7k/day until 2026-02-05 | 2 (`rhJQSU…`) |
| 2026-01-14..18 | `large`, new > old, ~4.2k/day | 2 (`rBYCfQ…`) |
| 2026-04-09, 04-15, 04-23 | `float` bursts 9–14k | 2 (`rGV6cX…`) |
| 2026-05-05..27 | paired missing ~150/day | 2 (`r9sqRf…`, `rwnYLU…`) |
| 2026-06-22/23, 2026-08-01/02 | paired missing 1–4.6k | 2 (`rJ4EpE…`) |
Steady from 2025-10: ~1,000 `float` + ~300 `small` keys/day = `rXPMus…` + `rsNvoA…` (B/D flows −12 and −20 drops).
Lopsided small days (2025-10-29, 2026-01-08, 2026-07-09) checked: no one-side ledgers, only stack-split row
differences in shared ledgers.

## Event 2: samples
- `rJ4EpE…` 2026-08-02 (`FINAL`): old 2,311 rows per sign at `odt` 1970-01-01, Σ −450.512098; new 2,311 at `odt`
  2025-03-04 23:51:40, Σ 2683.693834. Per row the gap is 1.3562; the address's flow in B/D is 1.356212.
- `rP4xfx…` 2025-11-07: both sides 2,487 rows per sign at `odt` 1970-01-01; old per-row −0.224..−0.194, new
  −0.117..−0.087, a constant 0.10745 gap; B/D flow 0.107454.
- `rXPMus…`, `rsNvoA…`: B/D flows −0.000012 and −0.00002; their daily `float`/`small` differences are this offset on
  large stacks.
- Negative epoch-dated rows (`amount < 0 AND odt < 1971`, no `FINAL`): both sides 35k–93k/month; old has
  ~24.5k more over 2025-06 .. 2026-09, in bursts (2025-07 +5.5k, 2025-11 +7.0k, 2026-06 +2.5k, 2026-08 +8.2k).
- **Refutation check** ("if old's error were anything but the missed B/D flow, its net flow per address would
  differ"): per address and month, 2025-07 .. 2026-09-02, exact sums:
  ```sql
  SELECT contractAddress, address, sumIf(v, s=0) o, sumIf(v, s=1) n, sumIf(f, s=0) fo, sumIf(f, s=1) fn, countIf(s=0) orows, countIf(s=1) nrows
  FROM (SELECT 0 s, contractAddress, address,
          if(abs(amount) < 1e25, toDecimal128(round(sign*amount, 6), 6), toDecimal128(0, 6)) v, toFloat64(sign*amount) f
        FROM default.xrp_stacks FINAL WHERE dt >= {a:Date} AND dt < {b:Date}
        UNION ALL <same for test.xrp_stacks_test>)
  GROUP BY contractAddress, address HAVING abs(o - n) > toDecimal128(0.000001, 6) * (orows + nrows) OR abs(fo - fn) > 1e-9 * greatest(abs(fo), abs(fn))
  ```
  Filtered again for |Δ| > 1 µ per row and > 1e-12 relative: no B/D or other address differs in any month, except
  in 2025-12 (event 3) and 5 token addresses whose Δ is an exact power of two in µ (2^22..2^30 µ, Float64 ULPs of
  ~1e17 amounts; their keys compare equal). Held. First attempt failed on `DECIMAL_OVERFLOW` (token amounts > 1e32).

## Event 3: lost output on 2025-12-22
- One-side ledgers on the day (`GROUP BY blockNumber … HAVING o != n`, both `FINAL`): 36 new-only, 0 old-only, 0
  shared with different counts. They are 101038121 + 3k, k = 0..35, 02:19:11–02:26:01, 129–290 rows each, 7,235 in
  all. December's net row difference is +7,268.
- By shard (Distributed `_shard_num`): new's rows for them are all on shard 3 (`blockNumber % 3 = 2`); old has 41
  other `% 3 = 2` ledgers on shard 3 in 02:15–02:30, and all `% 3 = 0, 1` ledgers on shards 1, 2.
- Balances: `default.xrp_balances` and `test.xrp_balances_test` both have 8,867 rows in the 36 ledgers.
- Addresses: 1,129 (contract, address) pairs in the 36 ledgers, 164 of them B/D. The 7,119 failing keys of the day
  that are not B/D `differs` are all on these pairs. The 965 non-B/D ones fail on no other day.
- Flow check: the 992 addresses whose December net flow differs all match `old = new − flow in the 36 ledgers`
  exactly (2 more have flows of 2 and 34 µ, under the probe's tolerance).
- Size: XRP born in the ledgers 32.88M; XRP spent on the day old 26,346,280,736 vs new 26,379,165,505 (−0.125%).

## Unexplained
None.

## Catalog candidates
- **Lost output on one side** (proposed in the first pass): add seen-in `xrp_stacks` 2025-12-22, 36 ledgers on one
  shard (every third ledger, one of the 3 topics) for 7 minutes; the signature can be every n-th ledger, n = topics.
- **Wrong state after lost input**: the error does not fade to zero: an address that keeps re-spending one affected
  stack fails every day (~1,300 keys/day on 2 addresses 15 months later). Deficit (epoch) stacks exist on both sides
  routinely (35k–93k rows/month); the lost input shows as a constant per-row offset equal to the missed flow.
  Seen-in: correct "2026-05-31 sweep of 2.44M dust stacks", which does not appear with the consumers' key.
- **Playbook stacks, Cautions:** "a key's rows must sit on one shard" does not hold for XRP: ledgers that share a
  close time (10 s resolution) put one `(address, dt, odt)` key on several shards (2025-08-09 03:00:30, 3 ledgers).
  Both sides place rows by `blockNumber`, so equal keys stay equal per shard; a failing key is counted once per shard
  (2025-08-09: +2/+2 missing and +15 `differs` in the month file vs a global grouping).

## Notes
- Cost: 429 day probes (~2.5 h at 2 parallel), 15 month flow probes (~20 min), ~20 small probes. One accidental
  full scan (`max(dt)` over both Distributed tables, ~4 min) ran alongside one probe.
