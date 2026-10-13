# Cause catalog

Known causes of differences. Edited by hand only; the agent proposes candidates in `investigation-log.md`.
Each entry: **applies to**, **signature** (as it shows in the month files), **confirm**, **typical impact**,
**seen in**.

## Float noise
- **Applies to:** any; sums of Float64 in a different order.
- **Signature:** `differs` only, all `float` (≤ 1e-6), usually ≤ a few ULP; both directions; no missing keys on the
  same day/group.
- **Confirm:** a sample's relative difference is ~1e-16..1e-12.
- **Impact:** benign.
- **Seen in:** `xrp_stacks` 2024-01 (25 of 5.3M amounts, 1 ULP); balances 2013-02 duplicate row.

## Lost input on one side (blocks, ledgers)
- **Applies to:** transfers, balances, stacks; and everything derived from them.
- **Signature:** `missing_in_<side>` concentrated on 1–few days, the other side has more rows; on stateful tables
  followed by `differs` on later days for the same addresses.
- **Confirm:** list the block numbers on each side for the day; find the ranges present on one side only; the extra
  rows must add up to the ranges. Check the upstream (balances, transfers, Kafka) for those blocks.
- **Impact:** the side with the blocks is better.
- **Seen in:** `xrp_stacks` 2025-06-17/26/30, old `_v8` lacks 5 ranges (135,131 rows); XRP balances old, 12 ledgers.

## Wrong state after lost input
- **Applies to:** stateful jobs (stacks, balances): Flink state that never saw some input.
- **Signature:** after a lost-input event, `differs` (mostly `small`) and paired `missing_in_new`/`missing_in_old` on
  the same addresses, for months; net flow per address equal; fading. Can surface much later as a burst when the
  address spends the affected stacks.
- **Confirm:** the failing addresses are those that had activity in the lost blocks; per-address balance differs by
  the lost amount.
- **Impact:** the side that saw the input is better.
- **Seen in:** `xrp_stacks` 2025-06 .. cutoff; 2026-05-31 sweep of 2.44M dust stacks.

## Key renumbering
- **Applies to:** tables whose key holds a sequence number (`nonce`, rank) or a column that depends on processing
  order.
- **Signature:** `missing_in_new` and `missing_in_old` in equal numbers on the same days, values equal when keys are
  matched on everything else.
- **Confirm:** re-key without the column (the consumers' key); the pairs become equal.
- **Impact:** benign if consumers don't use the column. Fix the config, not the table.
- **Seen in:** `xrp_stacks` with raw `nonce` (1.9M pairs in 2024-01).

## Key column differs
- **Applies to:** any.
- **Signature:** paired `missing_in_new`/`missing_in_old`, equal counts, same days, often same values.
- **Confirm:** join the two sides on the key minus one column at a time; the column that makes them match is it.
- **Impact:** depends on the column.
- **Seen in:** —

## Zero padding
- **Applies to:** metrics, and other tables where a job writes 0 for days without data.
- **Signature:** `missing_in_<side>` with `zero` ≈ `keys`, on days before the data exists (or after it ends);
  contiguous ranges per group.
- **Confirm:** the source has no rows for those days; the padded values are exactly 0.
- **Impact:** benign, unless a non-zero value appears before the data exists (then a bug on that side).
- **Seen in:** `daily_metrics` (Optimism): new pads 2017-07 .. 2020-04, old 2021-02 .. 2021-10.

## Source gap, one side
- **Applies to:** derived tables, metrics.
- **Signature:** all groups fail for 1–few days (`missing_in_<side>` or `differs`); for lookback metrics the
  failure continues for exactly the lookback length afterwards (e.g. 1y), then stops.
- **Confirm:** the source table of the side lacks those days; the other side's source has them.
- **Impact:** the side with the data is better.
- **Seen in:** `daily_metrics` old 2024-02-26..28 (from `opt_erc20_transfers`), carried a year by 1y metrics.

## Source gap, both sides
- **Applies to:** derived tables.
- **Signature:** none in the comparison (both agree); shows as one-day pauses in failing groups, or in invariants.
- **Confirm:** the shared source is near empty on the day.
- **Impact:** both wrong; outside the comparison. Report in Notes.
- **Seen in:** `opt_erc20_stacks` 2024-02-24/25, 2025-01-12, 2025-02-01.

## Contract (or definition) switch
- **Applies to:** metrics per asset, when an asset's contract was migrated.
- **Signature:** some assets `differs` (mostly `large`) on every day until a fixed date, equal after.
- **Confirm:** `asset_metadata` / the job config: one side used the earlier contract for history, the other today's.
- **Impact:** a design question (which history is wanted); usually reported as benign with a question for the owner.
- **Seen in:** `daily_metrics` o-usd-coin, o-velodrome-finance, o-lyra-finance, o-aave until 2025-07-22.

## Asset reference mismatch
- **Applies to:** metrics.
- **Signature:** an asset fails on all its metrics for all days, or has no rows on a side.
- **Confirm:** `asset_metadata.asset_ref_id` vs the hash the source rows are under (e.g. `cityHash64('OPT_' ||
  contract)`).
- **Impact:** that side is wrong for the asset (often both).
- **Seen in:** `daily_metrics` o-aave (both), o-veloce-vext.

## Job ordering (missing dependency)
- **Applies to:** metric pipelines, experimental re-runs especially.
- **Signature:** a family of metrics that depends on another job's output (prices, a seam) `differs`, `large`,
  systematically in one direction; or is missing for the first days of an asset (launch month), then wrong.
- **Confirm:** the dependency's rows were written after the consumer's (`_inserted`/part modification time, Airflow
  task times); the consumer joined stale or empty data. Then the job's `dependsOn`.
- **Impact:** the side with the ordering bug is wrong.
- **Seen in:** `daily_metrics` new P/L family: seam had no `dependsOn: price_usd` (fix `0cd7ac1b`); launch months.

## Coverage (group not computed)
- **Applies to:** configs with `common_groups_only`.
- **Signature:** in `coverage.json`, not in the month files.
- **Confirm:** the experimental run's job list.
- **Impact:** not checked; list in the summary.
- **Seen in:** `daily_metrics`, 1,666 groups only in old.

## Side still being written
- **Applies to:** any.
- **Signature:** failures only in the last days or weeks before the cutoff, growing towards it.
- **Confirm:** level-0 parts and a recent `modification_time` on that side.
- **Impact:** not a difference; move the cutoff back.
- **Seen in:** —
