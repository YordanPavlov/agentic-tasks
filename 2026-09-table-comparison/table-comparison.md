# Comparing an old and a new version of a table (runbook)

**Started:** 2026-09-30
**Status:** `compare.py` (`scripts/README.md`) writes per month the failing keys per day and per group, with zero
counts and difference size and direction (2026-10-13). Next: the analysis step, an agent following a recipe
(design below, "Analysis"); not written yet. `xrp_stacks`: equal through 2025-05; everything after traces back to
old losing blocks in June 2025 (see "Runs"). `daily_metrics` (Optimism): experimental was rewritten on 2026-10-13,
so the 2026-10-09 findings are out of date; to be analysed again.
**Origin:** [`2026-09-rerun-comparison-framework`](../2026-09-rerun-comparison-framework/rerun-comparison-framework.md)
has the development history and the first XRP balances results.

## When to use

A pipeline was re-run and wrote a new version of a table (transfers, balances, stacks, or metrics in
`*_experimental` tables). Before the new version replaces the old one, we need to know:
- what differs, and where (days, keys)
- why, as far as the data shows it
- a verdict, written up as an executive summary (template at the end)

Ground rules:
- **Judge by the data, not by the code change.** The tool works for any table, given its keys and value.
- **The full dataset, not samples.** Aim for ~1 h per comparison, 2 h at most.
- **Prod is read-only.** At most **2 long-running queries** at a time; small probes alongside are fine.
- **Leave the live tail out.** Set the cutoff a few days back: recent data is still being written.
- **One value per table,** summed per key and compared with a 1e-9 relative tolerance. The key is what consumers
  aggregate by; it must match exactly. Columns that only tell rows apart (e.g. stacks `nonce`) stay out of it.
- **Stop early.** Many differences in the early years mean the new run needs a look before the rest is compared.

## The tool

`scripts/README.md` describes the method, the output and the usage; `scripts/configs.py` documents the config
fields. In short: `compare.py` compares month by month in date order and writes `scripts/results/<config>/`,
one JSON file per month; its log has one line per month.

Reading the categories:

| category | usually |
|---|---|
| `equal` | OK |
| `differs` | list the keys of the cell with the window's query (see the README), then look at the rows |
| `missing_in_new` / `missing_in_old` | lost in new / added in new (e.g. recovered data), or a key column that differs: then they come in equal numbers on the same days |

### Design decisions (2026-10-09)

The previous version kept up to 10,000 key hashes per category for drill-down; the full stacks run left a 36 GB
cache that could not be loaded, and its results were spread over key ranges, not dates.
- **One value column.** A key that doesn't match and a value that differs need the same investigation, so every
  other column that matters is key. Balances `old*` columns are checked as invariants instead (below).
- **The consumers' key, value summed.** Stacks are keyed `(contractAddress, address, sign, assetRefId, dt, odt)`,
  as `daily_metrics/job_functions/xrp_stacks.py` aggregates them; `nonce`, block and tx are left out. Keying on a
  rank of `nonce` turned one extra stack in a 2.44M-stack sweep into 2.43M missing keys per side (2026-05-31).
  This dropped the per-table sources and the `*_multi` categories. A coarser key needs all its rows on one shard
  (true for stacks: checked on 2025-07-15).
- **Actual keys, no hashes.** Costs ~50% more memory per window. Example keys were dropped from the output
  (2026-10-09): they complicated the query, and a cell's keys can be listed with the window's query.
- **No fingerprints.** A month file records the config and when it ran; after a change, re-run with `--force`.
- **Output per month, failing keys only**, self-describing, the input of the analysis. Two views (2026-10-13):
  `days`, and with `group_by` `groups` (one line per failing group, its days as ranges): one line per group and
  day made a `daily_metrics` month 119,714 lines, now ≤ ~4,700. Per cell: missing keys with value 0 (`zero`),
  `differs` by size (`float` ≤ 1e-6, `small` ≤ 1%, `large`; `--float-max`, `--small-max`) and direction
  (`new_gt_old`). Zeros still count as failing and toward `--max-failing-keys`.
- **Metrics:** `daily_metrics_v2_experimental` holds a few assets (Optimism ERC-20 tokens) and lacks the metrics
  that were not part of the run. `where` limits old to its assets; `common_groups_only` leaves the missing
  metrics out of the comparison and lists them in `coverage.json`. A metric that is present must be complete.

### Analysis (design, 2026-10-13)

The step after `compare.py`: find the causes and write the summary. An agent does it, following a recipe, so that
every comparison is analysed to the same standard and the write-ups read alike.
- **A README, invoked explicitly** (not a skill): `scripts/analysis/README.md` (the recipe), `catalog.md` (causes),
  `template.md`, `playbooks/{stacks,balances,transfers,metrics}.md`, committed with the scripts.
- **One agent with one context**, no per-cluster subagents: causes interact (2026-05-31 was the June 2025
  incident; one ordering bug behind the P/L family and the launch months). Subagents only for mechanical probes.
- **Input: the month files** of `compare.py`, nothing derived; the agent queries the tables and reads the source
  code as it needs. A pre-digest (change points, clusters) was tried and dropped: hard to read, and it adds an
  interpretation layer. `summary.py` was removed for the same reason.
- **Walk the time line:** start with the earliest failing year (much of what follows is accumulated); then go
  forward month by month, and account for every new onset: groups or days that start failing are a new event or
  explained by an earlier one. Late, independent causes exist (`xrp_stacks` 2025-06, the 2025-07-23 contract
  switch).
- **Stop at high confidence, no budget.** Confidence: high = a catalog signature, confirmed on samples across the
  event's extent, mechanism shown in the data (rows of both sides, and the source or job when that is the cause);
  medium = signature and samples, no mechanism; low = signature only. A cause that cannot be found is
  `unexplained` once the testable hypotheses are ruled out; it goes to the log for a later iteration.
- **Cause catalog:** one entry per cause (applies to, signature, how to confirm, typical impact, seen in). The agent
  never edits it; it proposes candidates, which are promoted by hand. Entries come from "Runs" below (lost blocks,
  zero padding, key renumbering, contract switch, source gap, job ordering, …).
- **Output, in `results/<config>/`, not committed:** `analysis.md`, 2–3 pages: verdict and impact first, then the
  events in order of first appearance, each with a duration in fixed wording (`1 day`, `N days (A..B), not
  recurring`, `intermittent, N days between A and B`, `from A until the cutoff`, plus `growing` / `fading` when it
  matters) and links to related events ("Consequence of 1"); `investigation-log.md`: what was tried, what stayed
  unexplained, catalog candidates. Comparisons across runs are not part of the format; the agent can be given two
  runs' results.

## Procedure

1. **Check that the new table is completely written.** Per side, look at `max(dt)`, the unmerged parts and the
   last write:
   ```sql
   SELECT hostName(), max(max_time), countIf(level = 0), max(modification_time)
   FROM cluster('default_cluster', system.parts)
   WHERE database = 'default' AND table = '<local table>' AND active GROUP BY 1
   ```
   - A side that is still being written has level-0 parts and a recent last write. The live old table always does.
   - Ask the owner of the re-run whether the backfill has finished and whether its end date is intended. If you
     can't ask, put the question in the summary and go on assuming it has finished.
2. **Write the config.**
   - Set the cutoff a few days before the earlier of the two `max(dt)`.
   - Compare the sorting keys of both local tables (`system.tables.sorting_key`). If they differ, see "Key changes".
3. **Time one heavy month:** `python3 compare.py <config> 2024-01`. Then estimate the full run from the rows per
   month (see "Cost").
   - If the estimate is over budget, ask whether a longer run (overnight, resumable) is acceptable.
   - Otherwise run the most recent 2–3 years plus one heavy month per earlier year, and report the rest as not
     checked.
4. **Run everything** in the background: `nohup python3 compare.py <config> > compare.log 2>&1 &`. It prints one line
   per month, and stops after the month in which the failing keys pass `--max-failing-keys` (default 100k).
5. **Analyse** the results ("Analysis" above). Until the recipe exists: for every failing category, list a few keys
   of a failing cell (README, "The comparison query") and query both tables for them; find the cause from the rows.
6. **Optionally, run table invariants** (below) on both sides.
7. **Write the summary**, and add the run to "Runs" below.

### Key changes

When the two tables have different sorting keys, each side's ReplacingMergeTree has deduplicated by its own key.
Rows that are distinct under one key can have been merged away under the other.
- Choose the key to compare by explicitly. For XRP stacks, old uses `(…, dt, nonce)` and new `(…, blockNumber,
  nonce)`; the consumers' key holds neither, so only what `FINAL` merges away on each side can differ.
- To size the effect, run a second config with the other key and compare the summaries. A difference that
  disappears under the other key comes from the key change, not the data. Which key is right is a design question
  for the table's owner.

### Invariants (table-specific, not scripted)

They catch errors that old and new share, which a comparison cannot see:
- **balances:** chain continuity. Every (address, asset, `oldBlockNumber`, `oldBalance`) must point to an
  existing (address, asset, `blockNumber`, `balance`). XRP balances old has ~2,862 dangling pointers from 12 lost
  ledgers; new should have none.
- **stacks:** Σ `sign * amount` per block ≈ 0, apart from issuance (`table_qa/*_stacks.py`,
  `get_stacks_with_huge_delta`).
- **transfers:** tx count and block coverage against the chain.
- The yearly `table_qa/test/` checks in `clickhouse-tables` (see the `santiment-add-table-qa-tests` skill).

### Cost

- Stacks 2024-01 (2026-10-09): 498M rows with `FINAL`, both sides together; 9 windows, 264 s query time, 2:44
  wall at `--parallel 2`. A window query takes 22–57 s per shard and ~10.5 GiB; the counting query 31 s, 1.2 GiB.
  `TCMP_WINDOW_ROWS=14000000` brings a window back to ~7 GiB. Small months take 1–2 s.
- `daily_metrics` (one period, 2 windows): ~70–190 s for the whole range (2026-10-13).
- `xrp_stacks` full, 2013-01 .. 2026-09: 43 min wall (2026-10-13, light load).
- Cluster load can change the speed by an order of magnitude. Check the load with
  `SELECT count(), countIf(elapsed > 60) FROM system.processes` (that sees one broker only), and prefer evenings
  and weekends.
- With `--parallel 2`, a full run uses both long-query slots, so nothing heavy can run beside it.

## Misc

**ClickHouse:**
- Port 30900 lands on the load balancer (a random broker); 30941, 30942 and 30943 land on the shard hosts
  `clickhouse-0`, `-1` and `-2`. `cluster(...)` reaches every shard from any of them; the `readonly` user has the
  `REMOTE` grant this needs.
- The `readonly` user has `readonly=2`: it can change settings and create temporary tables.
- The server runs the old query analyzer (`enable_analyzer=0`).
- In the generated SQL, the `cmp_` prefixes keep aliases from shadowing real table columns. With the old analyzer, a
  shadowed name would change the expressions, and `sum(x) AS x` fails with `ILLEGAL_AGGREGATION`.
- `SELECT *` skips MATERIALIZED columns (e.g. `assetRefId` in balances). Always list columns.
- `system.parts`, `system.processes` and `system.query_log` cover one broker. Use `cluster(...)` for the whole
  cluster, and measure time on the client.
- `clickhouse-client --format` overrides a `FORMAT` clause in the query.
- The server quotes 64-bit integers in JSON. `common.run_query` turns that off.

**Environment:**
- `pkill -f <pattern>` can match its own shell; kill by PID.
- Keep outputs local. Don't publish artifacts or docs unless asked.

## Summary outline

Until `scripts/analysis/template.md` exists:
```
# <old> vs <new>: <PASS | PASS with caveats | DIFFERS>
Verdict and impact: ≤3 sentences (e.g. "new is right; old lost 5 block ranges in June 2025").
Scope: <start> .. <cutoff>, months run / skipped; rows and keys per side; run date.
Events, by first appearance: date, what, duration, impact (new better / old better / benign / regression),
  mechanism with an example row per side, confidence.
Not checked: months skipped, invariants not run, open questions (e.g. backfill end date).
```

## Runs

| date | config | scripts | result |
|---|---|---|---|
| 2026-09-30 | `xrp_balances` | tier scripts | new ⊇ old, equal within 1e-9 (Tier 1 97 min, Tier 2 28 min); details in the origin doc |
| 2026-09-30 | `xrp_stacks`, probe | tier scripts | no keys lost; `nonce` renumbered; holdings and ages equal |
| 2026-10-02 | `xrp_balances` 2013-01..02, `xrp_stacks` 2013-02..03 and 2024-01 | current | validation of the new scripts, below |
| 2026-10-07 | `xrp_stacks` 2013-01 .. 2026-09-03, full | key-range windows (314) | DIFFERS, below |
| 2026-10-09 | `xrp_stacks` 2013-02..06, 2024-01 | per-month output | equal; trial, below |
| 2026-10-09 | `daily_metrics` | per-month output, then direct queries | DIFFERS: mixed, new not strictly better; below |
| 2026-10-09 | `xrp_stacks` 2013-02 .. 2026-09-03 | per-month output, `nonce_rank` key | equal until 2025-06-17, then old lost blocks; below |
| 2026-10-13 | `xrp_stacks` 2013-01 .. 2026-09-03 | days/groups output, consumers' key | equal through 2025-05, then every month fails; below |
| 2026-10-13 | `daily_metrics`, after experimental was rewritten | days/groups output | first failing year 2020; below |

**Validation, 2026-10-02:**
- Key and row counts match `uniqExact` / `count()` on the Distributed tables exactly (stacks 2013-03, balances
  2013-02).
- Stacks 2024-01-03 categories match the earlier Tier 2 run: 61,369 `only_old`/`only_new` pairs; 3,879
  `value_diff` + 112 `old_multi` + 1 `float_diff` = Tier 2's 3,992 `value_diff`.
- **Stacks 2024-01, under the strict rule: DIFFERS.**
  - old: 498M rows on disk, i.e. the whole month twice. With `FINAL`, both sides have 249,030,813 rows.
  - 1.9M `only_old`/`only_new` pairs: the `nonce` renumbering.
  - 97,924 `value_diff`, 8 `float_diff`. No key moved between shards.
  - `FINAL` vs a plain read of the same month: 284 s vs 203 s, under heavier load (17 queries over 60 s).
    - The plain read's 1.7M `old_multi_new_matches` became `equal`: the latest version is the one that matches.
    - Its 1,660 `old_multi` became `value_diff`: the version consumers see differs from new.
- Balances 2013-02: equal under `FINAL`. Old holds one row twice, with balances that differ in the last ULP
  (8.114739e-9 vs 8.114739000000001e-9). `FINAL` returns the newer copy (part `201302_4_4_0`), and it equals new.
- Side finding (2026-09-30): 2013 stack rows with `sign = 1` have `odt = 1970-01-01` on both sides.
- Cache of these runs: `~/.cache/table-cmp-v2/`.

**`xrp_stacks` full run, 2026-10-07:** 274 min query time. 8.41B keys per side (new +143k).
- `only_old` 10,200, `only_new` 153,310 (none moved between shards), `value_diff` 4.0M, `float_diff` 9.3M,
  `soft_diff` 1.13B (`nonce` offset only), `equal` 7.26B.
- 443 failing days. 2026-05-31 alone has 2.43M `value_diff`; mid-June to mid-August 2025 is mostly
  `float_diff`, with `only_new` spikes on 2025-06-17 and 2025-06-26.
- The cache was deleted on 2026-10-08; the redesign makes this run obsolete.

**Trial of the per-month output, 2026-10-09:**
- Stacks 2013-03..06 cut into 1 window and into up to 30 windows give the same rows and keys per category.
- Stacks 2024-01: equal (the 97,924 `value_diff` of 2026-10-02 came from keying on raw `nonce`). Checked
  independently: a checksum of the key columns is identical on both sides; on a 1/50 sample, 25 of 5.3M `amount`s
  differ, all by 1 ULP (2.2e-16 relative).

**`daily_metrics_v2` vs `_experimental`, 2026-10-09:** 18 Optimism assets (`o-*`), 430 common metrics; 1,666
groups only in old (not part of the run). `compare.py` stopped at 2017-07 (108,760 `missing_in_old`); the whole
range was then aggregated with direct queries (scratchpad, not kept): equal 7.10M, `differs` 4.47M (3.79M > 1%),
`missing_in_old` 7.16M (6.69M zero), `missing_in_new` 213k (51k non-zero). Causes, by confidence:
- **Zero padding (benign):** new has zeros 2017-07-12 .. 2020-04, old 2021-02 .. 2021-10; no chain data before
  2021-11. Exception: ~2.4k non-zero `mvrv_z` / `std_dev_marketcap_usd` in new before the chain existed.
- **Old has gaps, new right:** 2024-02-26..28 for every asset (source `opt_erc20_transfers` has them; carried a year
  further by 1y-lookback holder metrics); o-wrapped-bitcoin 2025-07-23 .. 2026-06-17.
- **Contract switch, not data:** o-usd-coin, o-velodrome-finance, o-lyra-finance, o-aave differ until exactly
  2025-07-22; old used the earlier contracts (USDC.e, VELO v1), new today's contract for all history.
- **o-aave broken in both (new entirely):** `asset_metadata.asset_ref_id` 5802947053947896739 ≠
  `cityHash64('OPT_' || contract)` = 22850480597297386, under which the source rows are. o-veloce-vext reuses
  `veloce-vext`'s ref; its contract has no source rows.
- **New's P/L family is broken (proven):** in `distribution_deltas_5min_experimental`, coins acquired in the month
  they move (88–97% of moved rows) carry the previous month's last 5-min price as `acquisition_price`; outflow and
  inflow then cancel, so `transaction_volume_profit` + `_loss` is ~0.4% of volume (old 5–68%), with negative loss
  sums. Cause: the seam (`age_distribution_5min_delta`) declared no `dependsOn: price_usd`; in experimental runs its
  ASOF price table is `intraday_metrics_experimental`, and each month's seam was written ~1 min before that month's
  prices. The ASOF join itself (PR #2132, `d678ad9a`) is right. Fix: `dependsOn` added, commit `0cd7ac1b` on
  `seamDependsOnPrice` in `clickhouse-tables` (job sort checked: prices now precede the seam). Also affects
  network P/L and `stack_price_consumed` (not checked separately). The unmerged `validate_price_coverage`
  (`origin/priceValidation`, `29c2d2c5`) would have caught it.
- **Launch month (medium):** new lacks USD stack metrics for a token's first days, then bad values
  (o-walletconnect-token: `mvrv_usd_30d` 37.9 on 2025-05-01, negative `stack_mean_age_dollar_days` on 462 days);
  likely the same ordering. Old's values there are plausible.
- **Where old is worse:** realized price / MVRV (OP 2026-09-15 at $0.099: `mean_realized_price_usd_30d` new 0.089,
  old 0.44); old has impossible values (negative and 33.7M-day `stack_mean_age_days`, `stack_liveliness` > 1,
  `percent_of_total_supply_in_profit` > 100).
- **Unexplained:** ~1M non-trivial `differs` before 2024-02-26 on the 13 assets without a contract switch, in
  balance/stack-based metrics, while transfer metrics are equal. `total_supply` matches neither side to the source
  (one 2^32 mint, ~8k OP burned; both tables grow above 2^32 from 2024).
- Source notes: `opt_erc20_stacks` keeps supply on a `mint` pseudo-address as a negative stack with
  `odt = 1970-01-01`; the prod seam has `acquisition_price` NULL for all OP rows before 2025. Source nearly empty on
  2024-02-24/25, 2025-01-12, 2025-02-01 (shared by both sides; outage or ingestion gap not checked).

**`xrp_stacks` per-month run, `nonce_rank` key, 2026-10-09:** every month equal up to 2025-05, and 2025-06-01..16.
2025-06: 2.31M `differs`, 159k `missing_in_new`, 294k `missing_in_old`; every later month still differs.
- **Cause: old (`_v8`, written live from `xrp_stacks_v6`) has no rows for 5 block ranges**, which new has:
  96867745–96867916 and 96868120–121 (06-17, 50.9k rows), 97066419–97066618 and 97066622–631 (06-26, 55.2k),
  97154619–97154718 (06-30, 29.4k). These add up to new's +135,131 rows exactly.
- The live `xrp_balances_shard_v8` has the three large ranges (same rows as `_v10`), so the source had them and
  only the old stacks pipeline dropped them. It lacks the 12 blocks of the two small ranges too: probably a short
  upstream gap at the time, which `_v10` recovered.
- **Large ranges: only the output was lost.** Old's later rows spend stacks born inside them (gap C: the same
  counts as new in June and July), and addresses only in them are equal from 2025-07 (0 of 4,909).
- **Small ranges (12 blocks): never reached the job,** so old's Flink state for their addresses is wrong from then
  on. ~420 heavy addresses differ in June; in 2025-07..10, 323 → 82 of the addresses in both kinds of range and
  39 → 15 of those only in small ones, plus 6–10 others (likely `odt` inherited through transfers; not verified).
- Their stacks carry amounts a few drops off (`differs` on the same key); net flow per address is equal. The
  missing keys pair up with `missing_in_old` (another split or `odt`); on 2025-07-01..07 only 7 txs are in old
  alone, all leftover dust stacks of the wrong state.
- **2026-05-31: same incident, surfacing late.** At 12:12:12, tx `384C54B2…` (block 104604271) sweeps 2.44M dust
  stacks of `rTLdxcBkCUeNR1rJc7KVz3uqchfR73hpF`. On 2025-07-15 old, short by ~0.0096 XRP for that address, had
  spent 6 stacks of 2025-05-10 that new kept; in the sweep new spends them, shifting every later `nonce_rank`:
  2.43M missing keys per side. With the consumers' key: 4 and 10.
- Verdict: new is right; old lost blocks and, for the small ranges, has a corrupted state since.

**`xrp_stacks` full run, consumers' key, 2026-10-13:** 8.41B keys per side; `differs` 9.35M, `missing_in_new` 308k,
`missing_in_old` 449k, all from 2025-06 (peak 2025-07: 4.0M `differs`, 212k missing per side; 40–170k `differs` a
month from 2025-09). 2025-12 is lopsided: 7,294 `missing_in_old` vs 142 `missing_in_new`, 7,119 of them on
2025-12-22 (new +7,268 rows).

**`daily_metrics`, 2026-10-13:** `daily_metrics_v2_experimental` last written 2026-10-13 05:06 (13.2M rows; probably
the re-run with `seamDependsOnPrice`, not confirmed). Equal 2009–2019; from 2020-01 old has zero-valued keys new
lacks (3 groups, then 572 from 2021-02). 2025, seen while trying a pre-digest: change points on 2025-02-27/28
(~800 groups stop: the 2024-02-26..28 gap leaving the 1y lookback), 03-25 and 04-15 (o-walletconnect-token switches
`missing_in_old` → `differs`), 07-23 (contract switch), 12-22/23 (241 groups start, 169 stop; unexplained);
source days 2025-01-12 and 02-01 as one-day pauses in ~250–290 groups. 2,023 groups differ on every day of 2025.

## Open

- Write the analysis recipe (`scripts/analysis/`), then try it on `xrp_stacks` and `daily_metrics`.
- `daily_metrics`: confirm what rewrote experimental on 2026-10-13; PR for `seamDependsOnPrice`; fix o-aave's
  `asset_ref_id`; explain the pre-2024 balance/stack `differs`, `total_supply`, and 2025-12-22. Its zero padding
  stops `compare.py`: run with a high `--max-failing-keys`.
- An HTML page for the results: daily stacked bars per category, a year → month → day table.
- Full `xrp_balances` run.
- Stacks: 2025-12-22, new has ~7.1k keys old lacks: more blocks lost by old? (Same day as the `daily_metrics`
  change point, on another chain.)
- Stacks: the few addresses outside the lost blocks that differ (2 in 2025-06, 6–10 per month after).
- Script the invariants, starting with the balance chain.
