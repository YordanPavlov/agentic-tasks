# Comparing an old and a new version of a table (runbook)

**Started:** 2026-09-30
**Status:** Generic runbook and scripts (`scripts/`). Regression-tested on XRP balances. A cold-read trial on XRP
stacks by a fresh agent (probes only) found no data loss; the full run is blocked on the time budget (see "Runs").
**Origin:** [`2026-09-rerun-comparison-framework`](../2026-09-rerun-comparison-framework/rerun-comparison-framework.md)
has the development history and the XRP balances results.

## When to use

A pipeline was re-run and produced a new version of a source table (transfers, balances, stacks) or of metrics
(`*_experimental` tables with the production structure). We need to know whether the new version can replace the
old one:
- what differs
- where it differs
- why, where that can be found from the data
- a verdict in an **executive summary**

Ground rules (from the user):
- **Black box.** Don't reason from the code change; judge by the data. The checks must work for any table.
- **Full dataset, not samples**, within **~1 h per comparison (2 h max)**. Falling back to basic guardrails is
  acceptable when the budget does not allow more.
- **Prod is read-only** (`-u readonly`). At most **2 long-running queries** at a time; small probes alongside are fine.
- **Historic cutoff** a few days back. Live data adds little and is still being written.
- **Tolerance 1e-9 relative** for floats. Non-float columns must match exactly.

## Method in one paragraph

Both tables are Distributed over `ReplacingMergeTree` shards, usually with **no sharding key**. Duplicates of one key can
therefore sit unmerged on different shards, and neither `FINAL` nor per-shard counts can be trusted. Every check must
either deduplicate globally or be **dedup-invariant**.

- **Tier 1** runs over the full history, day by day, on each side separately. It computes counts (`uniqExact` of
  blocks, txs, keys) and a content fingerprint: `sumDistinct` of a hash of (key, content). In that hash, floats are
  truncated to ~32 mantissa bits, so ULP noise does not move the fingerprint but any change above ~4.7e-10 does.
- **Tier 2** runs only on the days Tier 1 flags. It joins old and new per key and puts every differing key into a
  category, with exact float diffs.
- Optional **aggregate checks** and table-specific **invariants** (Tier 3) catch changes where rows may legitimately
  differ.

## Scripts (`scripts/`)

All scripts are plain Python 3 plus `clickhouse-client`, with no other dependencies. Run them from `scripts/`.
Results are cached under `$TCMP_CACHE/<config>/` (default `~/.cache/table-cmp`). Everything is resumable: a finished
window or day is never re-queried.

| script | tier | what | cost |
|---|---|---|---|
| `configs.py` | | one entry per comparison | |
| `common.py` | | CH access, metadata, hash expressions, window plan | |
| `tier0.py <cfg> [--reset]` | 0 | schema diff, key and value columns, coverage and write activity per side, suggested cutoff, rows per month, window plan | 20–40 s |
| `tier1.py <cfg> [lo hi]` | 1 | per day and side fingerprints over the full history, 2 queries in flight | 0.15–2.7M rows/s per side, see "Cost" |
| `tier1_compare.py <cfg>` | 1 | totals, failing days, `tier1_fails.json` | local |
| `tier2.py <cfg> [--key old] [--sample N] [day ...]` | 2 | per-key categories on failing days | 0.08–0.8M rows/s: 1.5–7 min for a 20–30M-row day |
| `tier2_agg.py <cfg> <check> [--sample N] [day ...]` | 2 | aggregate equality per group on failing days | about the same as tier2 |
| `tier2_summary.py <cfg> [subdir]` | 2 | totals per category; `subdir` is `tier2`, `tier2_key-old` or `tier2_agg-<check>` | local |
| `rows.py <cfg> <day> <kh ...> [--key old]` | 2 | actual rows on both sides for sample key hashes | scans the whole day: up to ~1.5 min |

`--sample N` picks every failing day with a blocks/txs/keys difference, plus N content-only days spread evenly over
the history.

`lo hi` of `tier1.py` restricts the run to windows fully inside that range. Use it for probes; a full run takes no range.

### Config (`configs.py`)

```python
'xrp_balances': dict(old='default.xrp_balances', new='test.xrp_balances_test',
                     dt='dt', block='blockNumber', tx='transactionHash',
                     start='2013-01-01', cutoff='2026-09-26',       # cutoff exclusive
                     agg_checks={'net_change': dict(group=['assetRefId'], value='balance - oldBalance')}),
```

- **Required:** `old`, `new` (Distributed or local tables), `dt` (DateTime/Date column the tables are partitioned by),
  `start`, `cutoff`.
- `block`, `tx`: columns for the blocks/txs checks. Use `None` when the table has none (metrics).
- `key`: `'new'` (default: the new shard table's sorting key), `'old'`, or an explicit list of expressions.
- `exclude`: value columns to leave out, for bookkeeping columns that always differ (`computed_at`, insert time).
- `split_by`: extra Tier 1 grouping columns besides the day (e.g. `['metric_id']`). Tier 2 then works per (day, value).
- `where`, `where_old`, `where_new`: extra SQL filters. Example: restrict old to the metrics or assets present in the
  experimental table.
- `agg_checks`: `{name: dict(group=[cols], value=expr)}` for `tier2_agg.py`.

Value columns are the columns present on both sides, minus key and `exclude` columns. ALIAS columns are skipped;
MATERIALIZED ones are included. `tier0.py` prints columns that exist on one side only; they are not compared.
Non-float columns whose type differs between the sides are compared as strings.

Tested configs for balances and stacks are in `configs.py`. Use them as templates.

`meta.json` (key, columns) and `windows.json` (window plan) are fixed on first use, so cached results stay
consistent. **After changing `key`, `exclude`, `start` or `cutoff`, run `tier0.py <cfg> --reset` and delete
`tier1/`.** Also delete the cache when either table's data changed, e.g. a backfill progressed or merges happened
after a run. `tier1_compare.py` warns if a day appears in two cached windows.

## Procedure

1. **Add the config without `cutoff`** (`cutoff=None`) and run `tier0.py`. With no cutoff it makes no window plan.
2. **Check the preconditions from its output.**
   - **Is the new table completely written?**
     - Per side, tier0 prints unmerged level-0 parts, the last part write time and `max dt` over all shards.
     - A side that is still being written has level-0 parts and a recent last write; the old live table always
       does.
     - Merges can rewrite parts long after the backfill, so a recent write with 0 level-0 parts is not proof
       that the backfill is still running.
     - Ask the user whether the backfill has finished and whether its end date is intended. Write the question
       in the summary and continue on the assumption that it has finished.
   - **Set `cutoff`** a few days before the suggested one (the earlier `max dt`), then run `tier0.py <cfg> --reset`.
   - **Rows per month** (new/old; a local-shard estimate, up to ~40% off). Flags outside 0.8–1.25 before the cutoff:
     - exactly 0.50 or 2.00 for a month: one side holds that month twice. Whole months were re-inserted, and
       Tier 1 `dups` will confirm it.
     - low only in the most recent months: the backfill is unfinished.
   - An unfinished merge is harmless: every check is dedup-invariant.
3. **Review the schema part of `tier0.py`.** Check:
   - Do the sorting keys differ? If so, see "Key changes" below.
   - Are any columns only on one side, or with a different type? Are the value columns the ones that matter?
   - What `engine` does each side use? `ReplacingMergeTree`-like engines are assumed. For `CollapsingMergeTree`
     or aggregating engines, rows are not one per key and the method needs thought.
4. **Probe Tier 1** on one heavy and one light window (`tier1.py <cfg> <lo> <hi>`, with dates from `windows.json`).
   - It catches SQL errors before the full run.
   - Estimate the full run as estimated rows ÷ observed rows/s, using the slower side.
   - If the estimate is over budget, see "Cost and budget" below before going on.
5. **Run the full Tier 1** in the background (`nohup python3 tier1.py <cfg> > tier1.log 2>&1 &`), then run
   `tier1_compare.py`. Read it as follows:
   - `dups`: rows minus unique keys per side, i.e. unmerged or duplicate rows. Clean after merges is 0.
   - `blocks` / `txs`: a difference means a processing gap or extra data; this is the most serious class.
   - `keys`: rows missing or added.
   - `content` only: the same keys with different values. Expected false positives are old's multi-version keys
     and floats on a truncation-bucket edge.
6. **Tier 2 on every failing day** (`tier2.py <cfg>`, 2 in parallel at most by splitting the days), then run
   `tier2_summary.py`. See the categories below. For each non-benign category, fetch sample rows with `rows.py`
   and find the cause.
   - If most days fail on `content` alone, run `--sample N` with N sized to the remaining budget, then run the
     aggregate checks on the same days. Report the other days as "counts equal, content unchecked".
   - A heavy content failure that is only `old_conflict_new_matches` means old's duplicates, not a problem.
7. **Table-type checks** (below): aggregate checks on failing days and invariants.
8. **Write the executive summary** (template at the end). Record the run in this doc under "Runs".

### Tier 2 categories

| category | meaning | usually |
|---|---|---|
| `old_conflict_new_matches` | old has several versions of the key and one equals new | benign: old duplicates |
| `new_conflict` | new has several versions of the key | new-side problem, or the backfill is still merging |
| `value_diff` | one version each, different content | check `maxrel_*` (≤1e-9 means noise) and `viol_*` / `mism_*` (must be 0) |
| `only_old` | key only in old | lost in new, **or** a key change (below) |
| `only_new` | key only in new | lost in old, **or** a key change |

`maxrel_<col>` measures the distance from new to the nearer of old's min and max. That is exact for single-version
keys. `mism_<col>` counts keys whose new value is not among old's values. Float stats are meaningless in the `only_*`
categories.

### Key changes

When the sorting keys differ, e.g. old `(…, dt, nonce)` and new `(…, blockNumber, nonce)`, each side's
`ReplacingMergeTree` has deduplicated by its own key. Rows that are distinct under one key can have been merged away
under the other.
- Tier 1 compares under one key (`key` in the config). Tier 2 can re-key a day with `--key old`.
- A difference that appears under one key and disappears under the other is caused by the key change, not the data.
  Report it as such: which key is right is a design question for the user.
- To size the effect, compare `tier2_summary.py <cfg>` with `tier2_summary.py <cfg> tier2_key-old` over the same
  days.

### Cost and budget

Observed speed varies by an order of magnitude with cluster load and data shape:
- XRP balances full run: 2.7M rows/s on average.
- 2026-09-30 afternoon probes, with other DMF queries running: 0.15–0.8M rows/s.
- The old side of a duplicated table is slowest (xrp_stacks old: 89M rows in 434–625 s).

The cost is in the scan and `uniqExact(key)`, not in hashing the content: dropping the content fingerprint gave no
speed-up. Check load with `SELECT count(), countIf(elapsed > 60) FROM system.processes` (that sees one broker only),
and prefer evenings and weekends. Tier 1 already occupies both long-query slots, so nothing heavy can run beside it.

When the estimate is over budget, in order:
1. Ask the user whether a longer run is acceptable, e.g. Tier 1 overnight in the background. It is resumable.
2. Run Tier 1 on a subset: the most recent 2–3 years (the data that matters most) plus one heavy window per
   earlier year. Report the rest as unchecked.
3. Use the minimum guardrail, the yearly `table_qa`-style counts per side: `uniqExact` of blocks and txs and
   `count()` per year. The count is not dedup-invariant, so compare it only for a side that has no duplicates.

### Aggregate checks (`tier2_agg.py`)

Some tables can differ row by row while being equivalent. A stack can be split into different pieces, or a row
can be emitted with a different `nonce`. For those, compare a sum per group over deduplicated keys instead of rows.
`viol` counts groups where |old − new| > 1e-9 × max(Σ|old|, Σ|new|); `samples` shows up to 5 of them as
(group…, old, new). Choose the groups so that equality is a real requirement. Examples:
- **balances:** `balance - oldBalance` per asset: the net change per day.
- **stacks:** `sign * amount` per (contractAddress, address, blockNumber): the net balance change, which must not
  depend on how stacks are split. Adding `toDate(odt)` to the group checks the age distribution at day resolution.
  It does not catch a changed FIFO/LIFO order within the same day; for that use `odt` itself, or `toStartOfHour(odt)`.

If Tier 2 shows many `only_old` + `only_new` keys on a day but the aggregate check has 0 `viol`, the rows are
organized differently while holding the same amounts. Report that as a "representation change" and show a sample
with `rows.py`.

### Invariants (Tier 3, table-specific, not scripted yet)

Run them on both sides. They catch errors that old and new share, which the old-vs-new comparison cannot see.
- **balances:** chain continuity. Every (address, asset, `oldBlockNumber`, `oldBalance`) must point to an existing
  (address, asset, `blockNumber`, `balance`). XRP balances old has ~2,862 dangling pointers from the 12 lost
  ledgers; new should have 0.
- **stacks:** Σ `sign * amount` per block ≈ 0, apart from issuance. `table_qa/*_stacks.py` has a yearly version
  (`get_stacks_with_huge_delta`).
- **transfers:** tx count and block coverage against the chain.
- The yearly `table_qa/test/` checks cover blocks, txs, unique balance changes, per-address chains and stacks sum.
  For a chain with no `table_qa` test yet, see the `santiment-add-table-qa-tests` skill.

A full-history invariant needs a global per-key dedup, costing ~2.5M rows/s. Run it only when the budget allows,
otherwise on the failing days.

### Table types

- **Balances / stacks / transfers:** `dt`, `blockNumber` and the tx hash column (`transactionHash`, `txID`, …) exist.
  The key comes from metadata.
- **Metrics (`intraday_metrics_experimental`, `daily_metrics_v2_experimental`):**
  - set `block=None, tx=None, split_by=['metric_id']` (or `asset_id`), `exclude=['computed_at']`
  - restrict both sides with `where` to the re-run metric ids and assets
  - the experimental table only holds what was re-run, and old holds everything
  - see the `santiment-clickhouse-metrics-navigation` skill for ids

## Gotchas

**ClickHouse:**
- `cityHash64(…)` returns NULL if any argument is NULL. `common.canon` wraps nullable columns as
  `isNull(x), ifNull(x, default)`.
- `SELECT *` skips MATERIALIZED columns and fails in UNION ALL when the sides differ. Always list columns explicitly.
- Aliases in `tier2.py` are prefixed (`v_<col>`). Shadowing a real column name would change the hash expression.
- `clickhouse-client --format` overrides a `FORMAT` clause in the query.
- IN subqueries on Distributed tables need `GLOBAL IN` (`distributed_product_mode = deny`).
- `x IN (col, col)` is invalid; use `=` / `OR`.
- `readonly` cannot use `cluster()` / `remote()`. `system.parts` covers the local shard only, so `tier0` multiplies
  by the shard count, which makes it an estimate.
- `system.query_log` / `system.processes` are per broker behind a load balancer, so measure time client-side.
- Shard tables of `test.*` Distributed tables can live in `default` (`test.xrp_stacks_test` →
  `default.xrp_stacks_shard_v9`). `common.shard_table` follows the engine arguments.

**Environment:**
- The container lacks `column`, `bc`, `/usr/bin/time`.
- `pkill -f <pattern>` can match its own shell; kill by PID.

**Deliverables:** keep outputs local. Don't publish artifacts or docs unless asked.

## Executive summary template

```
# <old> vs <new>: <PASS | PASS with caveats | FAIL>
Scope: <start> .. <cutoff>, all assets; rows old/new; run date; wall time per tier.
Verdict: one or two sentences (e.g. "new ⊇ old, content-equal within 1e-9; new recovers N blocks").
| check | old | new | result |    (rows, keys, dups, blocks, txs, fingerprint days differing, T2 categories,
                                     aggregate checks, invariants)
Discrepancies: per class: size, where (dates/blocks), cause if found (with a sample row), benign or not.
Not checked: tiers skipped, days not covered by T2, invariants not run.
```

## Runs

| date | config | result | notes |
|---|---|---|---|
| 2026-09-30 | `xrp_balances` | new ⊇ old, content-equal within 1e-9 | T1 97 min, T2 28 min; details in the origin doc. The scripts were regression-tested against that run. |
| 2026-09-30 | `xrp_stacks` (probe only) | no data loss in the probes; the row representation changed | see below |

**xrp_stacks probe, 2026-09-30:**
- **Scope:**
  - 2 Tier 1 windows (2013-01-01..2014-05-28 and 2024-01-02..06, 515 days).
  - Tier 2, `--key old` and both aggregate checks on 3 days.
  - cutoff 2026-09-03.
- **Coverage and duplicates:**
  - New ends at 2026-09-05 20:17. **Open question for the user:** is that the intended end of the backfill?
  - Old holds many whole months twice: new/old ratio 0.50, and 2024-01 old rows = 2 × keys.
- **Tier 1:** keys, blocks and txs are identical on all 515 days. 493 days fail on content.
- **Tier 2 on 2024-01-03:**
  - 61,369 `only_old` / `only_new` pairs: the same stack with `nonce` shifted by 1.
  - 3,992 `value_diff`: different `odt` or `amount`, i.e. stacks split or consumed differently.
  - `--key old` gives identical numbers, so the key change has no effect.
- **Aggregate checks:** `net_change` and `age_dist` have 0 violations in ~5.6M groups (max rel 7.9e-16).
  - Verdict so far: a representation change with equal amounts per block and per age-day.
- **Estimate:** full Tier 1 at 7–15 h at the observed speed, ~1 h at the balances speed.
- **Next:**
  - rerun the probe off-peak to measure speed
  - full Tier 1 if the user accepts the run time
  - `--sample` Tier 2 plus aggregate checks, including one with exact `odt`
  - Tier 3 block sum
- **Side finding:** 2013 rows with `sign = 1` have `odt = 1970-01-01` on both sides.

## Open

- Tier 1 speed. Two ideas:
  - bucket keys by `kh % 16` for a parallel merge (`sumDistinct` sums add up mod 2^64)
  - window merging is now done in `plan_windows` but has not been measured
- Script the Tier 3 invariants (balance chain first).
- Generate the summary from the caches.
