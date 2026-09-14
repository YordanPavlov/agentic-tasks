# XRP balances async re-run — output QA vs prod (2026-09-14)

Comparison of the async-state re-run's output (`test.xrp_balances_test`, shard `v10`,
image `29c50449`, fresh bootstrap started 2026-09-11) against the production table
(`default.xrp_balances`, shard `v8`, the old window pipeline). ClickHouse production
cluster, all queries as `-u readonly`.

**Verdict: PASS.** All 148 comparable months (2013-01 → 2025-04, ~9.93B deduplicated
rows) match: identical key sets, bit-identical non-float columns, float sums within
1e-9 relative. The new run had reached block 95,833,461 (chain time 2025-05-01) when
the comparison ran; months beyond that weren't comparable yet.

This is the "output equivalence check vs prod on the old image" from the main doc's
next steps. The planned re-run after the `Order_balances` heap-buffer fix
(`a0370199`, ex-`76a73cb1`) should repeat this process — see the checklist at the end.

## Method

Raw row counts are NOT comparable — both tables are `ReplicatedReplacingMergeTree`
and carry unmerged duplicates (old: 16.2B raw vs ~9.9B distinct, from years of
restart re-emissions; new: 14.7M dups from an uncleared first run, see insights).
Everything is therefore compared after **dedup by GROUP BY the ORDER BY key**
`(dt, assetRefId, address, blockNumber, transactionIndex)`.

Per month × table, one aggregate row (see `compare_months.sh`):

| column | meaning | check |
|---|---|---|
| `count()` | distinct keys | equal |
| `sum(kh)` | key-hash sum, kh = cityHash64(key cols) | equal → same key *set* (mod-2^64 sum, order/dup-insensitive) |
| `sum(cityHash64(kh, min_eh))`, `…max_eh…` | eh = hash of all non-float cols (currency, issuer, issuerCurrency, oldDt, oldBlockNumber, addressType, transactionHash; NULLs canonicalized with isNull flags); min/max across a key's duplicate versions, bound to the key | all four values equal (old min = old max = new min = new max) → non-float content bit-identical AND no conflicting versions inside either table |
| `sum(bal)`, `sum(abs(bal))`, same for oldBalance | bal = avg(balance) per key | old vs new within 1e-9 relative (abs sum as denominator) |
| `max(spread)` | max(balance)−min(balance) within a key | internal float-consistency indicator |
| `sum(dups)` | rows minus keys | duplication census |

`analyze.py` sums the per-bucket rows (hash sums mod 2^64 — address buckets
partition the key space, so results are additive) and prints failures.

### Why not FINAL / plain hash multiset

- `FINAL` doesn't dedup across shards on a Distributed table; GROUP BY the key does,
  and is immune to in-progress merges.
- A whole-row hash multiset (the ETH stacks proof) **cannot pass here even old-vs-old**:
  the old table contains ~0.2% of keys with duplicate versions that differ in the last
  float bit (restarts recomputed IOU balances via slightly different float paths, e.g.
  `3.27252e-7` vs `3.2725199999999997e-7`). Hence: exact hash proof for everything
  except the two float columns + tolerance check for the floats.

### Runbook

```bash
# 1. Scope: how far has the new run got?
#    (also sanity: min dt/block should equal prod's chain start: block 38129, 2013-01-02)
SELECT max(dt), max(blockNumber) FROM test.xrp_balances_test

# 2. Month list + bucketing input (comparable months only, i.e. dt < first incomplete month):
clickhouse-client -h clickhouse.production.san --port 30900 -u readonly --format=TSVWithNames --query="
SELECT month, sumIf(c, src='old') AS old_rows, sumIf(c, src='new') AS new_rows, new_rows - old_rows AS diff
FROM (
  SELECT 'old' AS src, toYYYYMM(dt) AS month, count() AS c FROM xrp_balances WHERE dt < '2025-05-01' GROUP BY month
  UNION ALL
  SELECT 'new', toYYYYMM(dt), count() FROM test.xrp_balances_test WHERE dt < '2025-05-01' GROUP BY toYYYYMM(dt)
) GROUP BY month ORDER BY month" > workdir/monthly_counts.tsv

# 3. Sweep (background; ~2.5 h for 148 months, old+new in parallel per month):
./compare_months.sh workdir     # edit NEW_TABLE at the top if the table name changes
# progress: tail workdir/progress.log; failures: workdir/errors.log

# 4. Verdict (rerun any time mid-sweep for interim results):
./analyze.py workdir
```

Bucketing/memory: months are split into `ceil(max_raw/120M)` address-hash buckets
(`cityHash64(address) % K = b`) and each query carries `--max_memory_usage=25000000000`.
Three old-side buckets of the biggest months (2024-01/02/07) still tripped the guard —
re-run failed pieces with 4× finer splits (`%6=2` → `%24 ∈ {2,8,14,20}` etc.; label
them with any distinct bucket id and append to the same results.tsv — the analyzer
just sums per month). One slice needed disk spill on top:
`--max_bytes_before_external_group_by=10000000000 --max_memory_usage=30000000000
--distributed_aggregation_memory_efficient=1`. The prod `readonly` user is
`readonly=2`, so per-query settings are allowed.

## Results (this run)

- 148/148 months PASS on all checks; total sweep ~2.5 h + ~15 min retries.
- Key sets exactly equal every month; non-float columns bit-identical; float sums
  within 1e-9 (most months within float noise, many bit-exact).
- New table internally clean: zero conflicting versions (max float spread across its
  duplicate versions = 0.0).

## Insights / gotchas (keep for the next run)

1. **Uncleared first run = benign, diagnosable duplicates.** The new table held 14.7M
   duplicate rows spanning genesis → block 2,694,975 exactly — the output of the first
   deploy attempt (2026-09-11 10:38–16:14, the pre-fix ~700 rec/s run) which was
   restarted from scratch without truncating the table. All duplicates bit-identical
   (deterministic job), so ReplacingMergeTree merges absorb them — the 2013-01/02
   partitions had already merged clean while 2013-03..10 still showed raw dups. If a
   future re-run reuses a table, expect the same pattern; the dedup comparison is
   immune to it. The table has **no computed_at column** — the duplicate block-range
   boundary is what identifies the run.
2. **Old-table duplication is massive and self-inconsistent.** 16.2B raw vs ~9.9B
   distinct rows; dup versions disagree on floats — mostly ULP noise, but spreads up
   to 1.4e14 (2018–2019 months) and 2.4e80 (monster-IOU months). Non-float columns
   never disagreed. The new run's output is strictly cleaner than prod's.
3. **Monster IOU balances weaken the sum-based float check.** XRP IOUs legitimately
   reach ~1e95; ~60 months from 2018 on have abs-balance sums 1e30–1e99, so a 1e-9
   relative tolerance there can't see discrepancies in normal-sized balances. Not
   closed this round (non-float proof + clean normal months judged sufficient).
   Options if wanted next time: per-issuerCurrency float sums, or a native-XRP-only
   pass (the balances that feed metrics).
4. `assetRefId` is MATERIALIZED (`cityHash64('XRP_' || issuerCurrency)`) on both shard
   tables — safe to use in the key hash, and it stands in for issuerCurrency there.
5. Old shard is `xrp_balances_shard_v8` (zk path says v9), new is `…_v10`; both
   Distributed over `default_cluster`, partitioned by `toYYYYMM(dt)` — `WHERE
   toYYYYMM(dt) = M` prunes to one partition, which is what makes the per-month sweep
   cheap.
6. `sum()` of UInt64 hashes wraps mod 2^64 in ClickHouse — that's what makes the
   bucketed sums additive; the analyzer reduces mod 2^64 too.

## Files

- `compare_months.sh` — the sweep (edit table names at the top).
- `analyze.py` — bucket aggregation + verdict.
- Raw results of this run were in the session scratchpad (ephemeral); the doc above
  records everything that mattered.
