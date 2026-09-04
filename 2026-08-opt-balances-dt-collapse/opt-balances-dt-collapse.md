# Same-second collapse in dt-keyed balances/stacks tables

## At a glance

- **Goal:** fix balance/stack records lost to same-second blocks on chains
  with sub-second block times. Started from `opt_erc20_balances`
  (negative holder counts); turned out to be a **class of defect** across
  several chains and both balances and stacks tables.
- **Status 2026-09-04:** the first 4 re-keyed tables **passed QA and are
  live** — Distributed tables switched, old shards dropped. Next: metric
  re-runs over the damaged ranges, then the remaining tables
  (avax balances first — still accruing damage).
- **Root cause:** these tables are `ReplacingMergeTree` with `dt` (second
  resolution) in the sorting key instead of `blockNumber`. When a chain puts
  several blocks in one second, one address gets several rows with identical
  `dt`; merges keep one and the intermediate transitions vanish. The Flink
  output is provably correct — this is purely a table-key bug.

## Why metrics go negative

A `positive → 0` row can survive while its matching `0 → positive` row (same
second) collapses. `ERC20BalancesSignedBase` derives holder deltas by
`ARRAY JOIN [-1,+1] × [oldBalance, balance]`, so it decrements without the
offsetting increment. `cumulative_sum_job` then carries the error forward
forever (`prev_value + arrayCumSum(delta)`, `depends_on_past: True`). That
job already carries a `_make_negative_value_debug_hook` dumping negative
cumsums to `test.debug_cumsum_negative_values` — same symptom, seen from the
metric end.

## The fix: re-key + full Kafka re-ingest

**No recomputation and no Flink change needed.** The output topics have no
retention, so the entire history is still in Kafka:

- A new `ClickhouseTableKafkaStream` gets a fresh timestamped
  `kafka_group_name` from the operator and starts at the earliest offset, so
  the new MV back-fills all history and then follows the live tail — v1 and
  v2 can consume the same topics side by side.
- Precedent: `erc20_balances_stream_v20` was created 2025-12-22 12:27:05 and
  2016-dated rows in `erc20_balances_shard_v20` carry `computedAt`
  12:27:30 — i.e. that migration was done exactly this way.
- Shard placement is preserved: each broker consumes only
  `<topic>.clickhouse-{host_index}`, and the sink picks the topic as
  `floorMod((contractAddress, address).hashCode, 3)`
  (`AddressBalancesCalculation.scala:425`) — deterministic, so a re-ingest
  puts every key on the shard it already lives on. (Matters because `FINAL`
  does not dedup across shards.)

**Key choice — use `blockNumber`, deliberately drop `dt`:**

- balances → `(assetRefId, address, blockNumber)` (as `arb_..._v4` already has)
- stacks → `(assetRefId, address, sign, blockNumber, nonce)`

This mirrors the producer's own identity:
`AccountModelChange.primaryKey = (blockNumber, contractAddress, address, nonce)`
and `serializationKey = sign-contract-blockNumber-address-nonce`
(`package.scala:115-118`). `nonce` uniqueness is guaranteed *per block*, not
per second — `HandlerOneAccountChange.scala:60-67` says a re-appearing
address restarts its nonce counter and is "collision-free **because its new
rows carry a later block**". Keeping `dt` in the key (the ICP/ICRC shape)
would also fix the collapse, but `dt` is derived data: a corrected block
timestamp would then create a new row instead of replacing the old one.
`PARTITION BY toStartOfMonth(dt)` stays, so dt-range pruning is unchanged.

## SWITCHED 2026-09-04 — first 4 tables live on v2

Distributed tables repointed and old shards dropped (operator, 2026-09-04):
`opt_erc20_balances` → `opt_erc20_balances_v2_shard`, `opt_erc20_stacks` →
`opt_erc20_stacks_shard_v2`, `arb_erc20_stacks` → `arb_erc20_stacks_shard_v5`,
`polygon_balances` → `polygon_balances_shard_v2`. Verified via
`system.tables.engine_full`.

## QA verdict (2026-09-04): all 4 pass — strict supersets, nothing missing

All counts are deduplicated keys (`GROUP BY` new ORDER-BY key), full history,
cutoff 2026-09-03. Evaluated via temporary `test.*_test` Distributed tables.

| Table | v1 rows | v2 rows | Recovered | Fully-lost blocks¹ | Damage window |
|---|---|---|---|---|---|
| `opt_erc20_balances` | 3,177.5M | 3,344.8M | **167.3M (5.3%)** | **8,171,421** | 2021-11 → 2023-06 |
| `polygon_balances` | 2,031.8M | 2,031.9M | 29,979 | 71 | 2022-02 → 2023-12 + 2025-09-10 gap |
| `opt_erc20_stacks` | 9,100.6M | 9,100.6M | 3,808 | 0 | 2025-12-22 gap only |
| `arb_erc20_stacks` | 8,378.8M | 8,378.8M | 4,051 | 10 | 2025-12-22 gap only |

¹ blocks that vanished from v1 entirely — every record overwritten by a
same-second successor.

Evidence, in increasing strength:

1. **Continuity** (`oldBalance[N] == balance[N-1]`, 1% address sample): opt
   v1 had 304,434 breaks + 1,973 impossible-genesis rows; polygon v1 122
   breaks; **v2 = 0 everywhere**.
2. **End-state equality** (last row per key at cutoff): zero keys missing
   either side; only diffs are 1-ULP float-parse noise (max 2.2e-16 rel —
   today's CH parses the same Kafka bytes with different last-bit rounding).
3. **Row-level anti-joins** (1% balances / 0.2% stacks samples, all months):
   **v1-only = 0 everywhere**; v2-only confined to the damage windows.
4. **XOR fingerprint** (groupBitXor over ALL distinct key hashes, per month,
   full coverage — not a sample): every equal-count month (213 across the 4
   pairs) is **provably identical**; only superset months differ.
5. Reproducer address shows all 10 links in v2 (v1: 2).
6. v2's unmerged same-key duplicates (opt_stk 2024-10, arb_stk 2021-09, pol
   2020-21) carry byte-identical values on real addresses — merge fodder,
   not conflicts. Only value-conflicting keys: 42k polygon `TOTAL` sentinel
   rows differing in txID only (v1 has the same ambiguity).

**Damage assessment of the old data:** `opt_erc20_balances` was the
disaster — inside its 20-month window **38.7% of transitions were missing**
(worst month 2022-01: 65.8%); 98.1% of pre-Bedrock blocks shared their
second with another block. Extrapolated: ~29.5M broken chains, ~190k
addresses with lost genesis rows. End states were intact — the corruption
was entirely in the *path*, exactly what delta/holders/network-growth
consume. The other three tables were scratches.

**Two incidents discovered incidentally, both healed by the re-ingest:**

- **2025-12-22 02:37–02:39 UTC consumer drop** — the *entire* diff of both
  stacks tables (3,808 opt + 4,051 arb rows, 10 arb blocks fully gone; opt
  balances unaffected). Checked all other balances/stacks tables per-minute
  around the window: no dips (caveat: a partial drop this small is only
  provable by re-ingest diff). Stacks-fed metrics for that day need a re-run.
- **polygon_balances 2025-09-10 04:00–06:00** partial gap (385 keys).

**Key empirical surprise:** the dt-collapse **never actually fired in any
stacks table** — the `nonce` in the old key absorbed every same-second
collision on opt AND arb ("was accruing" in the matrix below was wrong).
All real collapse damage lives in balances tables. Stacks re-keying is
still right (correct identity, healed the gap), but it's not urgent.

## Remaining affected tables (2026-09-04 survey of live sorting keys)

Measured damage: chain breaks on 1% address sample (×97 ≈ table-wide).

| Table | Sampled breaks | Est. lost rows | Priority |
|---|---|---|---|
| `avax_erc20_balances` (shard_v2) | 12,117 — **8,483 in 2026 YTD** | **~1.2M, compounding** | **1 — accelerating**: multi-block s/month ~600 (early 2025) → 59,349 (2026-06) |
| `avax_erc20_stacks` (shard) | n/a | likely ~0 (nonce) but avax collision rate ~100× opt/arb | 2 — same re-ingest batch as avax balances |
| `polygon_erc20_balances` (shard_v2) | 1,372 + 75 bad-genesis | ~133k + ~7k genesis | 3 — historical (bulk 2022, ~20/yr by 2025) |
| `polygon_stacks` (shard_v2) | n/a | likely ~0 | 3 — chain had only 8,400 shared-second blocks ever |
| `xrp_stacks_shard_v8` | n/a | ≤ a handful | 4 — exactly ONE multi-ledger second ever (2025-08-09 03:00:30, 10 ledgers); fold into future XRP work. **Matrix correction:** xrp stacks key is `contractAddress, address, sign, dt, nonce` — no blockNumber, contrary to the matrix below. |

**Cleared with data:** `bep20_balances_exact_v3_shard` — BSC is now the most
sub-second chain of all (9.7M excess blocks Jun–Aug 2026) but `txID` in its
key keeps same-second rows distinct: 0 chain breaks in the sub-second-era
sample. All UTXO/eth/erc20/icp/icrc + `xrp_balances`: safe keys or slow
blocks.

## What's left to be done

1. **Metric re-runs** for the 4 switched tables (scope section below):
   opt Tier-1 chronologically over 2021-11 → 2023-06, Tier-2 (cumsums,
   composite) through today; polygon equivalents over its windows;
   one-day re-run around **2025-12-22** for opt/arb stacks-fed metrics
   and **2025-09-10** for polygon balances-fed ones.
2. **avax_erc20_balances_v3** (+ avax stacks) — deploy re-keyed tables,
   same Kafka re-ingest recipe. Only table still actively accruing damage.
3. **polygon_erc20_balances + polygon_stacks** re-key — no urgency.
4. Drop the `test.*_test` evaluation Distributed tables (operator).
5. Consider table_qa test files for the switched tables to lock in expected
   values (santiment-add-table-qa-tests skill).

## Affected tables — full matrix

A dt-keyed table is affected iff its chain has ever had >1 block in a second.

| Table | Affected | Note |
|---|---|---|
| `opt_erc20_balances_shard` | yes, not accruing | sub-second 2021 → Bedrock 2023-06-06; clean since. **v2 deployed** |
| `opt_erc20_stacks_shard_v1` | yes, not accruing | same window. **v2 deployed** |
| `arb_erc20_stacks_shard_v4` | yes, was accruing | Arbitrum still sub-second. **v5 deployed** |
| `arb_erc20_balances_shard_v4` | no | already re-keyed 2024 (devops #4388) |
| `avax_erc20_balances_shard_v2` | **yes, still accruing** | rate rose ~200x in 12 months (0.002% → 0.49% of keys). No v3 yet — highest priority |
| `avax_erc20_stacks_shard` | **yes, still accruing** | same chain |
| `polygon_balances_shard_v2` | in progress | native POL. **v2 deployed** |
| `polygon_erc20_balances_shard_v2` | yes, not accruing | multi-block seconds in 2023; none on recent sampled days |
| `polygon_stacks_shard_v2` | yes, not accruing | same |
| `erc20_*`, `eth_*`, `eth_beacon_*` | no | 12 s blocks/slots |
| btc/bch/ltc/doge/cardano stacks | no | block times >> 1 s; their balances tables already key on `blockNumber, txPos` |
| `icp_*`, `icrc_*`, `xrp_*` | no | already include `blockNumber` |

Exposure was established from single-day samples per year — enough to answer
"affected", not enough to bound the corrupted range precisely.

## QA checklist before switching

1. **Continuity** — per `(assetRefId, address)` ordered by `blockNumber`,
   `oldBalance[N] == balance[N-1]`. ~0 violations in v2, many in v1. This is
   the direct test of the bug.
2. **End-state equality** — last row per `(assetRefId, address)` at a fixed
   cutoff must match v1 exactly. v2 only *adds* intermediate transitions; a
   changed final balance means the re-ingest diverged.
3. **Row counts by month** — v2 > v1 in the corrupted range, v2 == v1 outside
   it (also catches a re-ingest that silently started at the tail).
4. **No duplicate keys** — `count()` vs `GROUP BY <ORDER BY key>`, per shard.
5. **Reproducer address** shows its full chain (below).
6. **Stacks-specific** — `sum(sign * amount)` per `(assetRefId, address)` up
   to T should equal the balance at T.

Switch itself needs no downtime: metric jobs resolve the table via
`context.source_table('opt_erc20_balances', ...)`, so a config flip is
instant and reversible; `EXCHANGE TABLES` if the name must be preserved.
Keep v1 until the metric re-run is validated. Watch disk — v1 and v2 coexist.

## Metric re-run scope (after the switch)

**Tier 1 — read balances directly, re-run over the corrupted range:**
`opt-erc20-active-addresses-intraday-deltas`,
`opt-erc20-holders-distribution-deltas`,
`opt-erc20-active-holders-distribution-deltas`,
`opt-erc20-network-growth`. Chronologically — `depends_on_past`.
(Network growth fails differently: it counts rows with `oldBlockNumber IS
NULL`, so a collapsed genesis row makes an address uncountable — a silent
undercount.)

**Tier 2 — must extend to *today*, not just the corrupted range**, because
cumsums carry the error forward: `opt-erc20-cumulative-sums-intraday` (+
daily equivalent) and `opt-erc20-composite-intraday-metrics`.

**Not affected:** everything fed by transfers or prices (transaction
volume/size, whale transactions, prices, volatility, RSI). The
stacks-fed jobs (age distribution, network profit/loss, stack age/price
consumed) *are* affected via the stacks tables.

## Reproducer

Address `0xa689913a965e33fa9f8b2e35c3da159e01b2f429`, DAI
`0xda10009cbd5d07dd0cecc66161fc93d7c9000da1`, Nov 2021. In
`opt_erc20_balances FINAL` only two rows survive (blocks 39431, 96125), both
`balance = 0` with non-zero `oldBalance` — the address's first row already
has a non-zero `oldBalance`, which looks impossible. It is a relay: each
incoming transfer is forwarded out in full a few blocks later, same second:

- `08:49:20`: +138.40 @39424 → out @39427 → +122.98 @39428 → out @39431
- `14:45:22`: +131.67 @96103 → out @96105 → +146.55 @96107 → out @96110 →
  +100.02 @96124 → out @96125

The surviving row `old = 122.98 @39428 → 0 @39431` is exactly the correct
*last link*; earlier links collapsed. When reproducing, query transfers with
**both** `to =` and `from =` — filtering only `to =` hides the outgoing legs.

Also a good stacks reproducer: this address empties and refills its stack
several times per second, which is exactly the nonce-reuse trigger.

## False trails worth remembering

- Both balances jobs *do* have silent late-record drop paths
  (`StreamDeduplicator` side-output; `isLate` at
  `AddressBalancesCalculationFlatMap.scala:152`), and event time = **block
  number** via `EventAwareWatermarkStrategy` (`KafkaSource2.scala:152`).
  Real mechanisms, but NOT the cause here — input order was fine.
- Raw (non-FINAL) rows showed two independent computations (`computedAt`
  2022-10-20 and 2023-11-14) emitting byte-identical values — deterministic.
- `opt_erc20_balances` has `min(dt) = 2017-07-12`, before Optimism existed —
  a few junk-timestamp rows. Don't let them drive a backfill range.

## Session log

### 2026-08-28 — investigation

Started from user-reported negative holder count. Read
`ERC20AddressBalances` / `ERC20AddressBalancesExact` /
`AddressBalancesCalculation(FlatMap)` / `ERC20TransfersSource` /
`StreamDeduplicator`; suspected the late-drop paths, disproved by data.
`SHOW CREATE TABLE opt_erc20_balances_shard` → the ORDER BY/version finding.
Environment note: container has no Kafka CLI (`/opt/kafka/bin` absent, no
kcat); `kafka-python` works but is flaky through the proxy.

### 2026-09-01 — fix direction chosen, first tables deployed

- Rejected the metric-side fix: the consumers are shared base classes in
  `daily_metrics/job_functions/erc20_balances.py`, and deriving transitions
  from consecutive surviving rows needs a look-back before each job's
  interval, for every chain.
- Established the re-ingest mechanism and its precedent (`erc20_..._v20`),
  so no Flink re-run is needed.
- Generalised the defect: surveyed every balances/stacks table's sorting key
  and each chain's blocks-per-second → the matrix above. Found Arbitrum
  stacks was still accruing loss, and **avax is accruing and accelerating**.
- Settled the stacks key from the producer's own `primaryKey` /
  nonce-continuity comment rather than assuming nonce uniqueness per second.
- Deployed and started back-filling: `opt_erc20_balances_v2_shard`,
  `opt_erc20_stacks_shard_v2`, `arb_erc20_stacks_shard_v5`,
  `polygon_balances_shard_v2`.

### 2026-09-04 — QA passed, tables SWITCHED

- Backfill caught up (max dt equal to old tables). Operator deployed
  `test.*_test` Distributed tables for evaluation.
- Ran the full QA ladder (see QA verdict section): per-month deduped key
  counts, continuity, end-state equality, row-level anti-joins on samples,
  and a full-coverage per-month XOR fingerprint over all distinct keys.
  All 4 tables strict supersets of old; recovered rows only in explicable
  windows; zero rows missing.
- Quantified old-data damage (opt balances: 167M rows / 8.2M whole blocks
  lost, 38.7% of the window). Discovered + healed two unrelated ingestion
  incidents (2025-12-22 stacks consumer drop, 2025-09-10 polygon gap).
- Established the nonce actually prevented all stacks-table collapse on
  both chains — damage class is balances-only in practice.
- Surveyed all remaining balances/stacks sorting keys on the live cluster;
  measured remaining damage (avax accelerating, ~1.2M rows; polygon erc20
  historical ~140k; xrp stacks one second of exposure; bep20 cleared by
  txID in key).
- Operator switched the 4 Distributed tables to v2 shards and dropped the
  old shard tables. Verified via `system.tables`.
- Gotchas for next time: `readonly` gets 600s max_execution_time (pass
  `--receive_timeout` too for long queries); double-distributed IN needs
  GLOBAL; v1 tables carried millions of unmerged identical duplicates from
  historical double-computations — only deduped counts are comparable.
