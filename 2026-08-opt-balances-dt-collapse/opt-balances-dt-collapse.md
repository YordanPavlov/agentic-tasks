# Same-second collapse in dt-keyed balances/stacks tables

## At a glance

- **Goal:** fix balance/stack records lost to same-second blocks on chains
  with sub-second block times. Started from `opt_erc20_balances`
  (negative holder counts); turned out to be a **class of defect** across
  several chains and both balances and stacks tables.
- **Status 2026-09-01:** fix direction chosen; **4 re-keyed tables deployed
  and back-filling** from Kafka. Next: wait for catch-up, QA against the
  current tables, then switch and continue with the remaining tables.
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

## Deployed 2026-09-01 — back-filling

| Table | Key | Progress (local shard, 11:05) |
|---|---|---|
| `opt_erc20_balances_v2_shard` | `assetRefId, address, blockNumber` | 375M rows, partition 2025-03 — **pre-Bedrock era complete, QA can start** |
| `opt_erc20_stacks_shard_v2` | `assetRefId, address, sign, blockNumber, nonce` | 318M rows, partition 2023-07 |
| `arb_erc20_stacks_shard_v5` | `assetRefId, address, sign, blockNumber, nonce` | 205M rows, partition 2023-04 |
| `polygon_balances_shard_v2` | `address, blockNumber` | 80M rows, partition 2022-06 |

Plan: wait for catch-up → compare against the current tables → switch →
proceed to the remaining tables.

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
