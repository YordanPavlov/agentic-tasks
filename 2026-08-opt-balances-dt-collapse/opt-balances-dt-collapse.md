# opt_erc20_balances same-second collapse — missing records, negative holder counts

## At a glance

- **Goal:** fix `opt_erc20_balances` losing balance records for same-second
  blocks (pre-Bedrock Optimism), which corrupts downstream metrics (e.g.
  negative holder count).
- **Status:** root cause CONFIRMED 2026-08-28; **no fix chosen yet — next
  session picks a fix direction and plans the reload.**
- **Headline finding:** not a Flink bug. `opt_erc20_balances_shard` is
  `ReplicatedReplacingMergeTree(..., dt)` with `ORDER BY (assetRefId,
  address, dt)` — `blockNumber` is not in the sorting key and `dt` (second
  resolution) is also the *version* column. Pre-Bedrock Optimism has one tx
  per block and many blocks per second, so one address can get several
  balance rows with identical `dt`; merges keep only the last-inserted
  (highest-block) row per second and the intermediate transitions vanish.
  The Flink job's emitted chain is provably correct.

## Reproducer

Address `0xa689913a965e33fa9f8b2e35c3da159e01b2f429`, DAI
`0xda10009cbd5d07dd0cecc66161fc93d7c9000da1`, Nov 2021:

- `opt_erc20_balances FINAL` shows only two rows (blocks 39431, 96125), both
  `balance = 0` with non-zero `oldBalance` — the first row of the address's
  history already has a non-zero `oldBalance`/`oldBlockNumber`, which looks
  impossible.
- The address is a relay: each incoming transfer is forwarded out in full a
  few blocks later, all inside the same second:
  - `08:49:20`: +138.40 @39424 → out @39427 → +122.98 @39428 → out @39431
  - `14:45:22`: +131.67 @96103 → out @96105 → +146.55 @96107 → out @96110 →
    +100.02 @96124 → out @96125
- The surviving row `old = 122.98 @39428 → 0 @39431` is exactly the correct
  *last link* of that chain; the earlier links collapsed away. (Caveat when
  reproducing: query transfers with **both** `to =` and `from =` — filtering
  only `to =` hides the outgoing legs and makes the data look wrong.)

## Evidence the computation is correct

- Raw (non-FINAL) rows show two independent computations (`computedAt`
  2022-10-20 16:33 and 2023-11-14 17:12/13) emitted byte-identical values —
  deterministic.
- Cross-checked other same-second multi-block cases (DAI, 2021-11-12:
  `0x3f4db615bd9042a21e64e5ff37591d3f44cb351a` @06:36:40,
  `0xadb35413ec50e0afe41039eac8b930d313e94fa4` @03:35:05): every surviving
  row chains arithmetically perfectly against the complete transfer set.
  Nothing dropped anywhere.
- False trail worth remembering: both balance jobs *do* have silent
  late-record drop paths (`StreamDeduplicator` side-output in the windowed
  job; `isLate` at `AddressBalancesCalculationFlatMap.scala:152` in the
  exact job), and event time = **block number** via
  `EventAwareWatermarkStrategy` (`KafkaSource2.scala:152`), so the 1 ms
  windows are per-block. These mechanisms are real but were NOT the cause
  here — input order was fine.

## Why metrics go negative

A `positive → 0` row can survive the merge while its matching
`0 → positive` row (same second) collapsed away. Anything deriving holder
deltas from per-row `oldBalance → balance` transitions decrements without
the offsetting increment → negative holder counts.

## Fix options (undecided)

1. **Schema fix:** add `blockNumber` to the sorting key (new table +
   reload). No recomputation needed — the Flink output is correct — reload
   from the balances Kafka output topic if retention allows, else re-run the
   backfill. Needs the usual devops/table-migration dance and a decision on
   the version column (e.g. `computedAt`).
2. **Metric-side fix:** keep the schema; make consumers derive transitions
   from *consecutive surviving rows per address* instead of trusting each
   row's `oldBalance`.

Blast radius: ETH mainnet unaffected (unique `dt` per block). Any chain with
multiple blocks per second is exposed — pre-Bedrock Optimism (this case;
post-Bedrock has 2 s blocks, so only the historical range is corrupted),
possibly pre-Nitro Arbitrum — check other `*_erc20_balances` tables with the
same `ORDER BY` shape before deciding.

## Session log

### 2026-08-28 — investigation

- Started from user-reported discrepancy (negative holder count; the two
  balance rows above).
- Read `ERC20AddressBalances` / `ERC20AddressBalancesExact` /
  `AddressBalancesCalculation(FlatMap)` / `ERC20TransfersSource` /
  `StreamDeduplicator` in etherbi-flink; initially suspected the late-drop
  paths; disproved by data.
- `SHOW CREATE TABLE opt_erc20_balances_shard` → the ORDER BY/version
  finding. Raw-row and cross-address checks as above (prod ClickHouse,
  readonly).
- Environment note: container lacks Kafka CLI (`/opt/kafka/bin` absent, no
  kcat); `kafka-python` 3.0.8 present. Turned out unnecessary. If topic
  inspection is needed later, download the Kafka tools binary (GET works
  through the proxy) and keep it in the container.
