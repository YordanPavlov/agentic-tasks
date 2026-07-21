# Flink TM memory distribution — right-sizing the hprod fleet

## Goal

Make the k8s memory **request** of each Flink job in
`devops/hprod/k8s-apps/flink-jobs-operator/` track its real daily usage, with
burst headroom carried by the k8s **limit** (chart sets `limit-factor: 2`),
while fixing the direct-buffer OOM exposure several jobs still have — and,
longer-term, settle on a memory *profile* that fits our workload (huge RocksDB
state, simple JSON-transform job graphs) instead of Flink's batch-oriented
defaults.

## Constraints discovered (why the obvious model doesn't work)

- The flink-kubernetes-operator sets `taskmanager.memory.process.size` **equal
  to** `taskManager.resource.memory` = the pod request (spec is applied after
  `flinkConfiguration`, so overriding process.size in `flinkConfigOverrides`
  is ignored). Native integration derives the pod request from process.size —
  there is no `request-factor`, only `limit-factor`.
- All JVM hard ceilings (`-Xmx`, `-XX:MaxDirectMemorySize`) are carved from
  process.size, i.e. from *inside the request*. The request→limit range is
  reachable only by non-JVM native memory (RocksDB overshoot, glibc, OS page
  cache). Direct-buffer burst headroom therefore **must live inside the
  request**; it cannot be moved to the limit.
- Flink cannot rebalance pools at runtime (JVM flags are fixed at startup;
  managed memory is deliberately a fixed pool). "Distribute as appropriate"
  can only mean better numbers per restart: chart-level defaults, or the
  operator's autoscaler memory autotuning
  (`job.autoscaler.memory.tuning.enabled`, available in operator 1.13.0).

## Fleet survey (2026-07-20, hprod, live REST + `kubectl top`)

| Deploy | Request | RSS | Heap used/max | Managed used/total | Direct used/ceiling | Overrides |
|---|---|---|---|---|---|---|
| btc-balances-v12 | 15.6Gi | 10.8Gi | 6.2/8.8g | 1.4/1.4g | 1.4/4.0g | off-heap recipe |
| bch-balances-v13 | 15.6Gi | 11.9Gi | 3.5/8.8g | 1.4/1.4g | 1.5/4.0g | off-heap recipe |
| doge-balances-v2 | 15.6Gi | 10.9Gi | 4.0/8.8g | 1.4/1.4g | 1.4/4.0g | off-heap recipe |
| ltc-balances-v5 | 15.6Gi | 8.5Gi | 2.8/8.8g | 1.4/1.4g | 1.4/4.0g | off-heap recipe |
| bep20-balances-v3 | 15.6Gi | 13.1Gi | 2.9/7.2g | 5.9/5.9g | **1.5/1.6g** | none |
| erc20-address-balances-v18 | 2×8Gi | 4.3Gi ea | 0.9/3.4g | 2.8/2.8g | **0.73/0.84g** | none (pre-edit) |
| erc20-balances-exact-v2 | 4Gi | 1.9Gi | 0.3/1.6g | 1.4/1.4g | 0.35/0.47g | none |
| btc/bch/doge/ltc-stacks | 15.6Gi ea | 5.2–9.6Gi | 0.6–3.8/7.2g | 2.0/5.9g | **1.5/1.6g** | autoscaler only |
| polygon-erc20-balances-v2 | 8Gi | 4.3Gi | 2.0/2.9g | 0.7/0.7g | 0.7/3.2g | recipe |

Totals: ~195Gi requested vs ~106Gi used. **Bold** = <120 MiB slack under
`-XX:MaxDirectMemorySize` — the polygon/btc-balances "Cannot reserve direct
buffer memory" → heartbeat-timeout failure signature.
erc20-address-balances-v18 is the same job class as the polygon job that
actually OOM'd, on the identical default split.

Notes: "managed 100% used" = RocksDB cache filled, says nothing about hit
rate. Direct usage ≈ network pool fully allocated (normal) + a sliver; the
risk is everything else sharing the direct cap (Kafka clients, checkpoint
transfers). Stacks TMs restarted 2026-07-20 08:05 (node event on `heimdall`),
so their snapshots are young; balances pods were 22–33 days old.

## The distribution debate (unresolved)

- Flink's default gives **40% of Flink memory to managed** — sized for batch
  sort/join arenas. For pure streaming, managed is only the RocksDB
  block-cache/memtable budget, task off-heap gets **0** by default (the OOM
  root cause), and task heap gets the ~48% remainder.
- First instinct (mirror the proven btc-balances recipe: managed 0.1,
  off-heap 2g+512m) was challenged, correctly: our jobs are **state-heavy**
  (full historical ledger, read-modify-write per address; state = hundreds of
  GB in checkpoints), RocksDB is the fleet's main bottleneck, and shrinking
  block cache makes the slow part slower. Mitigating nuance: block-cache
  misses mostly land in the OS **page cache** (compressed blocks, outside the
  JVM budget, can use the request→limit gap) — why btc-balances survives on
  ~470m/slot managed. Still slower per-read than block cache.
- The demonstrably **idle pool fleet-wide is heap** (0.6–3.8g used of 7.2g on
  stacks; 2.8–4g of 8.8g on most balances). So a state-heavy profile would
  take the off-heap safety margin out of *heap*, not managed — e.g. for a
  16000m stacks TM: managed ~0.35 (≈today's usable cache), task off-heap
  1–2g, heap shrinks to ~5g as remainder.

## Code-scan evidence for "heap should be lean" (etherbi-flink, agent sweep)

The gut feeling is supported. The three main pipelines
(`ERC20AddressBalances`, `ERC20AddressBalancesExact`, `UTXOAccountChanges`)
are structurally heap-lean: state lives in RocksDB (off-heap/disk), Jackson
mappers are shared singletons (no per-record parser), per-record/per-block
materializations are transient and bounded by one block/window. Steady-state
heap readings of 2–6g are therefore mostly **GC slack** (used ≠ live; big Xmx
lets old-gen fill before collection), not live data — a heap histogram on a
hot TM would confirm.

One legitimate multi-GB heap structure exists:
- Static Scaffeine cache in `BalancesProxyServicePerContract.scala:122-129`
  (companion-object `var`, one per TM, survives keying), fed by
  `EthClientProxyCache.getBalances`. `balancesCacheRowLimit` (default 10000,
  `Config.scala:180`) is overloaded as both max pages **and** page width in
  blocks — worst case tens of GB. Only populated on the negative-balance
  correction path and only when `ethClientProxyUrl` is set —
  **erc20-address-balances-v18 sets it**; balloons during backfills.
- Also: `rocksDBConfig` is read (`Config.scala:182`) but never applied —
  RocksDB tuning from job config is dead code.
- Kafka producer uses `batch.size=512KB`, default 32MB `buffer.memory` — tens
  of MB per sink subtask, not GB.

### Verification pass (2026-07-20, direct source read)

Confirms the sweep, with quantification of "heavy on state access":

- **Stacks** (`HandlerOneAccountChange` + `StackChangesState`): ≥5 RocksDB ops
  per account change — 2 gets of the *same* sizeNonce key (`getLastNonceFromState`
  and `getLastSizeFromState` each call `getSizeNonceState.get(key)` separately —
  free win available), 1 get of the segment-batch blob, then commit does 1 put
  sizeNonce + 1 put of the whole batch blob. Each negative change that crosses a
  batch boundary adds a remove+get per batch. UTXO stacks use
  `SEGMENT_BATCH_SIZE = 10` (`ComputeUTXOAccountSegmentChanges.scala:42`), so
  write amplification is modest, but every change is a read-modify-write of a
  serialized blob.
- **ERC20/BEP20 balances** (`AddressBalancesCalculationFlatMap`): get+put on the
  per-block delta MapState per change; at block close, full iteration of the
  delta map (`getAllAddressCurrentBalances` materializes it — bounded by one
  block's touched addresses) plus get+put/remove on the historical balances
  MapState per touched address.
- **UTXO balances** (`UTXOBalanceCalculation`): get + put/remove on the UTXO-set
  MapState per output/input — keys are tx hashes, i.e. uniformly random point
  lookups over the largest state in the fleet; zero locality, so block-cache hit
  rate / bloom filters are the dominant lever here.
- **Heap holds nothing keyed long-term.** Window operators use 1 ms tumbling
  event windows = one block (timestamps quantized per block); pre-fire contents
  live in RocksDB ListState, materialized to heap only at fire
  (`input.asScala.toList` + groupBy/sort). Largest transient ≈ one busy block ×
  keys per subtask. Jackson mappers are object-level singletons; per-record
  parse output is young-gen garbage — allocation *rate*, not live set.
- **Scaffeine cache bound confirmed worse than "row limit":**
  `EthClientProxyCache.getBalances` has **no SQL `LIMIT`** — `rowLimit` is used
  only as the block-range width and a HashMap size hint, so one page holds
  *every* balances-cache row in a 10000-block span, and
  `resetCache(pageCacheSize)` allows 10000 such pages (`resetCache`'s own
  default is 10 — the call site passing `balancesCacheRowLimit` looks like a
  config-overload bug). Only live where `ethClientProxyUrl` is set
  (erc20-address-balances-v18).
- `rocksDBConfig` dead-code claim re-verified: parsed at `Config.scala:182`,
  consumed nowhere in src/ or integration/.
- Minor heap-resident oddballs: `web3ErrorsPerContrat` mutable.Map (grows with
  distinct failing contracts — tiny); `blockGauge` per subtask — negligible.

## Changes made so far (edited, NOT deployed; devops branch `flinkMemRequest`)

1. `polygon/polygon-erc20-balances/values.helm.yaml` — request 8192m→7168m,
   `jvm-overhead` pinned 512m (was min 1g/max 3g); direct ceiling ~3.2g kept.
   Pilot for the "shrink request toward usage" tranche.
2. `erc20/erc20-address-balances-v18/values.helm.yaml` — off-heap recipe
   added (managed 0.1, task off-heap 2g, framework off-heap 512m), request
   kept at 8192m. Rationale: identical failure signature to the polygon OOM.
   **Caveat given the debate below: managed 0.1 shrinks its RocksDB cache
   2847m→712m; may deserve the state-heavy profile instead.**
3. bep20 + stacks edits were started and **deliberately aborted** pending the
   distribution discussion.

## Advised next steps

1. **Observability before more tuning.** Extend the RocksDB metrics block in
   `global/flink-jobs-operator/flink-job-template/templates/flinkdeployment.yaml`
   with block-cache metrics. RocksDB/Flink expose hit/miss **counters**, not a
   ratio — compute `rate(hit)/(rate(hit)+rate(miss))` in Grafana. Exact option
   keys for Flink 2.1 still need verification (docs or
   `RocksDBNativeMetricOptions` in the flink-statebackend-rocksdb jar):
   expected `state.backend.rocksdb.metrics.block-cache-hit`, `.block-cache-miss`,
   plus `.block-cache-usage` / `.block-cache-capacity` gauges.
2. With hit-ratio data, decide the fleet profile per family:
   - read-latency-bound + poor hit rate → **raise** managed (0.35–0.4+), pay
     for it from heap; off-heap 1–2g for direct safety.
   - good hit rate at small cache (page-cache doing the work) → the 0.1
     recipe is fine and requests can shrink further.
3. **Direct-buffer safety for the five at-risk jobs** (bep20 + 4 stacks) in
   whichever profile wins — the off-heap addition is required either way;
   only the managed fraction is in question.
4. Take a **heap histogram** on a hot TM (btc-balances shows 6.2g used) to
   confirm the GC-slack theory before shrinking heaps fleet-wide; check the
   Scaffeine correction cache on erc20-v18 during a backfill.
5. Bake the winning profile into the shared chart as defaults; per-deploy
   overrides only for outliers. Consider piloting operator **memory
   autotuning** on one stacks job (autoscaler already on).
6. Tranche 2 (request downsizing: bch/doge/ltc-balances →~12000m,
   btc →~14000m, stacks →~10000m) only after the polygon pilot survives a
   catch-up/backfill event and 2–3 weeks of peak RSS support it.
7. Adjacent RocksDB levers likely worth more than cache size:
   `state.backend.rocksdb.use-bloom-filter` for point lookups, partitioned
   index/filters, NVMe-local working dir; also the dead `rocksDBConfig` path
   in etherbi-flink if job-side tuning is wanted.

## Session log

### 2026-07-20 — initial investigation (this doc's founding session)

Full survey + constraint discovery as summarized above. Live data gathered
via jobmanager REST (`/taskmanagers`, `/taskmanagers/<id>/metrics`) and
`kubectl top` on hprod; per-TM metrics needed 2–3 retried queries (Flink's
metric fetcher populates lazily). Two values.helm.yaml edits made (polygon,
erc20-v18), none deployed.

### 2026-07-20 — source verification of the heap-lean / state-heavy hypothesis

Direct read of the stacks, ERC20/BEP20 balances, and UTXO pipelines in
etherbi-flink (see "Verification pass" above). Conclusion: the gut feeling
holds — every pipeline is a per-record read-modify-write against RocksDB
keyed state with random-key access patterns, while heap carries only
one-block transients plus GC slack. Exceptions worth acting on: the
Scaffeine correction cache (config-overload bug, no SQL LIMIT) on
erc20-address-balances-v18, the double sizeNonce RocksDB get in the stacks
handler, and the dead `rocksDBConfig` path.

Quantified the Scaffeine blow-up against prod ClickHouse:
`erc20_balances_cache` = 88.8M rows over 2532 populated 10k-block shards
(avg 35k rows/shard, worst 1.25M across 507 contracts). With
`maximumSize(10000)` > 2532 possible shards, eviction never fires — a
backfill over blocks 23.2M–24.7M would pin 36M rows ≈ 9–10 GB on a 3.4g-Xmx
TM → OOM crash loop.

### 2026-07-20 — Scaffeine cache fix applied (etherbi-flink, uncommitted)

Fixed in etherbi-flink working tree (master, not committed):
- `BalancesProxyServicePerContract.scala` — constructor now calls
  `resetCache()` (10 pages, its own default) instead of
  `resetCache(pageCacheSize)` (=`balancesCacheRowLimit`, 10000 pages);
  `balancesCacheRowLimit` keeps only its page-width-in-blocks role. Cache is
  now keyed by `(shard, contract)` so per-contract pages can't shadow each
  other.
- `EthClientProxyCache.scala` — `createErc20BalancesWithContractsStatement`
  gains the missing `AND contract = ?` predicate (it previously scanned all
  contracts in the block range despite its name).
New worst case: 10 pages × worst per-(shard,contract) page (500k rows,
prod-measured) ≈ ~1.3 GB, realistic ≪ 100 MB. ETH path unchanged
(`eth_balances_cache` has no contract dimension; pages still whole-range but
now capped at 10). `sbt compile` + all 105 unit tests pass; no existing
tests cover these classes.

Follow-up in the same session — cache shape made configurable, defaults
retuned to 1000-block pages × 100 pages (was 10000×10). Rationale: the
cache is TM-wide shared, and mass-correction events span hundreds of
contracts (507 in the hot shard), so 10 slots thrash; worst-case resident
rows are comparable (prod-measured top-10@10000w = 3.6M rows vs
top-100@1000w = 5.8M ≈ 1.0–1.6 GB), while typical residency drops ~10×.
New config keys `addressBalances.balancesCachePageBlocks` (default 1000)
and `addressBalances.balancesCachePages` (default 100);
`balancesCacheRowLimit` is now IGNORED with a deprecation warning at config
read (deliberately no fallback — old 10000 width × new 100-page cap would
compound). Constructor params renamed through
`AddressBalancesCalculationByContract`, `AddressBalancesCalculationFlatMap`,
`BalancesProxyServicePerContract`; `getBalances` param renamed
rowLimit→blockLimit. dev.conf + unit tests updated; all 105 tests pass.

Devops follow-ups (values files still setting the deprecated key —
harmless, warns in logs, but should migrate):
- `eth-address-balances-v15` sets `balancesCacheRowLimit=300` — the only
  meaningful override; migrate to `balancesCachePageBlocks=300` (ETH path
  has no contract filter, so narrow pages matter there;
  eth_balances_cache worst 1000-block page = 72k rows).
- avax/polygon/arb/opt/erc20-v18/bep20/erc20-exact + hstage copies set
  `=10000` (the old default) — just delete the line.

## Proposed generic memory profile (2026-07-21)

Goal restated: one set of chart-level fractions; the only per-job variable is
`taskManager.resource.memory` (= process.size = pod request). No per-job
`flinkConfigOverrides` memory blocks.

Fraction capability verified against the flink-core **2.1.0 jar** (bytecode of
`TaskManagerOptions`): native fractions exist for `managed` (default 0.4),
`network` (0.1, min 64m, **max unbounded** since the 1g cap was dropped) and
`jvm-overhead` (0.1, min 192m, max 1g). `task.off-heap.size` and
`framework.off-heap.size` are size-only → their "fraction" must be computed in
the helm template from `taskManager.resource.memory` (known at template time).
`task.heap` is left unset = remainder.

Chart defaults (state-heavy skew; managed stays on the Flink default per
2026-07-21 discussion — the state-heavy nature of the jobs means the
data-driven correction, if any, is expected upward, not down):

| key | value | note |
|---|---|---|
| `taskmanager.memory.managed.fraction` | 0.4 (Flink default, unset) | RocksDB cache; the ONE value still gated on block-cache hit-rate metrics. Expected adjustment direction is UP (0.45+) given random-point-lookup workload; 0.1 only if metrics prove page cache carries the misses |
| `taskmanager.memory.network.fraction` | 0.1 | min 256m, max 1g (set once in chart) |
| `taskmanager.memory.jvm-overhead.fraction` | 0.1 | min 512m, max 4g — scales with P, covers glibc/RocksDB overshoot |
| `taskmanager.memory.task.off-heap.size` | template: clamp(7.5% of P, 512m, 2g) | Kafka clients + checkpoint transfers; the direct-OOM fix |
| `taskmanager.memory.framework.off-heap.size` | 512m | fixed |
| metaspace / framework heap | 256m / 128m | Flink defaults |

Resulting split (MiB) across fleet sizes — heap is the derived remainder:

| P | Flink total | managed | network | task off-heap | task heap | direct ceiling |
|---|---|---|---|---|---|---|
| 4096 | 3328 | 1331 | 333 | 512 | 512 | 1357 |
| 6144 | 5274 | 2109 | 527 | 512 | 1485 | 1551 |
| 8192 | 7117 | 2847 | 712 | 614 | 2304 | 1838 |
| 12288 | 10803 | 4321 | 1024 | 922 | 3896 | 2458 |
| 16000 | 14144 | 5658 | 1024 | 1200 | 5622 | 2736 |

Checks against the fleet survey: direct ceiling ≥1.36g everywhere (at-risk
jobs today: 0.84–1.6g); bep20/stacks managed unchanged (already default 0.4);
the five 0.1-recipe jobs get their RocksDB cache back (1.4→5.7g on 15.6Gi
TMs) since direct safety now comes from task.off-heap instead. btc-balances
heap 8.8→5.6g vs 6.2g "used" — **below current "used"; safe only if the
GC-slack theory holds; heap histogram before rollout is a hard prerequisite**.
erc20-address-balances-v18 outlier: at 8192m heap=2.3g vs correction-cache
adversarial worst ~1.6g — either keep v18 at a larger P or set
`addressBalances.balancesCachePages≈50` there.

Caveats:
- Operator memory autotuning (`job.autoscaler.memory.tuning.enabled`) would
  fight a fixed profile — pick one; don't enable both on the same job.
- limit-factor 2 stays: the request→limit gap remains the page-cache/native
  burst carrier for block-cache misses and RocksDB overshoot.
- Small TMs (4g) land at ~512m heap — tight GC headroom; jobs at that size
  should move to P≥6g rather than get a per-job override.

### 2026-07-21 — generic profile session

Q&A session over etherbi-flink: confirmed no pre-loading in the balances
cache (lazy Caffeine loader only); removed the redundant contract component
from the inner HashMap key (uncommitted on balancesCacheBounds; ~50MB/page
saving on worst-case pages); rejected lossy key-hashing (silent collision =
wrong balance). Independent very-thorough heap sweep (67 sources) re-confirmed
heap-lean verdict: no unbounded heap accumulator outside the known Scaffeine
cache; heap pressure is transient window/block materializations (allocation
rate, not live set). All jobs hard-set rocksdb backend (package.scala:283);
StreamDeduplicator's HashMapStateBackend ref is commented-out dead code.
Profile above drafted and sanity-checked numerically. Managed fraction
initially proposed at 0.35, revised to the Flink default 0.4 (user call:
given the state-heavy workload, any data-driven adjustment is expected
upward, so pre-emptively shaving the cache made no sense).

### 2026-07-21 — live memory profile of btc-balances-v12 TMs (via hprod Prometheus)

hprod kubectl is not provisioned in the agent container, but
`http://prometheus-hetzner.production.san:30200` is reachable and scrapes the
flink-metrics-prometheus reporters (port 9249). Snapshot of both TMs
(taskmanager-2-1 up 7h34m; taskmanager-3-1 up 72m):

| metric | 2-1 (loaded) | 3-1 (idle) |
|---|---|---|
| heap used / max | 5.26 / 8.55 GiB | 0.20 / 8.55 GiB |
| **heap post-GC floor (min_over_time 6h)** | **0.13 GiB** | 0.04 GiB |
| managed used / total | 1.38 / 1.38 GiB | 0 / 1.38 GiB |
| direct used / capacity | 1.40 / 1.40 GiB | 1.39 / 1.39 GiB |
| network segments in use | 121 / 45260 (0.3%) | 0 / 45260 |
| metaspace | 0.12 GiB | 0.06 GiB |
| GC: young / old counts | 41 / **0** (493 ms total in 7.5h) | 11 / 0 |
| container working set | 9.97 GiB | 2.15 GiB |

Findings:
1. **GC-slack theory confirmed on live data.** Heap sawtooths 0.2→~5.5 GiB
   with a ~50 min period, then young GC reclaims everything; post-GC floor =
   **~130–190 MiB live set** on the loaded TM. Allocation rate ≈ 2 MB/s. Zero
   old-gen collections in 7.5h. The 8.55g heap is >40× the live set; the
   generic profile's ~5.6g remainder heap at P=16000m is still generous.
   The "heap histogram before rollout" prerequisite is substantially
   answered for btc-balances by the floor measurement (a jmap histo would
   only add composition detail); remaining check: same floor query on the
   other families (stacks, bep20, erc20-v18 — v18 during a backfill).
2. **Network pool is 99.7% idle** on the loaded TM (121 of 45260 32KiB
   segments in flight, pool 1.38 GiB fully reserved as direct memory).
   Supports the profile's 1g network cap; even min 256m would likely serve
   this family. The direct 1.4/1.4 "full" reading is just the eagerly
   reserved pool, not pressure.
3. **Idle standby TM anomaly:** all 12 task series + busy-time sit on 2-1;
   3-1 has held 3 unused slots for 72m+ (jobmanager reports
   taskSlotsAvailable=3). Native k8s mode should release idle TMs after
   ~resourcemanager.taskmanager-timeout (30s default) — a full 15.6Gi
   request is parked doing nothing. Worth checking why (redundant-tm
   config? release failure after the attempt-2→3 churn) — that's ~8% of the
   fleet's total request wasted on one job. Idle-TM footprint baseline:
   2.15 GiB working set (JVM skeleton + reserved direct pool, heap pages
   untouched).

### 2026-07-21 — fleet-wide heap-floor sweep (45 TMs, hprod Prometheus)

`min_over_time(Heap_Used[6h])` per TM (= upper bound on live set: with G1 old
collections at zero, the floor includes uncollected old-gen garbage too):

- **Heap floor ≤ 0.31 GiB on 43 of 45 TMs** — across balances, stacks,
  exact, address-balances, all chains, operator and statefulset deployments
  alike. Median ~0.10 GiB.
- Outliers: `xrp-balances-v5-taskmanager-0` floor 1.29 GiB (its peer TM-1:
  0.30; the job runs 80 task series/TM, heap max 5.7g — could be genuine
  live set or old-gen accumulation; zero old GCs so the floor is an upper
  bound, not a concern yet), `opt-erc20-stacks-tm-3` 0.72 GiB.
- **G1 old-generation collections: 0 on every TM in the fleet.**
- **Network in-flight is tiny everywhere at snapshot time**: <1% of segments
  on operator jobs; the busiest are xrp-balances (6.2k of 28k ≈ 193 MiB) and
  eth-address-balances (2k of 10.7k ≈ 64 MiB). NB: the Netty OOM signature
  occurs during backlog bursts, so idle snapshots don't argue for shrinking
  the pool below the profile's 1g cap — they argue the cap is sufficient.
- Stacks family managed used = 1.92/5.75g (33%) a day after their restart —
  RocksDB hasn't filled its budget; consistent with modest cache pressure,
  awaiting hit-rate metrics for the real verdict.
- erc20-address-balances-v18 floors 0.09/0.11g — correction cache currently
  empty (no backfill in flight); the backfill-time check remains open.
- Idle-TM scan: exactly one TM fleet-wide with zero tasks —
  `btc-balances-v12-taskmanager-3-1` (the 15.6Gi standby found earlier).

Conclusion: the heap-histogram gate is retired fleet-wide — live sets are
~0.1–0.3 GiB against 0.6–8.6g heaps. The generic profile's derived heap
(0.5–5.6g across tiers) is safe for every observed job; heap is officially
the pool the profile can raid for managed/off-heap.

## FINAL: generic memory profile v1 (2026-07-21, data-backed)

All gating evidence is now in (fleet heap floors ≤0.31g, zero old GCs,
network in-flight ≤193 MiB, live btc sawtooth): the profile is safe to bake
into `global/flink-jobs-operator/flink-job-template`. Per-job variable:
`taskManager.resource.memory` only.

Chart `flinkConfiguration` additions (computed keys via template; managed
and task-heap deliberately UNSET — Flink default 0.4 and remainder):

```yaml
{{- $tmMemMi := int (trimSuffix "m" .Values.taskManager.resource.memory) }}
{{- $taskOffHeapMi := max 512 (min 2048 (div (mul $tmMemMi 75) 1000)) }}
    taskmanager.memory.task.off-heap.size: "{{ $taskOffHeapMi }}m"
    taskmanager.memory.framework.off-heap.size: "512m"
    taskmanager.memory.network.fraction: "0.1"
    taskmanager.memory.network.min: "256m"
    taskmanager.memory.network.max: "1g"
    taskmanager.memory.jvm-overhead.fraction: "0.1"
    taskmanager.memory.jvm-overhead.min: "512m"
    taskmanager.memory.jvm-overhead.max: "4g"
```

Render these from a dict merged under `.Values.flinkConfigOverrides`
(mergeOverwrite) so a per-job override of a profile key can't produce a
duplicate YAML key.

Rollout checklist:
1. Add profile to chart; DELETE per-job memory blocks from btc/bch/doge/
   ltc-balances + polygon (the 0.1 recipes + jvm-overhead pins). Their
   RocksDB cache grows 1.38→~5.7g; heap 8.55→~5.6g (floor-measured live set
   ~0.13g); direct ceiling 4.0→2.7g — still ≥2× any observed burst use.
2. bep20 + 4 stacks: no per-job edits needed; profile lifts direct ceiling
   1.6→2.7g (fixes the at-risk signature), managed unchanged at 0.4.
3. erc20-address-balances-v18: keep P=8192m, add app-config
   `addressBalances.balancesCachePages=50` (correction-cache worst ~0.8g
   against 2.3g heap) once the etherbi-flink cache PR deploys.
4. Jobs at P=4096m get ~512m heap — fine for erc20-exact (floor 0.09g);
   prefer bumping any busier job to ≥6g over reintroducing overrides.
5. Statefulset-style chains (arb/opt/eth/icp/icrc/cardano/xrp) use a
   different chart — same fractions portable later; xrp first candidate to
   re-measure (tm-0 floor 1.29g).
6. Request downsizing (tranche 2) stays sequenced AFTER profile has survived
   a catch-up event; profile change and request shrink must not ship
   together (isolate variables).
Still open: managed direction (block-cache hit-rate metrics, expected UP),
v18 backfill observation, btc-balances idle standby TM.

### 2026-07-21 — profile v1 shipped as PR

devops PR https://github.com/santiment/devops/pull/5821 (branch
`flinkTmMemoryProfile`, off master — independent of the older
`flinkMemRequest` branch): profile baked into flink-job-template with
per-key why-comments; per-key hasKey guards against flinkConfigOverrides
(no duplicate YAML keys, outlier overrides still possible); MiB-format
validation with explicit fail message. The five hprod managed-0.1 recipe
blocks (btc/bch/doge/ltc-balances, polygon) deleted. hstage values files
left untouched (their overrides shadow the profile per key — verified by
render). Render+parse tested: btc (profile), bep20 (no overrides), hstage
ltc (suppression). Note: hstage jobs inherit profile defaults for keys they
don't override, since the chart is shared — called out in PR.

Amendment (same day): framework.off-heap dropped from the profile — the JVM
enforces only the sum of off-heap components, so its 384m delta over the
128m default is folded into the computed task off-heap
(384m + clamp(7.5%, 512m, 2g)); ceilings/heaps bit-identical, one key
fewer. Memory parser widened to accept "g" (the strict "m" check broke
erc20-exact "4g" and hstage "10g"). All 24 operator jobs in hprod+hstage
render-verified. Second commit on PR #5821.

### 2026-07-21 — session wrap-up

Third commit on PR #5821: sharpened the network/jvm-overhead template
comments to state the demand-curve rationale (network demand tracks
job-graph width — constant across pod sizes, eagerly allocated, so capped;
jvm-overhead tracks state size — paper-only budget, so widened). Came out
of review discussion: both keys keep fraction 0.1 and only move bounds, in
opposite directions, which read as inconsistent without the why.

Related: the inner-HashMap key slimming in etherbi-flink (contract removed
from the corrected-balances page keys, ~50MB/page worst case) is committed
and pushed on `balancesCacheBounds` (565a6a4f).

**Current open items, in order:**
1. Review/merge devops PR #5821 (profile), then deploy and watch the five
   ex-recipe jobs through a catch-up event. Ship nothing else with it.
2. Review/merge etherbi-flink `balancesCacheBounds` PR; after deploy, do
   the devops values migration (balancesCacheRowLimit -> new keys, see
   2026-07-20 entry) and set `balancesCachePages=50` on erc20-v18.
3. Block-cache hit-rate metrics (advised step 1 — still not started).
   Decides managed.fraction's direction; expected up from 0.4.
4. Investigate btc-balances-v12-taskmanager-3-1: idle standby holding a
   15.6Gi request; native k8s mode should release it after ~30s idle.
5. Watch erc20-v18 heap floor during the next correction backfill
   (Prometheus min_over_time query; floors were 0.09/0.11g with cache empty).
6. Tranche 2 (request downsizing) after 1 succeeds + 2-3 weeks of RSS data.

Tooling notes for future sessions: hprod kubectl is not provisioned in the
agent container; hprod Prometheus at prometheus-hetzner.production.san:30200
covers TM metrics (labeled by `pod`, instant + range). helm not installed
(fetched 3.16.4 into scratchpad from get.helm.sh); gh token lacks read:org
so `gh pr edit` fails — use `gh api repos/.../pulls/N -X PATCH`.
