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
