# eth-stacks-v12: post-London throughput collapse — findings and improvement plan

## At a glance

- **Goal:** assess the first long ForSt/async-state production run of the ETH
  stacks job (eth-stacks-v12, branch `ethStacksAsyncState`, Hetzner prod) and
  identify throughput improvements for the historical backfill.
- **Status:** investigation complete (2026-08-19). Root cause of the observed
  slowdown identified and confirmed. First fix (append-only block buffer)
  implemented and committed (`etherbi-flink` `a1cbf092`, not yet deployed).
- **Headline finding:** the job's throughput is capped by a **single hot key —
  the `burn` fee account** introduced by the London fork (EIP-1559, block
  12,965,000). When the backfill watermark crossed that block at 19:30 UTC
  2026-08-18, throughput dropped ~25 → ~4.5 blocks/s within 15 minutes and
  stayed there. The checkpoint-duration doubling (13 s → 30 s) that first
  drew attention is a *symptom* (barriers queueing behind the slowed flow),
  not a checkpointing problem.
- **Prior work:** [2026-07-stacks-working-set-state](../2026-07-stacks-working-set-state/stacks-working-set-state.md)
  (ForSt + async migration rationale and plan). This run also confirms the
  native-memory leak fix (`a6dbcf9d`) holds: TM RSS ~12 Gi after 22 h vs the
  earlier 2–4 Gi/h growth.

## What the run looks like (22 h in)

Healthy on every axis except throughput:

- 535 consecutive successful checkpoints, zero failures; the one savepoint
  committed in ~16 s — the disaggregated-state premise (fast savepoints,
  state already on S3) is validated.
- ForSt file cache: 100% hit rate, zero loadbacks; plateaued at ~96 GB total
  (~5.3 GB × 18 subtasks) with steady LRU eviction — evictions track
  compaction file churn, not read pressure.
- No resource pressure: CPU *under*-utilized after the slowdown (TMs at
  0.4–1.7 cores), zero CFS throttling, GC ~1 ms/s, heap ~4/10 Gi.
- Sibling ForSt jobs (btc/bch-stacks-v12) on the same S3/Kafka/nodes saw no
  disturbance at the transition — not shared infrastructure.

Investigation ran through Prometheus (`prometheus-hetzner.production.san:30200`
— Flink UI/REST is unreachable with read-only kube tokens; metrics carry
`app="eth-stacks-v12"`). `currentInputWatermark` = block number made the
fork-crossing correlation exact.

## Root cause

Two compounding mechanisms, both confirmed:

1. **Data shape (ClickHouse `eth_transfers`, blocks 12.955M–12.975M):**
   post-London every block adds ~184 `fee_burnt` records, all with
   `to = 'burn'` — literally one address (`uniqExact(to) = 1`). Records per
   block roughly double (~630 → ~1,180 after the from/to flatMap split).

2. **Code shape (`ComputeAccountStackChangesProcess`):** the async state V2
   framework serializes same-key records to preserve ordering, and
   `processElement` did an `asyncGet` + `asyncPut` of the *whole growing
   per-block record list* per record. For the burn key: ~368 sequential state
   round-trips per block plus O(n²) list re-serialization (~17k record
   serializations/block), all on one subtask. Watermark alignment
   (`sourceWatermarkAlignmentBlocks=40`, ~8 s of stream time) then makes every
   other split wait for it. At ~0.6 ms per effective round-trip this predicts
   ~4–6 blocks/s — matching the observed 4.5–5.4.

Notes for later: the `fee` records (~184/block) spread over ~48 miner
addresses were the pre-London analog and consistent with the ~25 blocks/s
seen before the fork; the hottest keys (burn > top mining pools > exchange
wallets) are the pipeline's rate limiters generally. `burn` rows ARE part of
the production `eth_stacks` output, so filtering the address out is a
product/semantics decision, not a pure optimization — parked.

## Improvement plan

**Implemented — append-only block buffer** (commit `a1cbf092`, "proposal 1"):
`blockBuffer` becomes `MapState[(timestamp, sequence), Record]`; buffering is
one blind `asyncPut` per record (no read-modify-write, no quadratic
re-serialization; `groupAndCompress` orders by primary key itself, so buffer
order is irrelevant). Timer firings drain *all* watermark-passed blocks in a
single buffer scan, oldest first, with a `drained-watermark` guard so the
burst's redundant firings exit after one read. Per-block head cycles, outputs
and stack semantics unchanged (window-variant equivalence tests, incl. a new
incremental-watermark test, all pass). State schema of `block-buffer` changed
intentionally under the same name → old checkpoints fail fast; deploy as a
fresh run per fleet policy.

**Tier 1 — coalesce head cycles across blocks (not yet built, small):** the
drain already processes k ready blocks per firing but still runs one
head-read/run/head-write cycle per block. Merging them into one cycle per
drain (per key) cuts the remaining ~5 round-trips/block to ~5 per watermark
stride. Pointless for the burn key until Tier 2 lands (record puts dominate);
becomes the binding constraint after it. At the chain head, strides are one
block — semantics and latency unchanged.

**Tier 2 — pre-group same-key records upstream of the keyBy (the
order-of-magnitude lever) — IMPLEMENTED 2026-08-20 (on `ethStacksAsyncState`,
pending review, not yet committed):** a chained non-keyed operator
(`PreGroupAccountChanges`) buffers records on heap per (account, block) and
emits one composite (`AccountChangeBatch`, possibly spanning several blocks)
per account per flush. The hot key then receives ≤ upstream-parallelism
composites per flush instead of 184 records per block. Implementation notes
vs the original sketch:
- Watermarks turn out to advance per block (per-event `blockNumber − 1`
  strategy), so multi-block strides don't come for free: flush cadence is a
  new config `stackPreGroupFlushIntervalMs` (default 200 ms of processing
  time). At the head, blocks arrive seconds apart → every block flushes
  immediately (latency unchanged); during catch-up many blocks coalesce per
  flush. Between flushes the operator *holds watermarks back* so a block's
  records always precede the watermark that covers it — the one invariant
  the downstream drain relies on.
- The operator is stateless across checkpoints: `prepareSnapshotPreBarrier`
  flushes everything ahead of the barrier.
- Correctness does not depend on flush timing: the keyed drain regroups
  scanned records by inner block timestamp across all buffered composites
  (a (key, block) split over several composites reunites there), processes
  only watermark-passed blocks, and trims/writes back composites that
  straddle the watermark. Each block still runs as exactly one
  byte-exact head cycle; per-inner-block timers keep drain timing identical
  to the per-record design.
- `blockBuffer` value type changed (record → composite) under the same
  state name → old checkpoints fail fast; fresh run per fleet policy, same
  as the `a1cbf092` deploy.
- Tests: window-variant equivalence suite extended (per-block composites,
  split composites + straddle/write-back via per-event watermarks, one
  giant composite per account via held flushes) + a new operator harness
  suite (grouping, hold-back, barrier flush, late singleton pass-through);
  all 132 unit tests and the job-graph build smoke tests green.

**Tier 3 — raise `sourceWatermarkAlignmentBlocks` (config-only, after
Tier 2):** today the envelope does NOT help (the hot key is slow in steady
state, not transiently skewed). After Tier 2 the envelope caps the batch
size, so raising 40 → a few hundred multiplies the batching factor directly.

## Expected results after re-deploying `a1cbf092`

Model: the burn key's sequential chain per block drops from ~370 round-trips
(one carrying O(n) bytes) to ~185 cheap ones.

- **Post-London block rate: ~4.5–5.4 → ~9–11 blocks/s** (records into
  Create Stack Changes ~5.3k → ~10–12k rec/s). This is the primary check that
  the round-trip-count model is right, and the gate for investing in Tier 2.
- **Checkpoint duration should fall back toward the ~15 s band** as
  `checkpointStartDelay` (currently 20–30 s max) shrinks with faster barrier
  flow. Incremental checkpoint size may also drop — the quadratic buffer
  rewrites were pure ForSt write churn.
- **Pre-London-range throughput** (if any re-run covers it) should also
  improve — miner-fee hot keys had the same buffering cost — but less
  dramatically (~25 blocks/s was already record-bound elsewhere).
- Watch via Prometheus (`app="eth-stacks-v12"`): block rate =
  `currentInputWatermark` slope; `busyTimeMsPerSecond` of
  Create_Stack_Changes_ETH should rise from ~230 ms/s;
  `asyncStateProcessing_numBlockingKeys` should drop.
- **Catch-up arithmetic:** ~10M blocks remain to head; ~9 blocks/s ≈ 13 days.
  Tier 2's projected 10×+ would bring it to ~1–2 days, bounded by the next
  limiter (sink, cold-key ensemble, ForSt write bandwidth) — measure, then
  decide Tier 1/Tier 3.

If the redeploy lands materially below ~9 blocks/s, the per-op latency model
is wrong (e.g. executor batching latency dominates differently) — profile the
async executor before building Tier 2.
