# eth-stacks-v12: post-London throughput collapse — findings and improvement plan

## At a glance

- **Goal:** assess the first long ForSt/async-state production run of the ETH
  stacks job (eth-stacks-v12, branch `ethStacksAsyncState`, Hetzner prod) and
  identify throughput improvements for the historical backfill.
- **Status:** deployed as a fresh run 2026-08-20 ~14:15 UTC and **evaluated
  after 16 h (2026-08-21) — success on every checklist axis**; see the
  evaluation section at the bottom. Post-London rate ~180–240 blocks/s vs
  4.5 pre-fix (~40–50×). Fix 1 (append-only block buffer, `a1cbf092`) and
  Tier 2 (upstream pre-grouping, `7fc40c01`, plus `c1ab9f77` comment cleanup
  and `ade93a08` flush default 200 ms → 2 s) are all on `ethStacksAsyncState`.
  **Next session: output correctness spot-check — `eth_stacks` rows around
  the London boundary vs pre-optimization (window-variant-era) data; must be
  byte-identical.**
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
the production `eth_stacks` output — RESOLVED 2026-08-20: they stay (relevant
for downstream metrics), so the hot key must be handled, not filtered.

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
order-of-magnitude lever) — IMPLEMENTED, committed `7fc40c01` (2026-08-20):**
a chained non-keyed operator (`PreGroupAccountChanges`) buffers records on
heap per (account, block) and emits one composite (`AccountChangeBatch`,
possibly spanning several blocks) per account per flush. The hot key then
receives ≤ upstream-parallelism composites per flush instead of 184 records
per block. Implementation notes vs the original sketch:
- Watermarks turn out to advance per block (per-event `blockNumber − 1`
  strategy), so multi-block strides don't come for free: flush cadence is a
  new config `stackPreGroupFlushIntervalMs` (default 2 s of processing time,
  `ade93a08`; see the tuning rationale below). At the head, blocks arrive
  seconds apart → every block flushes immediately (latency unchanged);
  during catch-up many blocks coalesce per flush. Between flushes the
  operator *holds watermarks back* so a block's records always precede the
  watermark that covers it — the one invariant the downstream drain relies
  on. Held watermarks are subsumed by the newer one forwarded at the next
  flush (watermarks are a monotonic clock, not messages).
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

**Flush interval rationale (`stackPreGroupFlushIntervalMs` = 2 s, discussed
2026-08-20/21) — this knob should NOT be tuned on deploys.** Batching factor
k ≈ interval × block rate, so it self-adapts to the backlog: zero effect at
the head (12 s block time ≫ interval → every block flushes immediately),
larger composites the further behind we are — with a helpful feedback loop
(bigger k → faster → bigger k) exactly in the hot-key-bound slow regime.
Insensitive across ~0.1–2 s; the bounds that define the band:
- upper: head block time (12 s), checkpoint *interval* (1–2 min in prod —
  every barrier flushes via prepareSnapshotPreBarrier, so longer intervals
  buy nothing), and drain-burst size (an in-flight k-block drain chain must
  complete before the barrier passes that subtask → up to ~one interval of
  checkpoint start delay; self-limiting, since a chain slower than the
  interval would drop the rate and hence k).
- lower: a few block-times-at-catch-up, else no cross-block batching.
- costs at 2 s, all bounded by the interval itself: ≤ ~2 s output staleness
  during catch-up, ≤ ~2 s checkpoint start delay, burstier (but not
  blocking — async state interleaves other keys) drains, tens-of-MB
  transient drain memory on the hot subtask at high rates.

**Tier 3 — raise `sourceWatermarkAlignmentBlocks` — REVISED, mostly
obsolete:** the original premise (envelope caps the composite size after
Tier 2) does not hold for the implemented design — the flush interval is
the batching lever, and the envelope caps nothing the interval doesn't.
Raising alignment would only matter against laggard-split stalls; leave at
40 unless the evaluation shows sources blocked on alignment.

## Evaluation checklist for the deploy (next session)

The run now carries fix 1 + Tier 2 together, so the original `a1cbf092`-only
gate (~9–11 blocks/s) is superseded; both fixes' effects land at once. With
Tier 2 the hot key's per-block cost is ~1–8 composite puts + amortized scan,
leaving the ~5 sequential head round-trips per block (~3 ms) as the modeled
limiter → the hot key alone would cap out around a few hundred blocks/s, so
the observed rate should be set by the next limiter, not by burn.

Watch via Prometheus (`app="eth-stacks-v12"`, job must run as a FRESH run):

- **Block rate** = `currentInputWatermark` slope. Expect ≫ 11 blocks/s
  post-London. If it lands near or below ~9–11, Tier 2 bought little →
  the round-trip model is wrong; profile the async executor before more
  code (Tier 1 included).
- **Batching factor for free:** numRecordsOut / numRecordsIn of the new
  `Pre-group account changes ETH` operator (should be ≪ 1, shrinking as
  rate grows); numRecordsIn of Create Stack Changes drops accordingly.
- **Checkpoints:** duration back toward ~15 s; `checkpointStartDelay`
  may include up to ~2 s of drain-wait (expected, benign — see flush
  interval rationale).
- **`asyncStateProcessing_numBlockingKeys`** should drop;
  `busyTimeMsPerSecond` of Create_Stack_Changes_ETH should rise from
  ~230 ms/s.
- **Regression checks:** TM RSS flat (leak fix), ForSt file cache hit rate,
  and sane output — spot-check `eth_stacks` rows around the London boundary
  against the window-variant-era data (semantics must be byte-identical).
- **Find the next limiter** (sink, cold-key ensemble, ForSt write
  bandwidth): whatever caps the rate now decides whether Tier 1 (coalesce
  the per-block head cycles per drain — the natural next code change, the
  drain already collects k ready blocks in one place) is worth building.
  Catch-up arithmetic: ~10M blocks to head; 50 blocks/s ≈ 2.3 days.

## Evaluation results — 16 h in (2026-08-21, fresh run started 2026-08-20 ~14:15 UTC)

All checklist items pass. Prometheus (`app="eth-stacks-v12"`) + Loki (hprod);
pods 16 h old, 0 restarts.

- **Block rate:** watermark at **20,940,986** after 16 h. Post-London
  (crossed 12.965M ~19:5x on 08-20) the hourly slope holds **~180–240
  blocks/s** vs 4.5 pre-fix — **~40–50×**. Early pre-London hours ran
  455–1,700 blocks/s (sparse blocks).
- **Now record-throughput-bound, not hot-key-bound:** total pipeline input
  is **flat at ~180–210k records/s for the entire run** while blocks/s
  drifts down (245 → 178 overnight) — that's rising block density, not
  degradation. The burn key no longer sets the rate.
- **Batching factor:** pre-group out/in stable at **~0.30** (in ~185k rec/s
  → out ~53k composites/s).
- **busyTime Create_Stack_Changes:** ~614 ms/s avg (373–711 per subtask),
  up from ~230. No task saturated at 1000 ms/s. `numBlockingKeys`
  oscillates 0.3–5k with no throughput correlation.
- **Checkpoints:** 398 completed, typical duration **5–8 s** (was 13–30 s).
  1 failed = benign startup race at 14:18:47 ("Not all required tasks are
  currently running", trigger vs deployment). One 30.5 s duration spike
  ~03:53 UTC, one-off.
- **Regressions:** TM RSS flat ~13 Gi all run (leak fix holds); ForSt file
  cache 100% hit / 0 loadbacks, ~106 GB used.

**Next limiter = CPU across the ensemble.** TMs run ~6.5–7 cores against a
10-core limit with **20–30% of CFS periods throttled** (was zero pre-fix,
when the job idled behind the hot key). No single operator is the choke
point. Cheapest next lever: raise the TM CPU limit (devops values), not
Tier 1 — head-cycle coalescing has low value now that burn isn't dominant.

Watch items (benign so far): Kafka fetch noise — 373 `DisconnectException`
reconnects/17 h (~22/h) plus a 7-occurrence burst of "Unknown server error
while fetching offset" on `eth_transfers_v3` partitions ~06:43; all
auto-retried, no impact. Job pulls ~200k rec/s from those brokers.

**Remaining gate (next session): output correctness.** Spot-check
`eth_stacks` rows around the London boundary against pre-optimization
(window-variant-era) values — semantics must be byte-identical. Not yet
done.
