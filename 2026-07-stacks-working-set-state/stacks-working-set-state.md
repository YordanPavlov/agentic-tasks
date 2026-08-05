# Stacks job: working-set-bounded state (ForSt + per-address async restructure)

## At a glance

- **Goal:** make the account-model stacks job future-proof for high-volume,
  low-latency chains by breaking the property that Flink state grows with
  chain age. Target: state (and its operational costs — local disk, compaction
  I/O, checkpoint/recovery time) proportional to the **active working set** of
  addresses, with dormant history living on object storage.
- **Status:** investigation distilled (2026-07-14); implementation + evaluation
  not started.
- **Direction:** benchmark Flink 2 **ForSt state backend + async State V2
  API** on one contained job (XRP stacks), re-keyed by true
  `(contract, address)`. ClickHouse state-rebuild demotes to bootstrap /
  migration / disaster-recovery tooling, off the realtime path.
- **Prior work:** [2026-06-odt-bucketing-xrp](../2026-06-odt-bucketing-xrp/verifying-the-batched-odt-migration.md)
  — configurable odt bucketing, validated end-to-end on XRP. **Bucketing by
  itself produced limited gains** (5-min landing: live Flink segments −27%,
  stacks rows −31%, seam cells −35% vs unbatched): it is a constant-factor
  lever on the wrong axis, because live state is dominated by *dormant*
  addresses whose temporally-spread acquisitions never merge. This task picks
  up where that one stopped. (Bucketing remains valuable — see
  [synergy](#insight-5-losing-groupandcompress-is-fine-because-of-bucketing).)
- **Repo docs:** `etherbi-flink/docs/concepts/stacks.md`,
  `etherbi-flink/docs/decisions/configurable-odt-bucketing.md`.
- **Architecture C (added 2026-07-14, ON HOLD 2026-07-15):** compute stacks
  natively in ClickHouse (micro-batch SQL + UDF fold, ≤5-min latency budget)
  — spun out to
  [2026-07-stacks-in-clickhouse](../2026-07-stacks-in-clickhouse/stacks-in-clickhouse.md).
  Yordan put C on hold (conflicted about migrating away from Flink; this
  task — optimizing the Flink job — is the active direction). C's spike
  still delivered assets this task inherits: a golden-tested executable
  replica of the fold semantics, prod state-composition measurements
  (see "Measured inputs" below), and validated state-rebuild SQL. Insight 2
  below was hard-validated there.

## Problem statement

From production experience the recurring bottleneck of the stacks jobs is
**huge keyed state spilling into RocksDB**. Today that state is
O(every address that ever existed × its live cohort count): it grows
monotonically with chain age, and a high-volume chain just reaches the wall
faster. Concrete pains: local-disk bound, compaction I/O that rewrites
dormant state forever, checkpoint size/duration, and full-state download on
recovery/rescale.

## Insights from the 2026-07-14 investigation

### Insight 1 — the growth is on the dormant axis; constant factors don't fix it

The XRP measurement (bucketing task) showed live state is dominated by
dormant addresses. Any lever that shrinks bytes-per-segment or
segments-per-active-address (bucketing, encoding tricks) leaves the
O(chain history) growth intact. Future-proofing = bounding state by the
*working set*, i.e. adding a cold tier.

### Insight 2 — the stack is sorted by `ots`; state is reconstructible from the job's own output

From code reading of `HandlerOneAccountChange` (high confidence; wants a
property test): the per-address stack is **always sorted by `ots` ascending,
bottom → top**. Pushes arrive in block order at receipt time; a partial-pop
remainder re-installs at the *deepest popped* segment's `ots`, which is ≥
everything still below it; liabilities sit at `ots = 0`.

Consequence: the downstream-visible stack content equals the **net cohort
composition** — `sum(sign · amount) GROUP BY odt` over the job's own emitted
rows, ordered by `odt`. So **Flink state is a materialized view of the job's
own sink** and can be rebuilt from ClickHouse at any time. *(Corrected
2026-07-23: an earlier version claimed the per-segment `nonce` is
non-reconstructible. Under current coin-ledger semantics it IS recoverable —
every coin has one birth row and ≤1 death row, so an anti-join on nonce
recovers surviving coins exactly, incl. stack order via `(ots, nonce)`. The
nonce only becomes non-reconstructible under bucketing/cohort merging, which
breaks the birth/death pairing.)* Even independent of the main
direction, this buys: state bootstrap for new layouts, migrations without
replaying chain history, disaster recovery.

Two adjacent segments can share an `ots` (e.g. a pop between two same-bucket
inflows); group-by-odt merges them, which is downstream-equivalent (LIFO over
equal-`ots` adjacent segments behaves identically except for nonce/row
granularity). Rebuild must be validated against this.

### Insight 3 — RocksDB in Flink has NO cold tier; ForSt is that tier

Misconception to retire: the RocksDB backend does not propagate cold keys to
S3. Primary state lives **entirely on TaskManager local disk** (memtables +
block cache in memory, SSTs local); S3 is only a checkpoint *backup*, never
read during processing. Hence: state hard-bounded by local disk, compaction
scales with total (not working-set) state, recovery downloads everything
before the first record.

**ForSt** (Flink 2.x, designed around the async State V2 API) inverts
ownership: S3 is the primary store, local disk/memory are caches. Local disk
needs only the working set; checkpoints are near-trivial; recovery/rescale
starts immediately with a cold cache. `etherbi-flink` is already on
**Flink 2.1.0**, so this is available, not aspirational. Maturity is the open
question (medium confidence) — hence benchmark-first.

### Insight 4 — "flip computation around addresses" ≡ Flink 2 async state execution

The hydration/blocking objection to ClickHouse-rebuild-at-runtime: the job is
block-synchronous, so one dormant address's cold read stalls the whole
pipeline. The fix — process out of block order while preserving per-address
order — is **exactly the semantics of Flink 2's async state execution**:
records for different keys proceed out of order, same-key records are queued
in order by the framework, and in-flight operations drain at watermarks and
checkpoint barriers. Adopting ForSt + async API *is* the restructure; a
cold-address S3 read (~tens of ms) is hidden by other addresses' records.

Corollary: with cold state natively on S3, **ClickHouse hydration is not
needed on the realtime path** — it demotes to bootstrap/DR/migration (where
Insight 2 makes it clean). If the ForSt benchmark disappoints, a custom
hydration design in the same async shape (prefetch stage attaching cold
snapshots ahead of the keyed operator, eviction only below the sink's
committed high-watermark) remains the fallback — buildable but with sharp
edges (read-your-writes between eviction and re-touch; races when two records
for a cold address are in flight).

Semantic safety of per-address reordering is high-confidence: addresses share
no state, the two legs of a transfer are independent keys, and every seam
consumer groups by `(dt, odt)` — arrival order is invisible.

### Insight 5 — losing `groupAndCompress` is fine *because of* bucketing

The per-record shape does not run the window variant's block-level same-sign
compaction (it processes one change at a time, so there is no per-block batch to
compress before touching state). But 5-min odt bucketing merges same-bucket
inflows into the open top segment anyway, subsuming most of what block-level
compaction bought. **The bucketing branch is what makes the per-record shape
state-competitive** — the two tasks compose. Residual cost: one state commit per
record instead of per address per block window — an I/O question the async
batching should largely absorb; measure it.

**Correction (2026-07-16): dropping the window is a *choice*, not a framework
limit.** An earlier framing here implied the async model *cannot* use windows.
That is wrong for Flink 2.3: DataStream keyed window operators **do** support
async State V2 on ForSt (FLIP-488; `enableAsyncState()` on `WindowedStream`;
async window operator in 2.0.0 / FLINK-37028, `trigger` async in 2.2.0 /
FLINK-38363). So eliminating the V1 keyed-state API does **not** require removing
windowing across the codebase. We still converge the *stacks* window variant onto
the per-record flatmap twin — because bucketing subsumes `groupAndCompress`, the
twin already exists, and per-record is the shape that hides cold reads (Insight 4)
— but other windowed operators can stay windows and just gain `enableAsyncState()`
(pending a spike on whether a *stateful* user `WindowFunction.apply()` can chain
`StateFuture`s; `.apply()` is a synchronous batch callback). See
[forst-async-migration-plan.md](./forst-async-migration-plan.md).

### Insight 6 — current keying is wrong for async, in both job variants

- Window jobs (`ETHAccountChanges:66`, `XRPStacks:41`) key by
  `hash(contract, address) % stateParallelismLimit` — async per-key
  serialization would be per *bucket*: one cold read stalls 1/Nth of all
  addresses.
- The FlatMap variant (`ETHAccountChangesExact:72`) keys by **contract only**
  — even coarser.

Full benefit requires re-keying by true `(contract, address)`. The historical
reason for coarse keying ("bad Flink performance once keys × windows grows")
was a *windows* problem; a per-record FlatMap with no timers shouldn't hit it
— first thing to benchmark (medium confidence until measured).

### Insight 7 — per-address progress state, foldable into one KV

The FlatMap's late/duplicate dedup (`progressState`: block, txIndex, logIndex
monotonicity) is per-contract today; re-keyed it becomes one entry per
address — new per-address bytes. It folds naturally into the same KV as the
size/nonce pair (and the top segment batch), which also halves today's
two-KVs-per-address layout. Ship together.

### Insight 8 — downstream block-completeness gating must be audited

Out-of-order emission changes when "block N is fully in the topic" holds.
Checkpoint barriers still fence durability, but anything inferring
completeness from "saw a row with blockNumber > N" breaks. The second stage
in `ETHAccountChanges:75` (re-key by blockNumber into another window) looks
like exactly such a block-completion construct — trace what it feeds before
committing to the design. Same audit for the clickhouse-tables loader side.

## Measured inputs from the CH spike (2026-07-15, prod, read-only)

The [stacks-in-clickhouse](../2026-07-stacks-in-clickhouse/stacks-in-clickhouse.md)
sessions measured prod tables to size that architecture; the same numbers
test THIS task's premises. All measured on prod ETH 2026-07-15 (method and
queries in that task's session log).

- **Working set is tiny — the core premise holds on ETH.** Touched state
  keys per 5-min window: ETH-native ≈ 11.3k addresses, ETH ERC-20 ≈ 11.5k
  pairs, ~76k across ALL chains with transfers in CH. Against 360M live
  ETH-native segments total, one window touches ~0.01% of state. A lazy
  top-64 read policy needs only ~20 segment rows per touched key (measured
  ~22 native / ~19 ERC-20). ForSt's local cache for steady-state ETH is
  megabytes, not disks.
- **Live-stack distribution of *recently-active* addresses** (1,500 sampled
  from a real 5-min window): median 6 segments, p90 347, p99 2.6k, max
  12.5M. ERC-20 pairs: median 4, p99 897, max 69k. So cold-tier reads on
  pop are typically tiny, with a thin heavy tail.
- **The whales are push-only or shallow-popping.** `burn` (12.5M live,
  push-only — EIP-1559 receipts every block), `0x…dead` (70k, push-only),
  beacon-deposit-class contracts likewise; the ERC-4337 EntryPoint is the
  pop-churner (2M lifetime, only 20k live). With bucketing OFF a push never
  reads the stack (only the nonce counter); with bucketing ON it reads only
  the top segment/batch — either way the mega-whales never cause deep state
  reads. Supports Insight 4's "cold reads are rare and hideable".
- **Lifetime-to-live ratio 15×** (10.56B `eth_stacks` rows ever, 5.46B
  pushes ever, 360M live). Relevant as the rebuild-scan size (Insight 2
  rebuild reads the output table) and as the tombstone/GC design point in
  architecture C — NOT as RocksDB size (RocksDB holds live only).
- **Insight 2 is now hard-validated, and an executable oracle exists.**
  `etherbi-flink` branch `clickhouseStacks`, `clickhouse-stacks/`:
  `stack_fold.py` is a byte-exact Python replica of
  `HandlerOneAccountChange` + `groupAndCompress` (Scala golden tests ported,
  14/14; validated 1338/1338 output rows vs prod baseline on a focus group;
  further cross-checked by two independent SQL implementations on 305
  fuzzed vectors incl. liability/remainder/zero-segment edges). Any
  restructure this task does (re-key, per-record shape, ForSt) can diff
  against it cheaply — no Flink run needed for semantic regressions.
- **State-rebuild SQL exists and its read-amplification trap is known.**
  The `argMax(…, ver)`/top-K assembly query is written out in
  `clickhouse-stacks/doc/executable-fold.md`; measured lesson: reading
  per-address state through the month-partitioned, `(assetRefId, address,
  sign, dt, nonce)`-keyed prod `eth_stacks` costs ~granule-per-(address ×
  month × sign) — 4.2B rows read for 1,500 addresses. A bootstrap/DR
  rebuild (plan step 4) should scan-and-regroup the output table once,
  or read from a purpose-keyed, unpartitioned copy — never point-read the
  prod layout.
- **Scope fact for rebuild tooling:** XRP and the UTXO chains have stacks
  but NO transfers/changes in CH — CH-side rebuild inputs exist today only
  for eth/erc20/polygon(+erc20)/arb/opt/avax_erc20/icp/icrc. (`bep20` has
  transfers but no stacks job output.)

Consequence for the plan: **step 1 is now partially satisfied from CH** —
the dormant-tail premise and working-set sizing have prod evidence without
a savepoint. What still requires the state-processor readout is the
byte-level pricing: key-vs-value bytes, sizeNonce-vs-batches split, Kryo
overhead — the inputs for constant-factor levers 1–3.

## Parked: constant-factor levers (evaluate later, compound with everything)

Plausibly 2–3× on live-state bytes combined; none change the asymptote.

1. ~~**Drop stored `nonce`; mint fresh nonce on pop**~~ — **CLOSED 2026-07-23**:
   implemented, reviewed, reverted. Yordan keeps coin-ledger semantics (pop
   echoes the spent coin's construction nonce); only worth reopening if the
   bucketing/cohort direction is revived. See the 2026-07-23 session log.
2. **Fold `sizeNonceState` into the top batch's KV** — halves KV count and
   key bytes for the dominant tail of 1–2-segment dormant addresses (the
   ASCII `(contract, address)` key is stored twice today). Combines with
   Insight 7. **IMPLEMENTED 2026-07-23** (head-entry layout, uncommitted on
   `stacksOptimizations` — see session log).
3. **Binary-encode keys** — **PREMISE CORRECTED 2026-07-24**: ETH map keys
   are already binary (`KeyGenerator.stringAsKey` hex-decodes `0x` strings —
   always has). What remains: (a) XRP keys are still base58/ASCII — modest,
   ship with the lever-2 relayout; (b) binary keyBy key is a *design
   requirement* for the ForSt re-key, else it regresses ~40 B/KV on ETH.
   See the 2026-07-24 session log.
4. ~~**Cheapen `ots`**~~ — **CLOSED 2026-07-24**: implemented
   (`SegmentStorageCodec`, seconds + delta encoding), then reverted by
   decision. Composition math: the dominant 1-segment dormant tail gains
   only ~1 byte/KV (~1.5%), weighted total ~5–6% of ETH state — not worth a
   permanent whole-second-timestamp format invariant (ICP is ns-native
   upstream) plus carry-cost through the ForSt Phase-B restructure. See the
   session log; trivially re-appliable if the step-1 readout contradicts the
   estimate.
5. **RocksDB tuning pass** — ZSTD bottom levels, partitioned index/filters,
   verify incremental checkpoints. One hour of config review, not more.

## Closed doors (do not reopen)

- **Age-tiered coarsening of old cohorts** (merge dormant segments into daily
  buckets as they age): silently = the odt-relabel failure mode rejected in
  the bucketing ADR; via visible compensating ±rows = fabricated consumption
  events (phantom coin-age-consumed / dormant-circulation spikes). Dead both
  ways.
- **Netting across sign changes / dt bucketing** — rejected in the bucketing
  ADR with measurements; unchanged here.

## Plan

1. **Measure state composition** (still open from the bucketing task:
   "RocksDB gauge readout"). Use the state-processor API (already in
   `build.sbt`) against a savepoint: KV count and key/value bytes split by
   (sizeNonce vs batches) and by address-activity cohort. Validates the
   dormant-tail premise on ETH (measured only on XRP so far) and prices the
   constant-factor levers.
2. **Spike: ForSt + async State V2 on XRP stacks** (validated baseline +
   comparison harness exist from the bucketing campaign):
   - per-record FlatMap shape, keyed by true `(contract, address)`, no
     windows;
   - 5-min odt bucketing ON (Insight 5);
   - per-address progress state folded into the address KV (Insight 7);
   - measure: throughput vs current, cold-read stall behavior, checkpoint
     duration, recovery time, local-disk footprint.
3. **Audit block-completeness consumers** (Insight 8) in parallel with the
   spike.
4. **Decide:** ForSt as the cold tier vs fallback (custom CH-hydration in the
   same async shape). Either way, build the **state-rebuild-from-ClickHouse
   bootstrap** (Insight 2) — it de-risks every migration including this one.
5. Output equivalence validation reuses the XRP methodology
   (`compare_xrp_experimental.py`, L1 + intraday + daily L2).

## Step 1 bootstrap — state-composition measurement

Everything a fresh session needs to start the measurement without re-deriving
it from the code.

**Target:** ETH first — the dormant-tail premise is only measured on XRP, and
ETH is the big prod state. XRP second as a cross-check (validated baseline +
`compare_xrp_experimental.py` harness exist in the
[bucketing task](../2026-06-odt-bucketing-xrp/)).

**Tooling:** Flink state-processor API (`flink-state-processor-api` is
already in `build.sbt`). It reads **canonical savepoints**, not incremental
RocksDB checkpoints — if the running jobs only have checkpoints, an operator
must trigger a savepoint first.

**Where the state lives (verified against code, 2026-07-14):**

| job | stacks operator uid | keyBy type |
|---|---|---|
| `ETHAccountChanges` (window) | `create-stack-changes-<executionName>` (`ETHAccountChanges.scala:70`) | `Int` — `(contract.hashCode + address.hashCode) % stateParallelismLimit` (`:66`) |
| `ETHAccountChangesExact` (flatmap) | `create-stack-changes-<executionName>` (`ETHAccountChangesExact.scala:75`) | contract (`String`) (`:72`) |
| `XRPStacks` (window) | `calculate-transaction-stack-changes` (`XRPStacks.scala:46`) | `Int` (`:41`) |

State descriptors to declare in the reader — reuse the existing ones from
`ComputeAccountStackChangesTimeWindow`:

- `storeDescriptor` — MapState `account-change-store`:
  `Array[Byte]` (KeyGenerator ASCII key + batch index) → `StorageSegments`
  (Avro, `segment.avsc`: per segment `nonce` long, `ots` long, `value` bytes).
- `sizeNonceDescriptor` — MapState `nonce`:
  `Array[Byte]` → `(java.lang.Long, java.lang.Long)` (Scala tuple → Kryo).
- Exact variant additionally: `progress-state`
  (ValueState[`CurrentBlockInfoStorage`]).

**What to emit per address** (then aggregate):

- KV counts and serialized bytes, split **sizeNonce vs segment batches**, and
  split **key bytes vs value bytes** (re-serialize values with the Avro
  writer to measure; keys are the map keys as stored).
- Segment count per address (from the size in the sizeNonce pair).
- **Dormancy cohort:** state holds no last-touch timestamp; use the **top
  segment's `ots`** (= last inflow) as the activity proxy, optionally
  cross-checked by joining sampled addresses against last-transfer times in
  ClickHouse.

**Outputs wanted:** totals + histograms per cohort (TSV in this directory,
summary table appended to this doc). This prices constant-factor levers 1–3
(share of bytes in duplicated keys / sizeNonce KVs / stored nonces) and
tests the dormant-tail premise on ETH.

**Operator input needed at session start:**

- Savepoint availability: S3 path + whether the container can reach it
  (`ALLOW_PROD_*` opt-in; S3 credentials). Alternative if the savepoint is
  not reachable locally: package the reader as a Flink batch job and run it
  on the cluster, shipping back only the aggregates.
- Which `executionName`s / contract scope to measure for ETH (per-token jobs
  vs ETH itself).

## Session log

### 2026-08-05 (cont.) — 10g cache cap deployed fleet-wide (template default), post-redeploy review clean

Yordan's call: 10g goes into the fleet template (`flink-job-template`
guarded default, was 100g) rather than per-job values — defaults over custom.
Same commit syncs eth-stacks-v12 values with live state: image → `a6dbcf9d`,
`fs.s3a.connection.maximum: 512`/`threads.max: 64` committed (were live-only
via reconfigure, never in git); DROPPED `forst.log.*` (half-working
diagnostic, async instances ignored it anyway) and `MALLOC_CONF` (jeprof
profiling off — leak validation evidence already collected).

Redeployed ~14:15 UTC (new pods eth-stacks-v12-taskmanager-1-{1,2,3}, still
all on vidar). Review at +25 min: config verified in pod spec/ConfigMap
(10g + s3a present, MALLOC_CONF gone); recovery clean, no S3A pool timeouts
(512 pool held); checkpoints 953–962 completing steadily (~3.4G, 9–20 s,
2.5 min cadence); caches rebuilding from empty (1.0–1.6 G/subtask), 0
evictions yet, hit ratio 1.0; RSS flat 9.2–10 G; throughput unchanged
(~10–11k rec/s both sides of the redeploy — the 07-31 24.9k baseline was a
sparse chain region, not comparable). Slightly lower CPU post-redeploy =
expected transient (compaction backlog reset on restart + prof sampling
gone). **Eviction first-fire ETA ~10 h** at ~0.9 G/h per cache — watch
lru_evict > 0, hit ratio, records/s, checkpoint duration when caches reach
10 g.

### 2026-08-05 — leak fix VALIDATED after 19h (RSS flat); Grafana "sawtooth" diagnosed as page-cache accounting, not a leak

Patched image (`a6dbcf9d`, the ReadOptions try-with-resources fix) resumed
~2026-08-04 18:00 UTC; TM pods `eth-stacks-v12-taskmanager-1-{4,5,6}` (all on
node vidar), 0 container restarts, 1 Flink job restart in 24h (= the resume
itself).

**Leak fix validated.** `container_memory_rss` per TM: 10.7 → 11.1 GiB over
19 h ≈ **~20 MiB/h**, vs 2.2–3.9 GiB/h before the patch (100–200× better).
This also answers item 5a as far as RSS can: total native is flat, so the
block-cache component has plateaued within the managed budget. The jeprof
re-diff (ReadOptions symbol must be flat) is still worth one confirmation
pass but is no longer load-bearing.

**The 10G→21G→10G sawtooth (3h flat, 2–3h rise, cliff, ~5–6h period) is a
metric artifact, not process memory.** Decomposition per TM over 24h
(cadvisor, 10-min step):

- `container_memory_rss`: **flat ~10.7–11.1 G** the whole time.
- `container_memory_cache` (page cache): ~15.5–17.5 G, roughly flat.
- `container_memory_usage_bytes` (rss+cache): ~27–29 G, roughly flat —
  pinned just under the 32 G limit; kernel memcg reclaim continuously evicts
  file pages to make room (normal for heavy file I/O in a limited cgroup).
- `container_memory_working_set_bytes` (= usage − inactive_file): **this is
  the sawtooth** — 11.4 → ~24 G ramps as file pages get re-referenced
  (ForSt local-cache SST reads/compaction re-reads promote pages
  inactive→active), then a cliff back to 11.4 when reclaim mass-deactivates
  the active file list. Roughly synchronized across the 3 TMs (same work,
  same node). Drops observed ~23:08, ~04:10–04:30, ~09:10–09:30 UTC.

The Grafana panel plots working_set (the standard k8s "memory usage"
metric). No OOM risk from this pattern: the anon component is flat and the
file pages are reclaimable — usage already plateaus below the limit. If the
panel noise bothers, plot `container_memory_rss` for leak-watching.

**Real item to watch instead — ForSt local file cache growth.** Each async
operator subtask has its OWN local cache (3 per TM): 30.5/31.1/40.7 G on
TM-1-4 at 19h, growing ~0.9 G/h per instance, monotonic, **0 LRU evictions
yet**, hit ratio still 1.0000. `remainingBytes` ≈ 5.46 T (the disk-free
bound sees vidar's disk). Expected during from-genesis backfill (working set
= entire history), but: the `size-based-limit: 100g` is evidently
**per instance**, so worst case 9 × 100 G ≈ 900 G on vidar alone (all three
TMs schedule there). At current slope the biggest instance hits the 100 g
cap in ~2–3 days → that will be the FIRST live test of size-based eviction;
verify evict actually fires (lru_evict > 0), disk stops growing, and hit
rate stays sane. Also note the cliff in working_set does NOT correlate with
checkpoints/restarts — purely kernel LRU timing.

**Still owed:** upstream JIRA for the ReadOptions leak with the profile
evidence (item 5c) + FLINK-XXXXX id into flink-patches/README.md; optional
jeprof confirmation diff; output-equivalence pass vs eth-stacks-v11 once
backfill lands.

### 2026-08-04 (cont.) — ReadOptions leak patch IMPLEMENTED on `ethStacksAsyncState` (uncommitted, awaiting review)

Executed the patch plan from the morning session. All four prep steps done;
deploy pending review.

- **Step 1 (locate the class) resolved without a pod** — eth-stacks-v12 pods
  are GONE: the FlinkDeployment was deliberately suspended (operator event
  `job.state: running -> suspended`, ~10:05 UTC), presumably by Yordan to
  await the patched image. Verified instead via the flink-dist pom at
  `release-2.3.0`: `flink-dist` declares `flink-statebackend-forst` as a
  bundled dependency → the class IS in `lib/flink-dist-2.3.0.jar` (matches
  the runtime evidence: ForSt runs with no forst jar in lib/ or opt/).
  `flink-dist` is NOT on Maven Central — pom + runtime evidence is the
  verification; the Dockerfile guard makes the build fail loudly if wrong.
- **The patch** (`flink-patches/ForStGeneralMultiGetOperation.java`): pristine
  `release-2.3.0` copy + one-line change — `try (ReadOptions readOptions =
  new ReadOptions())` in `process()`'s executor lambda (covers both early
  `return`s, exceptions, and normal exit; resource closes before the existing
  `finally`). Header comment marks it as a patched copy; README.md documents
  evidence, safety argument, and the version-bump procedure.
- **Dockerfile step** (blockprocessor stage, after apt, before `USER flink`):
  guard `jar tf | grep -q` that the class exists in flink-dist (a future
  bump that relocates it fails the build), `javac --release 21 -cp
  flink-dist` (jdk-headless already installed there), `jar uf` the compiled
  class(es) back, `chown flink:flink` the jar. Comment-inside-RUN pattern
  already precedented in this Dockerfile (Docker strips `#` lines before
  joining continuations).
- **Validated locally:** test-compile against
  `flink-statebackend-forst-2.3.0.jar` + `forstjni-0.1.8.jar` from Maven →
  clean, exactly ONE class file (lambdas are invokedynamic, no inner
  classes); the Dockerfile glob `ForStGeneralMultiGetOperation*.class`
  covers it either way.
- **Item 5b (sweep) DONE:** grepped the full `flink-statebackend-forst`
  sources jar for `new ReadOptions|new WriteOptions|new Options(` — all
  other sites are lifecycle-managed (`ForStDBWriteBatchWrapper` →
  `toClose`; `ForStResourceContainer.get{Read,Write}Options` →
  `handlesToClose`; `ForStIncrementalRestoreOperation` → `IOUtils.closeQuietly`
  in `close()`). The multiGet site is the ONLY leak of this class.
- **Deliberately NOT included:** the stats-dump
  `ConfigurableForStOptionsFactory` rider — image change kept minimal so the
  post-deploy slope change is attributable to the fix alone (per item 5d).

**Next:** Yordan reviews → commit on `ethStacksAsyncState` → CI image build →
resume/reconfigure eth-stacks-v12 on the new image → verify: TM RSS slope
(was 2.2–3.9 Gi/h) and re-run `symdiff.py` jeprof diff —
`Java_org_forstdb_ReadOptions_newReadOptions` must go flat; also re-check
whether the block-cache component plateaus at the 5.5 Gi budget (item 5a).
Then file the upstream JIRA with the profile evidence (item 5c) and update
flink-patches/README.md with the FLINK-XXXXX id.

### 2026-08-04 — leak-hunt round 2 settings LIVE (jemalloc prof + local ForSt LOGs); shared-cache hypothesis REFUTED at runtime; S3A pool fix

Shipped via `make reconfigure` on devops `ethStacksForst` (values of eth-stacks-v12):

- `state.backend.forst.log.dir: /var/log/flink` (+ max-file-size 25mb, file-num
  10) — relocate every instance's ForSt info LOG (incl. the async one that
  otherwise goes to S3 under UUID names) to the tmp-logs emptyDir. VERIFY on a
  TM: `ls /var/log/flink` (agent token can't exec).
- `containerized.taskmanager.env.MALLOC_CONF: prof:true,prof_prefix:/tmp/jeprof,
  lg_prof_sample:21,lg_prof_interval:32` — jemalloc allocation profiling, one
  .heap dump per ~4GiB cumulative allocation. The `containerized.taskmanager.
  env.*` prefix DOES reach TM pod specs under the operator (verified in pod
  env). No "Invalid conf pair" in any container log → prof is compiled into the
  image's jemalloc (also verified option strings in Debian's libjemalloc2).
  VERIFY dumps appear: `ls /tmp/jeprof*` on a TM (~1–2h), then
  `jeprof --text <java> /tmp/jeprof.*.heap` attributes the growth.
- **Suspect #1 (async instances on standalone 8MB default) REFUTED twice:**
  (a) bytecode: `createAsyncKeyedStateBackend` calls
  `allocateSharedCachesIfConfigured` same as sync path (decompiled
  flink-statebackend-forst-2.3.0); (b) runtime: every instance on every TM logs
  "Obtained shared ForSt cache of size 1977474552 bytes" = 5.5Gi managed / 3
  slots. Remaining suspects: pinned/"zombie" block-cache overrun, table-reader
  memory ∝ SST count, JNI write path outside accounting — the jemalloc profiles
  discriminate these.
- **New failure mode found+fixed on the reconfigure recovery:** ForSt cold-cache
  recovery is an S3 read storm; the shared S3A HTTP pool (default
  `fs.s3a.connection.maximum: 96`) exhausted → async multiGet failed
  ("Timeout waiting for connection from pool") → 10-min restart backoff.
  Fixed: `fs.s3a.connection.maximum: 512`, `fs.s3a.threads.max: 64` in the
  job values; **fleet-template candidate for all ForSt jobs** (S3 is primary
  store now, pool was sized for checkpoint-backup traffic). Second reconfigure
  deployed clean, checkpoints running.
- Also still pending from 08-03: the periodic native stats dump needs a custom
  `ConfigurableForStOptionsFactory` in etherbi-flink
  (`setStatsDumpPeriodSec(300).setDumpMallocStats(true)`) — Flink hard-codes
  `setStatsDumpPeriodSec(0)`; `state.backend.forst.options-factory` is the only
  seam. Rides the next image build.

**LEAK FOUND (same day, via the jemalloc profiles):** exec works for the agent
now; jeprof dumps analyzed offline (image lacks binutils — heap files + java,
libjvm, libjemalloc, libforstjni pulled via `kubectl cp`, symbolized with a
custom script `scratchpad/jeprof/symdiff.py`, jeprof-style heap_v2 sample
scaling at 2MiB rate; async ForSt LOG did NOT relocate to /var/log/flink —
only sync instances did, file-mapping layer wins).

Steady-state window on TM-1 (07:58→08:10, 12 min, post-warmup): +1180 MiB
live native, two components:

1. **CONFIRMED LEAK — `ReadOptions` never closed in the async multiGet path.**
   +530 MiB/12 min live in `Java_org_forstdb_ReadOptions_newReadOptions`.
   `ForStGeneralMultiGetOperation.process` creates `new ReadOptions()` per
   executed batch and no path closes it — verified in the 2.3.0 bytecode AND
   in apache/flink **master** source (still unfixed upstream as of
   2026-08-04). Leak rate ∝ read batches ⇒ matches "growth ∝ per-subtask
   volume" skew from the control runs. **Upstream JIRA candidate + local
   image patch candidate** (try-with-resources; note the sync backend shares
   one long-lived ReadOptions — precedent that closing/sharing is safe;
   multiGetAsList returns byte[] copies, nothing stays pinned).
2. **Block-data/cache stacks** (+723 MiB/12 min: UncompressBlockData →
   MaybeReadBlockAndLoadToCache → LRU insert under BlockBasedTable::Get):
   cumulative ~4–4.5 GiB since restart — still consistent with legitimate
   fill toward the 5.5 GiB/TM shared cache budget (shared cache IS wired:
   "Obtained shared ForSt cache of size 1977474552" per slot). VERIFY it
   plateaus: re-diff dumps a few hours in; if it grows past ~6 GiB, that's
   leak #2 ("zombie cache" — Confluent saw exactly this pattern with RocksDB).

Earlier window (07:28→07:45) grew +2975 MiB — warmup-dominated, don't use it
for slope estimates. jeprof text mode mis-attributes everything to
`malloc_stats_print` (Debian libjemalloc has no .symtab; jeprof picks nearest
dynamic symbol and fails to prune allocator frames) — use symdiff.py.

**NEXT SESSION — patch plan for the ReadOptions leak (decided: fix locally
first, file upstream JIRA after the fix validates):**

1. **Locate the class in the image** (blocking first step). It is NOT a
   separate jar: `/opt/flink/lib` and `/opt/flink/opt` have no `*forst*` jar,
   and etherbi-flink's build.sbt has no forst dependency (only
   `flink-statebackend-rocksdb % provided`). Almost certainly bundled in
   `/opt/flink/lib/flink-dist-2.3.0.jar` — verify on a pod:
   `unzip -l /opt/flink/lib/flink-dist-2.3.0.jar | grep ForStGeneralMultiGet`.
   (If it unexpectedly turns out to load from usrlib, the patch is simpler:
   override via our assembly — parent-first only wins when the parent HAS the
   class.)
2. **The fix**: in `ForStGeneralMultiGetOperation.process`'s executor lambda
   (flink tag `release-2.3.0`,
   `flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStGeneralMultiGetOperation.java`),
   wrap `new ReadOptions()` in try-with-resources spanning the whole lambda
   body. Safe: `multiGetAsList` is synchronous within the lambda and returns
   `byte[]` copies (nothing stays pinned); all early `return`s are covered by
   try-with-resources; the sync backend shares one long-lived ReadOptions as
   precedent. Same fix confirmed still missing on apache/flink master
   (2026-08-04), so the patched file ports forward.
3. **Injection into the image** (Dockerfile of etherbi-flink, blockprocessor
   stage): keep the patched `.java` in-repo (e.g. `flink-patches/`); add a
   build step that compiles it against the dist jar
   (`javac -cp $FLINK_HOME/lib/flink-dist-2.3.0.jar --release 21`) and
   injects the resulting `.class` files (incl. the lambda's inner classes if
   any) back into the same jar (`jar uf`). Deterministic, survives base-image
   digests, affects every job on the 2.3 image (desired).
4. **Verify after deploy**: RSS slope on the eth-stacks-v12 TMs (was
   ~2.6 GiB/h from ReadOptions alone at current volume) + re-run the
   symdiff.py jeprof diff — `Java_org_forstdb_ReadOptions_newReadOptions`
   must go flat. Tooling saved as [symdiff.py](./symdiff.py) in this task dir
   (usage: `python3 symdiff.py <base.heap> <new.heap> <libroot>` where libroot
   mirrors the pod's lib paths — needs local binutils; the flink image has
   jeprof but no objdump).
5. **Also settle**: (a) does the block-data/cache component plateau at the
   5.5 GiB budget (diff two dumps hours apart; if it keeps growing → second
   bug, "zombie cache"); (b) sweep remaining forst classes for other
   per-call `ReadOptions`/`WriteOptions` constructions without close
   (targeted checks so far: ForStIterateOperation and ForStWriteBatchOperation
   construct none; full sweep interrupted); (c) upstream JIRA with the
   profile evidence once the fix validates; (d) optional rider on the same
   image build: the stats-dump `ConfigurableForStOptionsFactory` — but
   consider keeping the image change minimal so the slope change is
   attributable to the fix alone.

### 2026-08-03 — native memory leak hunt (timer fix REFUTED, pmap forensics done) + savepoint/ops chapter; CONTINUE: read async-instance ForSt LOG from S3

**Where we left off (do this first next session):** find the async ForSt instance's
info LOG among the UUID-mapped files in
`s3://flink-checkpoints-production-eth-stacks-v12/checkpoints/ha/8294e141d423608ff2fe355753ce593f/shared/op_AsyncKeyedProcessOperator_b5c69ee72b976d0370e8f6ad9e961726__1_9__attempt_0/db/`
(`mc cat` the small 31–37KiB UUIDs, e.g. `52d8a4a1…`, `2b2e2209…`, `a9ccfb8e…` — a LOG
opens with "RocksDB version"/`Options.` lines), then grep
`write_buffer_manager|block_cache|capacity`. Control group: the SYNC instances
(StreamMap/WindowOperator) DO write local LOGs at
`/tmp/tm_<pod>/tmp/<jobId>/op_*/db/*_db_LOG` (path-mangled names — plain `find -name
LOG` misses them); grep the same there. **Hypothesis to test:** the managed-memory
wiring (shared write-buffer-manager + ~5.9GB block cache) is not applied to the
async/disaggregated instances → they run the standalone 8MB-cache default
(`state.backend.forst.block.cache-size` default 8mb) with unbudgeted native
allocation ∝ write volume — which would explain everything below.

**Memory leak — established facts:**
- Two control runs (2026-07-31 + 2026-08-03 09:xx): TM RSS grows linearly
  2.2–2.75 Gi/h from ~6Gi, no plateau; per-TM skew stable (17/14/12-shaped —
  whale keys hash to fixed subtasks; growth ∝ per-subtask write volume). k8s
  limit 32g (limit-factor 2, devops PR #5821); OOMKill horizon ~10–14h. No
  slowdown before the cliff: anon memory, no swap, `memory.max` is a tripwire.
- Flink accounting is healthy throughout: heap 1–3.3Gi (capped), managed pinned
  at exactly 5.5Gi (=0.4 budget), network 1Gi. The growth is native, OUTSIDE
  all Flink budgets.
- **Timer hypothesis REFUTED:** `state.backend.forst.timer-service.factory:
  HEAP` (verified loaded, run of 11:26) did not change the slope (3.0–3.9 Gi/h;
  possibly higher only because backfill was in a denser chain region). Heap
  stayed 0.6–2.1Gi — suspicion the factory may not even apply to async-state
  operators. Override left in place (harmless, matches FLINK-17800-era fleet
  stance).
- **pmap/smaps forensics** (TM-1-4 @ 18.7GB, two snapshots 20min apart,
  +1.06Gi): 99.9% `Anonymous`, `Pss_File` 16MB (no page cache/mmap involvement;
  ForSt does NOT mmap local SSTs). JVM heap region flat (+10MB, 3.6Gi resident
  of 5.6 reserved). Entire delta in jemalloc large extents: one NEW extent
  +756MB + one filling +516MB while two old 2.3–2.5Gi extents SHRANK ~230MB →
  churn with net accumulation; freeing works but is outpaced ~3Gi/h. Not
  fragmentation-only, not JVM, not files: native malloc through jemalloc
  (image preloads it; TMs run via docker-entrypoint.sh).
- **ForSt native metrics DO NOT EXPORT in 2.3** — all `state.backend.forst.
  metrics.*` keys (incl. the sst trio enabled since day 1) are accepted but
  produce no prometheus series; only `forst_fileCache_{usedBytes,hit,miss,
  lru_evict,lru_loadback,remainingBytes}` exist. Observability gap for the
  whole migration; candidate upstream report. The 5 extra gauges
  (mem-tables/block-cache/pinned/capacity/table-readers) stay in values for
  future versions.
- Flink disables RocksDB-style periodic stats dumps (no DUMPING STATS in any
  LOG), so LOG startup options dump is the native-side evidence.
- ForSt fileCache: 25–83MiB used, 100% hit, 0 evictions (cache is NOT the
  leak); S3 shared-file reuse verified across 3 restarts (Jul-31 SSTs still
  referenced Aug-3 — claim/fast-duplicate model works).
- Suspect ranking: (1) managed-memory budget not wired into async instances
  (8MB default cache + unbudgeted memtables/readers), (2) pinned block-cache
  overrun ("zombie cache"), (3) table-reader memory ∝ SST count, (4) JNI write
  path outside ForSt accounting.

**Savepoint chapter (all fixed on devops `ethStacksForst`):**
- ForSt REJECTS CANONICAL savepoints (`checkForcedFullSnapshotSupport` →
  IllegalStateException → task failure → job restart). Checkpoints (native
  incremental) were never affected — 36+ fine before the first savepoint
  request. Fix: `kubernetes.operator.savepoint.format.type: NATIVE` (per-job
  values; fleet-template candidate for the ForSt era). Consequence for ADR:
  native savepoints are backend-locked → no canonical cross-backend export;
  replay-from-Kafka is the escape hatch. ForSt native savepoints are plausibly
  CHEAP (S3 server-side copy) — unverified, measure when one fires (~Aug 30).
- Root cause of the surprise trigger: the chart's periodic-savepoint cron was
  derived from `.Release.Time` ("deploy day − 1") → every `helm upgrade`
  re-rendered YESTERDAY into the schedule; operator cron semantics are a
  freshness contract with catch-up → immediate savepoint after every upgrade.
  Fixed: helper now emits plain `30d` — operator anchors the first interval to
  the CR's immutable creationTimestamp (verified in operator source,
  SnapshotTriggerTimestampStore: max(creationTimestamp, last trigger)) →
  "first savepoint ~1 month after first deploy" preserved, upgrade-immune.
  eth-stacks-v12 next periodic: ~Aug 30. Fleet jobs >30d old with no savepoint
  get ONE catch-up at their next upgrade.
- Helm 4 SSA field-ownership: `make suspend/resume` (kubectl patches) steal
  `.spec.job.state` from helm → later plain `install` fails with a
  field-manager conflict. New `make reconfigure` target (= install +
  `--force-conflicts`, Helm 4) is the go-to for rolling out changed settings;
  plain `install` deliberately stays strict. Also: `restore_from` on an
  EXISTING FlinkDeployment is decorative — the operator honors
  `initialSavepointPath` only on first deploy (or with a savepointRedeployNonce,
  not wired in our template); `last-state` restores the newest retained
  checkpoint anyway.

**Ops notes:** agent kubectl token cannot exec or read flinkdeployments CRs
(pods/logs/PVC/PV fine) — CR-level checks and exec forensics go through
Yordan. `helm` and `docker` missing in the agent container (helm fetched to
/tmp ad hoc). ForSt async instances route even their info LOG through the
file-mapping layer to S3 (UUID names); sync instances write it locally under
the db dir with a path-mangled filename.

### 2026-07-31 — eth-stacks-v12 ForSt test deploy LIVE on hprod; baseline readings for later comparison

First production deployment of the ForSt + async-V2 stack (plan Phase A+B3
combined, ETH job only). Deploy chain:

- **etherbi-flink `ethStacksAsyncState`** through `f51f640b` ("Set Forst state
  backend" — `package.scala` flips `state.backend` rocksdb→forst; set in code
  via `env.configure`, which outranks the chart's `state.backend.type:
  rocksdb` line, empirically proven by the old chart's `filesystem` setting
  never winning). Branch also gained a Dockerfile fix (below). Note: commits
  `3a7d675c`..`c2c7ec99` (compacted keys, re-keying per (contract,address) via
  binary `AccountKey`) shipped in this image — the 07-28 entry's "no re-keying"
  deviation no longer holds.
- **devops `ethStacksForst`** (pushed): new
  `hprod/k8s-apps/flink-jobs-operator/eth/eth-stacks-v12/` (operator chart;
  topics `eth_stacks_v12.clickhouse-{0,1,2}`, source `eth_transfers_v3`, TM
  3×3 slots ×16g, parallelism 9, `pipeline.max-parallelism` left to Flink's
  auto-derivation = 128 at this size, Yordan's call) + two fleet-template
  changes: ForSt metrics trio alongside the rocksdb trio, and
  `state.backend.forst.cache.size-based-limit: "100g"` as a guarded template
  default (unset, ForSt's only bound is "evict at <256MB free node disk" —
  protects ForSt from a full disk, not the node from ForSt; TMs sit on
  `emptyDir`, no PVC/limit anywhere in the flink fleet).

**First deploy crashed** — `NoSuchMethodError:
scala.collection.Iterable.map(Function1)` in `Main.<clinit>`. Root cause: the
stock `apache/flink:2.3.0` image ships `lib/flink-scala_2.12` which BUNDLES
the Scala 2.12 stdlib; `scala.*` is parent-first, so it shadowed our
`scala-library-2.13.16` → 2.13-only signatures vanished. This was the
flinkUpgrade task's "watch first deploy" risk landing. Diagnostic path:
`kubectl logs` on the JM → `Classpath:` banner → spot the duplicate stdlib.
Fix: `rm $FLINK_HOME/lib/flink-scala_2.12-*.jar` in the Dockerfile (upstream
"bring your own Scala" guidance; nothing else needs it). Affects EVERY job
that moves to the 2.3 image, not just ForSt. Discussed but deferred: an
in-image smoke test (`flink run --target local` + a `--buildGraphOnly` mode)
that would have caught this in CI — sbt tests are structurally blind to
image-classpath composition.

**Baseline readings, ~10 min after start (2026-07-31 ~11:28 UTC, job
`8294e141d423608ff2fe355753ce593f`, from-genesis Kafka replay):**

- Job RUNNING, 0 pod restarts, all 9 `Create Stack Changes-ETH` subtasks up.
- **Disaggregation confirmed**: TM logs show the async operator
  (`op_AsyncKeyedProcessOperator_b5c69ee7…`) with `remoteForStPath:
  s3://flink-checkpoints-production-eth-stacks-v12/checkpoints/ha/<jobId>/shared/op_…/db`
  and a local cache dir under `/tmp/tm_<pod>/tmp/<jobId>/op_…/db`. The
  `StreamMap` and sort-stage `WindowOperator` instances show `remoteJobPath:
  null` (local-only, as ForSt does for non-async operators).
- Checkpoints: #1 69MB/1.1s, #2 299MB/73s (initial surge), then steady
  ~340MB in 7–9s (#3, #4).
- Throughput: ~24.9k records/s out of the stack-changes operator (5m rate).
- ForSt cache: 25/44/83 MiB used per TM, **100% hit rate, 0 evictions** —
  three orders of magnitude under the 100g cap; consistent with the CH-spike
  "working set is megabytes" prediction (backfill will grow it).
- Fleet context for comparison: node disks 14–23% full (7.1TiB each);
  RocksDB per-TM SST where exported: polygon-stacks 38.4GiB,
  avax-erc20-stacks 15.3GiB, xrp-stacks 12.5GiB (eth-stacks-v11/btc-v12
  predate the metric config and export nothing).

**Metrics for the later comparison pass** (hprod prometheus
`prometheus-hetzner.production.san:30200`):
`flink_taskmanager_job_task_operator_forst_fileCache_{usedBytes,hit,miss,lru_evict,lru_loadback,remainingBytes}`;
checkpoint duration/size (JM log or REST); the per-state
`…_forst_{estimate-num-keys,estimate-live-data-size,total-sst-files-size}`
trio was enabled in the template but had produced **no series yet** at
reading time — re-check; if still absent at next pass, investigate whether
ForSt native metrics register only after first flush or need different keys.

**Open items:** (a) verify the sort stage actually got the async window
operator — log says plain `WindowOperator`, so `enableAsyncState()` may have
quietly fallen back to sync (Path W evidence point; harmless here, state is
one block); (b) output equivalence vs eth-stacks-v11 in CH once backfill
lands (table_qa-style diff on `eth_stacks_v12.*` topics); (c) cache/checkpoint
behavior once state reaches 100s of GB — the actual ForSt thesis test;
(d) exec into a TM (needs rw creds; agent token is read-only) to `du` the
local dirs if log-level evidence isn't enough.

### 2026-07-28 — ETHAccountChanges migrated to async State V2 (branch `ethStacksAsyncState`, uncommitted, for review)

First implementation pass of the ForSt plan's Phase B3 core, scoped to the
`ETHAccountChanges` job and deliberately **output-preserving** (no re-keying, no
bucketing — see deviations below). Shape = the plan's Path P + the
prefetch/compute/commit restructure from the "stacks handler async restructure"
section:

- **Trait split:** `StackChangesState` is now backend-agnostic (abstract
  get/put/remove head + segment-array ops, shared Avro encode/decode helpers in
  its companion); the V1 MapState plumbing moved to a new `MapStackChangesState`
  sub-trait (window + flatmap variants unchanged behind it; the handler and all
  golden tests untouched).
- **`StackWorkingBuffer`** (new, `job/helpers`): per-(contract,address)
  in-memory `StackChangesState` — loaded head + loaded overflow batches +
  write overlay (read-your-writes), raises `BatchNotLoadedException` when a pop
  descends into an unloaded batch, exposes pending head/batch writes for the
  async commit. Reuses the exact encode/decode path → byte-identical stored
  state.
- **`ComputeAccountStackChangesProcess`** (new): async twin of the window
  variant. v2 MapStates with the same names/types ("account-head",
  "account-change-store") + a per-block record buffer MapState; event-time
  timer per block timestamp reproduces the 1 ms-window firing; on fire:
  `groupAndCompress` verbatim → per-address head prefetch fan-out
  (`StateFutureUtils.combineAll`, order-preserving) → synchronous handler runs
  against the buffer → on `BatchNotLoaded`, fetch all batches below the loaded
  top and re-run that address from scratch (deterministic, max 2 rounds) →
  emit in the window variant's exact order → async fan-out commit + emptied-
  address head removal. Late records dropped via `ts <= currentWatermark`
  (same rule as window lateness).
- **Job wiring:** first stage `keyBy(hash%100).enableAsyncState().process(...)`;
  sort stage keeps its window with `WindowedStream.enableAsyncState()` (Path W —
  stateless user function; verified `AsyncWindowOperator` exists in 2.3).
- **Handler:** only change is key derivation moved to companion helpers
  (`headStateKey`/`overflowStateKey`) so the driver prefetches exactly the
  handler's keys.

**Verification:** new end-to-end equivalence test
(`ComputeAccountStackChangesProcessTest`) runs both operators on a real local
Flink (RocksDB backend; async API served via `AsyncKeyedStateBackendAdaptor`)
and asserts record-for-record equality — scenario covers compression, pop
remainder, emptied-address clearing + nonce restart, same-block empty/refill
continuity, per-contract separation, and a 7-segment 3-batch deep pop that
provably exercises the BatchNotLoaded → reload → re-run round. Plus
`StackWorkingBufferTest` (7 cases). Full suite: 116/116 green, incl.
`AllJobGraphsBuildTest` (async graph translation works for both stages).

**Findings settling plan questions:**
- Phase-0 spike (b) is effectively answered: KeyedProcessFunction + event-time
  timers + v2 async state works, including same-key ordering across
  timer-driven flushes (D/C clearing-then-rebirth would corrupt otherwise).
- V2 API on non-async backends works through the runtime's
  `AsyncKeyedStateBackendAdaptor` (futures complete inline) — so the operator
  migration is decoupled from the ForSt backend flip (Phase A) and testable on
  RocksDB.
- `KeyedStream.enableAsyncState()` + `WindowedStream.enableAsyncState()` and
  v2 descriptors (which accept `Class` directly) confirmed on Flink 2.3.0 from
  the shipped jars; `RuntimeContext` has the v2 accessor overloads.

**Deviations from the full B3 scope (deferred deliberately):**
- No re-keying to binary `(contract, address)` (plan's lever-3c requirement)
  and no odt-bucketing re-introduction (`788c0ded`) — this pass keeps today's
  coarse hash%100 keying and byte-identical output so equivalence is provable;
  the re-key + bucketing change output/state layout and belong to a follow-up
  step with its own validation.
- Size-adaptive round-1 prefetch (topIdx ≤ K) not implemented — round 1 loads
  the head only; the rare deep pop pays one extra async round (re-run). Cheap
  follow-up optimization if profiling justifies it.
- `ETHAccountChangesExact` (flatmap variant) and `XRPStacks` still on V1 sync —
  separate phases in the plan.
- Backend still `rocksdb` in `package.scala` — the ForSt flip is Phase A,
  intentionally not on this branch.

Per-record RMW of the block buffer (get+put of a growing list per record) is
the one knowingly-accepted inefficiency vs the window operator's internal
namespaced ListState append; fine at hash%100 granularity, revisit if hot.

### 2026-07-24 (cont. 2) — ForSt project-health check (Yordan's "is ForSt abandoned?" question)

Yordan noticed https://github.com/ververica/ForSt/ looks abandoned and asked
whether the only async-capable backend is in fact inactive. Verified
2026-07-24 — the concern conflates two layers:

- **The state backend is actively developed inside apache/flink.**
  `flink-statebackend-forst` releases track every Flink release through
  May 2026 (2.2.1/2.1.2/2.0.2); latest module commit 2026-07-17
  (FLINK-40157, MapState putAll serialization fix, by Zakelly). The async
  execution model (FLIP-425), State V2 API, remote file cache, and
  checkpoint fast-duplication are all Java code there, not in the C++ fork.
- **ververica/ForSt is the thin C++ layer** (RocksDB fork: remote-FS `Env`
  over JNI + small primitives). JNI releases: 0.1.0-beta (2024-04) →
  0.1.8 (2025-03); Flink 2.3 and master both pin exactly `forstjni 0.1.8` —
  nothing to release since. Same quiet-vendored-fork pattern as FRocksDB,
  which has backed the RocksDB backend for ~8 years. Jan-2026 commits are
  README trademark cleanup, not a death rattle.
- **Legitimate residual risks (sharpen migration-plan risk #2, don't change
  direction):** single-vendor C++ fork outside ASF governance (bus factor);
  RocksDB baseline frozen (no rebase activity); a native-layer bug found in
  our benchmark would wait on a Ververica release cycle. Mitigations are
  exactly the plan's existing ones: Phase-0 spikes, Phase-A canary, pinned
  versions, per-operator V1-on-local fallback.

### 2026-07-24 (cont.) — lever 4 (cheapen ots) implemented, reviewed, REVERTED by decision

Implemented and fully working (`SegmentStorageCodec`: ots stored in seconds +
(nonce, ots) delta-encoded vs the previous segment within each batch, wired
into the four `StackChangesState` accessors, round-trip/guard/size tests, all
green), then **reverted** after Yordan questioned the cost/benefit and a
composition re-review agreed with him:

- **Gains are single-digit.** The dominant tail (1-segment dormant
  addresses, ~80% of ~200M live ETH addresses) gains ~1 byte per ~65 B KV
  (~1.5%) — delta encoding does nothing for a first segment, only ms→s does.
  Active 6-segment addresses gain ~12%; burn-class whales ~40% of their
  segment bytes but they are ~100–300 MB fleet-wide. Weighted: **~5–6% of
  ETH stacks state** — vs the structural 30–40%+ of lever 2 + clearing.
  Insight 1 said it upfront: constant factors on the dormant axis don't pay.
- **Costs are permanent.** Stored state stops being literal (state-processor
  readout, rebuild tooling, savepoint debugging all need the codec); it bakes
  a "whole-second timestamps forever, all account-model chains" invariant
  into the state format (ICP is ns-native upstream — one extractor schema
  change from a runtime `require` failure); the ForSt Phase-B restructure
  would have to port it; golden-test timestamps had to be scaled ×1000,
  denting `stack_fold.py` vector correspondence.
- Re-appliable cheaply if the step-1 readout shows segment bytes matter more
  than estimated: the design and pitfalls are all recorded here (the "delta
  only, no ms→s division" variant avoids the timestamp invariant at ~2/3 of
  the gain).

**Kept:** the stale-comment fix in `ComputeAccountStackChangesTimeWindow`
("we encode Strings as ASCII bytes" → KeyGenerator hex-decode reality; the
lever-3 finding). 107/107 tests pass after the revert.

Levers 1–4 are now all resolved (1 rejected, 2+clearing shipped to the
branch, 3 reduced to design constraints, 4 rejected on review). Lever 5
(RocksDB tuning) is moot if ForSt proceeds — **the current-architecture
track is wrapped up**; next is the async/ForSt feasibility evaluation
(Phase 0 spikes in
[forst-async-migration-plan.md](./forst-async-migration-plan.md)).

### 2026-07-24 — lever 3 (binary keys) evaluated: premise wrong for ETH; plan split into 3a/3b/3c

**Premise correction.** The lever assumed ETH addresses sit in state keys as
42-byte ASCII hex. False: `KeyGenerator.stringAsKey`
(`common/store/KeyGenerator.scala:25`) hex-decodes any `0x`-prefixed string —
and has since the function was introduced (pre-Scala-2.13 history);
`KeyGeneratorTest` locks it in. So today's MapState user keys are already
binary: ERC-20 head KV key = 20 B contract + 20 B address; ETH-native =
20 B address + `"ETH"` 3 B. **There is no standalone ETH win — that part of
lever 3 is closed.** (The comment at
`ComputeAccountStackChangesTimeWindow.scala:130` — "We encode the Strings as
ASCII bytes" — is stale/misleading and should be fixed on the next touch.)

Where ASCII (and other key fat) actually remains — three sub-items:

**3a. XRP stacks keys are still ASCII (real, modest — ship with the lever-2
relayout).** `XRPStacks` keys state by `(issuerCurrency, address)` through the
same `KeyGenerator`; neither part starts with `0x`, so both stay ASCII:
address = base58 r-address (~33–34 B), contract = `"XRP"` (3 B) for native or
`issuer + "/" + currency` (~38–72 B) for IOUs. The codec already exists in the
repo and is prod-proven in the XRP balances jobs:
`xrp.serializeAddressWithType` (`xrp/package.scala:96`) = 1 tag byte +
`NumberBaseUtils` base58 decode (~25 B incl. version+checksum; 21 B if the
4-byte checksum is stripped — fine since stacks never decodes keys back).
Plan:
- Introduce a per-job address codec on the stacks key path (default =
  current `KeyGenerator` behavior, so ETH jobs are untouched byte-for-byte;
  `XRPStacks` passes the base58 codec). `HandlerOneAccountChange` builds all
  keys via `KeyGenerator.keyFor` — one seam to parameterize.
- Encode IOU contracts structurally too: tag + decoded issuer (20 B) +
  currency bytes (XRP currency codes are 3-ASCII or 40-hex — the hex form
  also halves).
- Estimated: native head KV key 37 B → ~25–29 B (−25–30%); IOU pair keys
  ~72 B → ~46 B. Share of total XRP state = the step-1 readout's
  key-vs-value split; dormant 1–2-segment addresses have small values, so
  key bytes are likely 30–40% of head-KV bytes there.
- Bonus: tag bytes + fixed-length decoded forms remove a latent injectivity
  wart — today's `keyFor` concatenates variable-length ASCII parts with no
  separator, and `getIssuerCurrency` can produce `"/CUR"` for a missing
  issuer.
- Migration: changes every stored map key → same savepoint break as lever 2,
  so it must ride the same XRP relayout/backfill (one migration, not two).
  Output is unchanged (state keys never leave the operator), so
  `compare_xrp_experimental.py` should show byte-identical output.
- Test watch-out: leading `'r'` in the XRP alphabet is digit 0 —
  leading-zero handling in `NumberBaseUtils` round-trips must be covered
  (prod-proven in balances, but stacks tests should pin it anyway).

**3b. FlatMap variant's Flink key is ASCII (real, but don't do standalone).**
`ETHAccountChangesExact` keys by contract `String` — every RocksDB entry of
the ERC-20 exact jobs carries ~43 B of ASCII contract in its serialized Flink
key (the window jobs carry only a 4 B `Int`). Subsumed by the ForSt re-key
(3c); a standalone fix would be a throwaway state migration.

**3c. The real ETH payoff: binary keyBy key as a design REQUIREMENT of the
ForSt re-key (forst-async-migration Phase B).** After re-keying by true
`(contract, address)`, the Flink key enters every RocksDB/ForSt state key. A
naive `(String, String)` key costs ~86 B ASCII + length prefixes per KV —
i.e. the re-key would *regress* ETH key bytes by ~40–45 B/KV vs today
(4 B Int + binary map key). Plan: key by a dedicated binary key type —
e.g. `AccountKey(bytes: ArraySeq[Byte])` (hex-decoded contract+address;
`ArraySeq` gives deterministic MurmurHash3 value semantics for key-group
assignment) with a compact custom `TypeSerializer`. Then the map-state user
keys drop `(contract, address)` entirely: head becomes `ValueState`
(zero user-key bytes), overflow keyed by `Long` batchIndex only — roughly
byte-neutral vs today instead of a regression, with Insight 7's progress
state folded in. Action: add this as an explicit design point in
[forst-async-migration-plan.md](./forst-async-migration-plan.md) Phase B.

**Net effect on the lever:** no quick standalone win exists. 3a is the only
near-term item (piggybacks on the lever-2 migration); 3c is deferred into the
ForSt migration where it prevents a regression rather than harvesting a gain.
The step-1 savepoint readout still prices 3a exactly (key-vs-value byte
split per state).

### 2026-07-23 — lever 1 implemented then REVERTED by decision; lever 2 implemented

**Lever 1 (mint fresh nonce on pop): implemented, reviewed, and rejected.**
Yordan keeps the coin-ledger semantics: the nonce identifies a unique coin and
a `-1` row echoes the spent coin's construction nonce. His argument, verified
in discussion: under current semantics every coin has exactly one birth row
and ≤1 death row (partial spends = whole-coin pop + new remainder coin), so
per-address surviving coins **including their nonces** are exactly recoverable
from the output table via anti-join (`+1` nonces with no matching `-1`), and
even exact stack order via sort by `(ots, nonce)`. **Correction to Insight 2:**
"the only non-reconstructible field is the per-segment nonce" is wrong for
current semantics — it becomes true only under bucketing/cohort merging, which
breaks the birth/death pairing invariant. Since bucketing/cohort is not
planned (estimated gains too small — see 2026-07-16 revert), lever 1's payoff
doesn't justify giving up the coin model. **Lever 1 is closed** unless the
bucketing direction is revived.

Kept from the lever-1 work (still valid): the "drop nonce entirely, key by
`(assetRefId, address, sign, dt, odt)`" variant is dead — measured on prod
2026-07-21: 184,560 colliding key groups / 507,996 rows in one day (worst
group 513), incl. same-key rows with identical amounts; `eth_stacks_shard_v4`
is `ReplicatedReplacingMergeTree ORDER BY (assetRefId, address, sign, dt,
nonce)` so key duplicates silently collapse; the Kafka record key also embeds
nonce (`AccountModelChange.serializationKey`). Also: the window-harness golden
tests in `ComputeAccountSegmentChangesTest` are commented out wholesale — live
handler coverage is thinner than it looks.

**Lever 2 (fold `sizeNonceState` into the top batch's KV): implemented**,
uncommitted on `etherbi-flink` branch `stacksOptimizations` for review
(alongside the Flink 2.3 + dependency commits; all 105 tests + assembly pass).
Design — "head entry" layout, account-model twins only (window + flatmap;
UTXO's `NoncePair` layout untouched):

- New Avro record `AccountStackHead {size, nonce, segments}`
  (`account-head.avsc`, references `StorageSegment` cross-file — sbt-avro
  handles it). `StorageSegments` itself is untouched, so overflow entries and
  the UTXO twin carry zero overhead from this change.
- New MapState `account-head`, keyed by `(contract, address)` — the former
  `sizeNonceState` ASCII key — holding `{size, nonce, top batch}` in ONE KV.
  The Kryo `(Long, Long)` tuple state `nonce` is gone (also −1 Kryo type).
- `account-change-store` now holds only full **overflow** batches below the
  top, keyed `(contract, address, batchIndex)`; batch transitions flush/pull
  between head and overflow (`increase/decreaseArraySegmentUsed`).
- Dominant-tail addresses (≤1 batch): 2 KVs → 1 KV, key bytes halved, and
  1 state read + 1 write per touch (was 2–3 reads + 2 writes).
- Bonus: `putHead` persists only the live prefix of the top batch — the old
  layout kept stale popped segments in the stored batch value until
  overwritten.
- Emptied addresses keep their head entry (`size=0`, empty batch): the nonce
  counter must survive emptying or coin nonces could be reused (test encodes
  this invariant).

Landing constraint: state relayout — old savepoints are NOT restorable
(values moved from the `nonce` state into `account-head`); needs a
state-processor migration job or a fresh backfill. The step-1 measurement
descriptors below describe the OLD layout, which is what prod savepoints will
contain until this lands.

### 2026-07-23 (cont.) — emptied-address clearing: design agreed, implemented for review

Follow-on discussion to lever 2. Measured on prod: **39.5% of all ETH-native
addresses ever seen are currently emptied** (1/1024 sample of `eth_balances`,
argMax(balance) ≈ 0; ~131M of ~331M addresses) — each holding a dead head
entry (~40–60 B incl. RocksDB overhead) forever: ~5–7 GB dead weight on ETH
native alone, more on ERC-20 pairs; compaction/checkpoint/recovery pay for it
forever.

**Rejected designs:** emptiedAt + offline savepoint sweep, and TTL'd
tombstones — both erase state at ops-chosen / wall-clock times, so a re-run
(recomputation, backfill, validation harness) produces different nonce
sequences than prod history. Yordan requires clearing to be a **pure function
of the input stream**.

**Agreed design (Yordan's): clear at block boundaries.**
- Window variant: the window IS the block — after processing all of a block's
  changes, remove head entries whose final `size == 0`. Intra-block
  empty→refill cycles (forwarders, flash loans) keep full counter continuity
  because clearing only sees the block's final state.
- FlatMap variant: a `pendingClear` MapState (addresses emptied during the
  current block, bounded, per contract key) drained on block advance —
  the `progressState` hook already detects transitions; drain re-checks
  `size == 0` so same-block rebirth needs no bookkeeping. Gives the flatmap
  twin the same state win AND output equivalence with the window variant.
- Cross-block nonce reuse is collision-free on CH keys iff block timestamps
  strictly increase (ETH slots, XRP monotonic close times). For
  same-second-block chains the **primary key must gain `blockNumber`**:
  `ORDER BY (assetRefId, address, sign, dt, blockNumber, nonce)` — dt-prefix
  queries unaffected, Kafka key already block-scoped. Affected tables measured:
  `arb_erc20_stacks` 8.2B, `opt_erc20_stacks` 8.3B, `avax_erc20_stacks` 5.9B,
  `polygon_stacks` 5.4B rows — per-chain rebuild+backfill, independent
  migrations. Audit `clickhouse-tables` for hardcoded ORDER BY (rebuild SQL,
  table_qa) per chain.
- Semantics: nonce uniqueness becomes scoped to a **holding period** (resets
  after an empty block boundary). `-1`→`+1` pairing stays exact via nearest
  preceding birth; the global set-difference anti-join no longer holds for
  reborn addresses. Approved by Yordan.
- The one-time backlog flush is the lever-2 migration itself (don't carry
  `size == 0` entries into `account-head`).
- **Flag for [forst-async-migration-plan.md](./forst-async-migration-plan.md):**
  after re-keying to `(contract, address)` there is no per-contract map to
  drain — needs per-key event-time timers (deterministic but conflicts with
  the plan's "no timers" simplification) or an equivalent; Phase-B design
  point.

Implementation (same session, uncommitted on `stacksOptimizations` with lever
2): clearing is **unconditional** (Yordan removed the config-flag variation) —
window-end clearing in `ComputeAccountStackChangesTimeWindow.apply`,
`pendingClear` MapState drained on block advance in
`ComputeAccountStackChangesFlatMap`, harness tests proving nonce restart after
clearing and same-block continuity. **Hard deploy prerequisite therefore:**
same-second-block chains (arb/opt/avax/polygon jobs, if any run these
operators) must get `blockNumber` into their stacks-table ORDER BY *before*
this build reaches them — there is no flag to hold clearing back anymore.
The window-variant clearing needs no state (windows replay atomically; local
map in `apply`); the flatmap needs `pendingClear` in keyed state because
checkpoint barriers land mid-block (a heap set would leak entries on
restore/rescale and would break keyed-context scoping).

### 2026-07-16 — ForSt + all-keyed-async migration plan approved

Yordan confirmed the direction and widened it: **ForSt becomes the sole state
backend (no config knob)** and **every keyed-state operator migrates to the async
State V2 API**, removing the repo's dependency on the V1 *keyed* state API. Deploys
run pinned images, so master can carry a single backend freely. Approved plan
written to [forst-async-migration-plan.md](./forst-async-migration-plan.md)
(planning only this session; implementation deferred).

Key outcomes of the session:
- **Full stateful-operator inventory:** 13 keyed-state operators (9 window-based on
  the 1 ms/block tumbling window, 4 per-record flatmaps) + 3 stateless sort windows
  + operator-state users (dedup filters, metric counters). **No timers, no TTL**
  anywhere — removes two async/ForSt maturity risks.
- **Corrected window/async finding (see Insight 5):** DataStream windows **can** use
  async State V2 on ForSt in 2.3 (FLIP-488). Eliminating V1 does *not* require
  ripping out windowing; Path W (keep window + `enableAsyncState()`) vs Path P
  (convert to `KeyedProcessFunction`) is gated by a Phase-0 spike on whether a
  stateful `WindowFunction.apply()` can chain `StateFuture`s.
- **Backend flip decouples from the API migration:** under V1 sync, ForSt degrades to
  a local store, so `rocksdb → forst` can ship globally first (Phase A) at low risk
  before any operator is converted.
- **Checkpoint dedup confirmed** (the original question): with ForSt primary-dir ==
  checkpoint-dir (default), checkpoints *reference* the already-remote files
  (fast-duplicate), so state is **not** duplicated the way RocksDB (local primary +
  S3 backup) duplicates it today.
- **"Zero V1" floor:** operator state (`CheckpointedFunction`) has no V2 equivalent;
  recommended scope is "no V1 *keyed* state" (Option 1), with strict zero-V1
  (Option 2) available at the cost of re-keying the dedup filter and resetting two
  observability counters on restart. Decision still open (only affects Phase C).
- **Stacks handler restructure designed:** async prefetch → unchanged in-memory
  push/pop → async commit; size-adaptive prefetch + bounded (≤2-round) re-run for
  deep pops; fold size/nonce/progress into one `AccountHeader` KV; async golden-test
  harness via completed-`StateFuture` mocks.
- Phase-0 spike also tests **State-Processor-API vs ForSt/V2** — likely unsupported,
  which would block this task's Plan step 1 (byte-level state measurement via
  savepoints); fallback: measure via a debug operator/metrics or defer.
- **Reverted odt cohort-batching from `batchStacksOdt`** at Yordan's request — the
  modest <30% win didn't justify shipping the standalone lossy lever. Code restored to
  the pre-bucketing baseline (`39b44652`); all 26 stacks/job-graph tests pass. The ADR
  `configurable-odt-bucketing.md` is kept, status→reverted. **Recoverable at `788c0ded`;
  must be re-introduced in the async migration's Phase B3** (per-record shape needs it —
  Insight 5). Not committed — left in the working tree for review.

### 2026-07-15 — enriched with the CH-spike measurements; C on hold, this task active

Yordan put architecture C (stacks-in-clickhouse) on hold and named Flink
optimization the active direction. Imported what transfers from C's spike:
the "Measured inputs" section above (working-set / whale / distribution
numbers from prod, the hard validation of Insight 2, the executable fold
oracle on branch `clickhouseStacks`, rebuild-SQL + read-amplification
lessons, rebuild-scope facts). Step 1 re-scoped: premise checks done via
CH; savepoint readout still owed for byte-level lever pricing.

### 2026-07-14 — investigation distilled into this task

Discussion session in `etherbi-flink` (branch `batchStacksOdt`): reviewed the
stacks concept + bucketing ADR + handler/state code, established Insights
1–8 above, corrected the "RocksDB already tiers to S3" assumption, and
converged on ForSt + per-address async restructure as the direction, with
ClickHouse rebuild demoted to bootstrap/DR. Constant-factor levers parked for
later evaluation. No code written yet. Step 1 bootstrap section added
(operator uids, descriptors, measurement spec, access prerequisites) so the
measurement can start in a fresh session.
