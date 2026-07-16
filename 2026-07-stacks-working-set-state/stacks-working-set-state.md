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
rows, ordered by `odt`. The only non-reconstructible field is the per-segment
`nonce`. So **Flink state is a materialized view of the job's own sink** and
can be rebuilt from ClickHouse at any time. Even independent of the main
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

1. **Drop stored `nonce`; mint fresh nonce on pop** (the bucketing merge path
   already mints on emit; downstream pairs by `odt`, never `nonce`). Segment
   becomes `(ots, value)`. Prerequisite for clean CH-rebuild (Insight 2).
   Needs the same raw-table `ORDER BY` distinct-rows check as the bucketing
   ADR.
2. **Fold `sizeNonceState` into the top batch's KV** — halves KV count and
   key bytes for the dominant tail of 1–2-segment dormant addresses (the
   ASCII `(contract, address)` key is stored twice today). Combines with
   Insight 7.
3. **Binary-encode keys** — ETH addresses are hex-as-ASCII: 42 → 20 bytes,
   lossless, no hashing/collisions. Chain-specific for XRP (base58).
4. **Cheapen `ots`** — store seconds (or bucket index) not ms, delta-encode
   within a batch; Avro zigzag varints turn ~6 bytes into 1–2.
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
