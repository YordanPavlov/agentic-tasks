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

The async/per-record shape cannot run the window variant's block-level
same-sign compaction before touching state. But 5-min odt bucketing merges
same-bucket inflows into the open top segment anyway, subsuming most of what
block-level compaction bought. **The bucketing branch is what makes the
per-record shape state-competitive** — the two tasks compose. Residual cost:
one state commit per record instead of per address per block window — an I/O
question the async batching should largely absorb; measure it.

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

### 2026-07-14 — investigation distilled into this task

Discussion session in `etherbi-flink` (branch `batchStacksOdt`): reviewed the
stacks concept + bucketing ADR + handler/state code, established Insights
1–8 above, corrected the "RocksDB already tiers to S3" assumption, and
converged on ForSt + per-address async restructure as the direction, with
ClickHouse rebuild demoted to bootstrap/DR. Constant-factor levers parked for
later evaluation. No code written yet. Step 1 bootstrap section added
(operator uids, descriptors, measurement spec, access prerequisites) so the
measurement can start in a fresh session.
