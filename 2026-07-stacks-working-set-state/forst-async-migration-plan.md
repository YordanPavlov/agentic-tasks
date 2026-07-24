# Plan: Migrate all keyed operators to Flink async State V2 + ForSt backend

> **Approved 2026-07-16** (planning phase; implementation deferred to a later stage).
> Design validated by two parallel design passes (topology migration; stacks-handler
> restructure) plus framework research against Flink 2.3 docs/FLIPs. Companion to
> [stacks-working-set-state.md](./stacks-working-set-state.md); this is the concrete
> execution plan for the ForSt + async direction that task selected.

## Context

**Why.** The stacks jobs' production bottleneck is keyed state that grows
`O(every address that ever existed × its live cohorts)` — monotonic with chain age.
RocksDB (today's backend) keeps *all* primary state on TaskManager local disk and only
backs it up to S3, so state is hard-bounded by local disk, compaction rewrites dormant
state forever, and recovery/rescale downloads everything first. Cohort bucketing (prior
branch) was a <30% constant-factor lever on the wrong axis.

**Direction.** Flink 2.x's **ForSt** disaggregated backend makes S3 the *primary* store,
local disk a working-set cache, and checkpoints *reference* already-remote files
(fast-duplicate on the same DFS) instead of re-uploading — so state is bounded by the
active working set, checkpoints are near-constant-time, and recovery starts immediately.
ForSt only disaggregates for operators on the **async State V2 API**; under V1 sync it is
local-only.

**Decision.** Make **ForSt the sole state backend** (no config knob — deploys run pinned
images, so master can carry a single backend freely), migrate **every keyed-state
operator** to async State V2, and remove the repo's dependency on the V1 **keyed** state
API. Add an ADR under `etherbi-flink/docs/decisions/`.

### Corrected finding (supersedes an earlier assumption)

An earlier discussion framing held that DataStream windows can't use async state. **That
was wrong** (it rested on absence-of-evidence). Verified: **DataStream keyed window
operators support async State V2 on ForSt in 2.3** — FLIP-488, `enableAsyncState()` on
`WindowedStream`; async window operator integrated in 2.0.0 (FLINK-37028), `trigger`
async support in 2.2.0 (FLINK-38363). **Eliminating V1 does NOT require removing
windowing.** Confidence ~90% on the capability; **~60%** on whether a user
`WindowFunction.apply()` doing its *own* keyed-state I/O can chain `StateFuture`s cleanly
(`.apply()` is a synchronous batch callback; futures can't be blocked). That nuance is
the single biggest open question → gated by a Phase-0 spike.

### Environment facts that de-risk it

- Deploys run **pinned images**; master can change freely without touching live jobs, and
  (project memory) releases are for **new runs** — no savepoint/state backward-compat
  requirement. Confirm the jobs are **replay-from-Kafka** (KafkaSource earliest/block
  binary-search; XRP baseline replay) — if so, re-keying to true `(contract, address)`
  and V1→V2 serializer changes carry **no state-migration burden** (state rebuilt on
  replay). This assumption is load-bearing; verify in Phase 0.
- **No timers and no TTL** anywhere in the repo — removes two big async/ForSt maturity
  risks. (Timers are used *by* Path P below, but there are none to migrate.)
- **No downstream block-completeness gate.** `ETHAccountChanges`'s second stage
  (`KeyedStreamSortWindow`, `ETHAccountChanges.scala:75-80`) is a pure sort/re-emit, not a
  completeness consumer. Downstream (`clickhouse-tables`) treats output as an invertible
  delta stream paired by `odt`, aggregated by `(dt, odt)` — arrival order invisible. This
  largely dissolves the original Insight 8 concern.

### Bucketing status (2026-07-16)

The odt cohort-bucketing code was **reverted from `batchStacksOdt`** (modest <30% win, a
constant-factor lever the team chose not to ship standalone). It is **recoverable at commit
`788c0ded`**. This migration *depends* on it: 5-min bucketing subsumes `groupAndCompress`
for the per-record async shape (Insight 5), so **Phase B3 (ETH/ERC20 stacks) must
re-introduce the bucketing feature** (cherry-pick/re-apply `788c0ded`, then re-key/async it).
The ADR `configurable-odt-bucketing.md` is retained (status: reverted) with the rationale
and XRP validation.

## Scope & the "no V1" floor

**In scope:** ForSt backend (global) + drop RocksDB dep; migrate all **13 keyed-state
operators** to async V2; the ADR.

**Operator (non-keyed) state has no V2 equivalent.** `CheckpointedFunction` /
`OperatorStateStore` (the dedup filters' union `ListState`, and two metric-counter
`ListState`s) is out of State V2's scope. So literal "no `org.apache.flink.api.common.state.*`
import at all" is not achievable while keeping operator state. Two options:
- **Option 1 (recommended):** goal = "no V1 **keyed** state." Operator state is a separate
  API surface, unaffected by the ForSt/async migration; CI gate whitelists those 3 files.
- **Option 2 (strict zero-V1):** re-key `DataStreamDeduplicateFilter` by partition to a V2
  keyed `ValueState` (loses the "one instance sees all partitions" property) and drop the
  counter-restore `ListState`s (duplicate/late-record **metrics reset to 0 on restart** —
  observability only, not correctness). Choose only if literal zero-V1 is required.

*(Decision pending — recommend Option 1; only affects Phase C.)*

## The 13 keyed-state operators — target shape & difficulty (from full inventory)

**Path W** = keep window + `enableAsyncState()` + v2 descriptors (only if Phase-0 proves
stateful `apply()` works on async). **Path P** = `KeyedProcessFunction` + v2 `ListState`
block buffer + block-boundary timer/flush (fallback; also the uniform target if W fails).
All paths in `etherbi-flink`.

Window + keyed state (9):
1. `UTXOBalanceCalculation` (`utxo/helpers/AddressBalanceCalculation.scala:28`) — **Med**;
   remove non-prod `Thread.sleep(Long.MaxValue)` (blocking illegal under async).
2. `AddressBalanceCalculation` UTXO (`:114`) — **Med**; per-entity key natural (Path P).
3. `UTXOSinkTimeCorrect` (`utxo/helpers/UTXOSinkTimeCorrect.scala:16`) — **Med**; block-order-dependent sort.
4. `ComputeAccountStackChangesTimeWindow` (`job/ComputeAccountStackChangesTimeWindow.scala:105`)
   — **Med-High**; converge onto the per-record flatmap twin (retire the window variant).
5. `AddressBalancesCalculationByContract` (`addressbalances/AddressBalancesCalculation.scala:39`)
   — **High**; **synchronous `BalancesProxyServicePerContract` HTTP/CH call** inside state
   handling must move to Flink Async I/O or a rare non-async branch.
6. `TotalBalanceCalculation` (`addressbalances/TotalBalanceCalculation.scala:29`) — **Med**; single key by design.
7. `OrderAndAdjustTimestamps` (`xrp/balances/OrderAndAdjustTimestamps.scala:25`) — **Low-Med**;
   already `TypeInformation`-based descriptors → best template.
8. `PerAddressHistoricalAdjustments` (`xrp/balances/PerAddressHistoricalAdjustments.scala:26`)
   — **Med**; empty-key passthrough branch needs care.
9. `FlinkStreamDeduplication` (`job/helpers/StreamDeduplicator.scala:146`) — **High**; mixed
   keyed `ValueState` + operator `ListState`; windowless dedup-key derivation.

Flatmap / per-record + keyed state (4):
10. `ComputeAccountStackChangesFlatMap` (`job/ComputeAccountStackChangesFlatMap.scala:24`) —
    **Med**; the stacks migration base (see crux section).
11. `ComputeUTXOAccountSegmentChanges` (`job/helpers/ComputeUTXOAccountSegmentChanges.scala:29`)
    — **High** (hardest async chain: loop of dependent `storeBatch.get/remove/put`); can drop
    `CheckpointedFunction` (empty `snapshotState`, no operator state).
12. `AddressBalancesCalculationFlatMap` (`addressbalances/AddressBalancesCalculationFlatMap.scala:82`)
    — **High**; async iteration over `addressCurrentBalanceState.entries()` + proxy caveat.
13. `ReorderingFlatMap` (`job/KafkaTopicSorter.scala:73`) — **Med**; keyed `ListState`+`ValueState`; keep Kryo `TypeInformation`.

Stateless windows riding the WindowOperator's internal keyed state (3):
`KeyedStreamSortWindow`, `SortWindowFunction`, `CardanoOrderingSink` — **Low** each; cheapest
via Path W (`enableAsyncState()` on the windowed stream makes the internal buffer V2).

## Design — backend + dependency changes

- `build.sbt:20` — remove `flink-statebackend-rocksdb`; add
  `"org.apache.flink" % "flink-statebackend-forst" % flinkVersion % "provided"`.
- `net/santiment/package.scala:283` — `StateBackendOptions.STATE_BACKEND = "forst"` (keep
  `INCREMENTAL_CHECKPOINTS`). Ensure ForSt primary dir == checkpoint dir (default) so
  checkpoints stay lightweight; do **not** set a custom `state.backend.forst.primary-dir`;
  point it at the S3 checkpoint path already configured.
- `.enableAsyncState()` on each keyed/windowed stream feeding a migrated operator.
- **V1 re-introduction guard:** CI grep gate failing on imports of the V1 descriptor
  package (`org.apache.flink.api.common.state.{ValueState,MapState,ListState,Reducing,Aggregating}Descriptor`
  where not `.state.v2.`) — the *descriptor* import is the discriminator since the
  `getState`/`getMapState` accessor names are identical across v1/v2. Also gate on the
  `"rocksdb"` backend literal and `flink-statebackend-rocksdb` in `build.sbt`. Whitelist the
  operator-state files (Option 1).

## Design — general window → per-record recipe (Path P fallback)

The 1 ms tumbling window (event-time == blockNumber, `package.scala:315`) means one firing
== one block per key; the block boundary is a watermark crossing → an event-time timer.
Recipe: `keyBy(sameKey).enableAsyncState().process(new XxxProcess)`; keep the block's
records in a v2 `ListState` buffer; register a block-boundary event-time timer (or reuse
the in-repo **block-change-detection** flush the per-record twins already implement); in
`onTimer`/flush, chain on the buffer's `StateFuture`, run the **existing pure batch
function verbatim** (`groupAndCompress`, `reduceAndSortChanges`, total summation, sort),
`out.collect` inside the completion callback, then `clear()`. The batch functions are
already static/pure — only the state plumbing changes. **Sort/re-emit stages must still
buffer a full block** (can't sort without buffering the sort unit); per-key ordering is
guaranteed by FLIP-425, and emitting `sorted.foreach(out.collect)` inside one per-key
callback preserves the constant-Kafka-key single-partition ordering the sinks rely on.
Descriptor mechanics: v2 descriptors take `TypeInformation`, not `Class` — every
`new MapStateDescriptor(name, classOf[K], classOf[V])` becomes
`new v2.MapStateDescriptor(name, TypeInformation.of(classOf[K]), TypeInformation.of(classOf[V]))`
(`OrderAndAdjustTimestamps` is the existing template).

## Design — stacks handler async restructure (the crux)

Seam: `StackChangesState` trait (`ComputeAccountStackChangesTimeWindow.scala:26-84`) +
`HandlerOneAccountChange` (constructor reads `:43-56`; sole lazy deeper-batch read in
`decreaseArraySegmentUsed:116`; inflow path `:273-312`). Today it is
read-all-up-front → in-memory imperative LIFO → `commit()` write-back — which migrates
cleanly without touching the correctness-critical loop:

- **Phase A — async prefetch** (new): load header + needed batches into an in-memory
  `WorkingBuffer` (`Map[batchIndex → Array[Segment]]` + dirty/removed sets); returns a
  `StateFuture`. Move the constructor-time reads out of `new Handler(...)` into a
  `HandlerOneAccountChange.primed(value, buffer, …)` factory called after prefetch resolves.
- **Phase B — synchronous compute** (unchanged): handler runs against a `StackChangesState`
  view over the buffer; byte-exactness holds because the buffer uses the same Avro
  decode/encode path (`getSegmentArray:55-61` / `pushSegmentArray:50-53`).
- **Phase C — async commit** (new): fan-out `asyncPut`/`asyncRemove` for dirtied/removed
  batches + one `asyncPut` for the header. All `StateFuture` chaining lives in the
  KeyedProcessFunction driver; the hot loop never sees a future.

**Deep pop without blocking — size-adaptive prefetch + bounded re-run** (chosen over a
recursive `thenCompose` inside the pop loop, which would mean editing byte-exact code):
round-1 loads all batches `[0..topIdx]` when small (`topIdx ≤ K`, K≈8), else only the top
batch. If a pop descends into an unloaded batch, the buffer raises a typed
`BatchNotLoaded(i)` (distinguished from genuine `null` corruption via `size`); the driver
`thenCompose`s a load of the remaining lower batches and **re-runs the pure handler from
scratch** (deterministic). Capped at **2 async rounds**. Fits the measured distribution:
median 1 batch / 1 read / 0 re-runs; the 12.5M-segment `burn` whale is push-only so never
pops deep; only a huge *and* deep-popping stack pays the extra round. Inflow only ever
needs the top batch.

**Fold size/nonce + `progressState` into one `AccountHeader` KV** (Avro
`{size, nonce, blockNumber, transactionIndex, logIndex, changePerPrimaryKey}`), one
`asyncGet` + one `asyncPut` per record (down from 4 ops / 2 KVs). Kept as a byte-keyed
`MapState` to preserve the key-gen/mock path; the multi-batch `store` stays byte-keyed
`MapState`. Also fixes the stored-schema/runtime mismatch (`nonce-pair.avsc` records
`nonceUsed+batchIndex`; runtime stores `(size,nonce)`).

**Binary keyBy key — Phase B design REQUIREMENT (lever 3c, evaluated 2026-07-24 in the
parent task).** Re-keying by true `(contract, address)` puts the Flink key into every
RocksDB/ForSt state key. A naive `(String, String)` key costs ~86 B of ASCII + length
prefixes per KV on ERC-20 — a ~40–45 B/KV *regression* vs today (4 B `Int` keyBy + already
hex-decoded binary map keys via `KeyGenerator.stringAsKey`). Key instead by a dedicated
binary type — e.g. `AccountKey(bytes: ArraySeq[Byte])` holding the KeyGenerator-encoded
`(contract, address)` (`ArraySeq` gives deterministic MurmurHash3 value equality for
key-group assignment) with a compact custom `TypeSerializer`. The `(contract, address)`
prefix then leaves the map-state user keys entirely: `AccountHeader` can become
`ValueState` (zero user-key bytes) and the overflow `store` keyed by `Long` batchIndex
only — byte-neutral overall vs today. This amends the "kept as byte-keyed MapState"
choice above: byte-keyed maps may survive as the *mock/harness* interface, but the stored
keys must not duplicate what the Flink key already carries.

**Note the dedup-semantics change:** folding progress per `(contract,address)` changes the
flatmap variant's `isLate` dedup from **per-contract** (`ETHAccountChangesExact.scala:72`
keys by contract) to **per-(contract,address)**. Since upstream is ordered per contract,
each `(contract,address)` subsequence is also ordered and the "two changes per primary key"
rule holds — but verify on real data.

**Async golden-test harness:** a `MapStateV2Mock[V]` over the same `HashMap` returning
**already-completed** `StateFuture`s (chain runs inline, deterministic) — existing
byte-exact assertions in `HandlerOneAccountChangeTest`/`ComputeAccountSegmentChangesTest`
stay unchanged. Add **one deferred-future test** (lower-batch future completes late) to
exercise `BatchNotLoaded → load → re-run` and prove same-key ordering. **Locate the fold
oracle first:** `stack_fold.py` is **not** in `etherbi-flink` — it's on branch
`clickhouseStacks`/`clickhouse-stacks/`; confirm its path before using it for V1↔V2 diffs.

## Phasing

- **Phase 0 — de-risking spikes (throwaway branch, on ForSt).** Decide the whole approach
  before committing: (a) does a **stateful `RichWindowFunction` + v2 descriptors under
  `WindowedStream.enableAsyncState()`** run and produce byte-identical output? → picks
  Path W vs Path P globally; (b) `KeyedProcessFunction` + event-time timer + v2 async state
  on ForSt; (c) **can `flink-state-processor-api` read ForSt/V2 state?** → go/no-go for the
  separate byte-level state-measurement task (task Plan step 1); (d) confirm jobs are
  replay-from-Kafka (no state-migration burden).
- **Phase A — global backend flip, no API change.** rocksdb → forst; all operators stay V1
  sync (ForSt = local store). Deploy every job, confirm no regression vs baselines.
  Low-risk; validates ForSt operationally in isolation and unblocks everything.
- **Phase B1 — XRP stacks + XRP balances first.** `OrderAndAdjustTimestamps` +
  `PerAddressHistoricalAdjustments`: validated baseline + `compare_xrp_experimental.py`
  harness + byte-exact oracle; self-contained (its window output is already the sorted sink
  feed → no `KeyedStreamSortWindow` coupling); `TypeInformation` descriptors already;
  exercises both a single-key and a per-entity-key operator on the safest job.
- **Phase B2 — UTXO balances** (`UTXOBalanceCalculation`, `AddressBalanceCalculation`,
  `UTXOSinkTimeCorrect`, `CardanoOrderingSink`, `KeyedStreamSortWindow`).
- **Phase B3 — ETH/ERC20 stacks** (converge `ComputeAccountStackChangesTimeWindow` onto the
  flatmap twin; `ComputeUTXOAccountSegmentChanges`; ETH sort stage).
- **Phase B4 — ETH/ERC20 address balances** (`AddressBalancesCalculationByContract`/`…FlatMap`,
  `SortWindowFunction`, `TotalBalanceCalculation`) — last among balances (the sync proxy is
  the highest-risk async conversion).
- **Phase B5 — dedup/sorter infra** (`FlinkStreamDeduplication`, `ReorderingFlatMap`) + the
  operator-state Option-1/2 decision.
- **Phase C — lock down.** Remove `flink-statebackend-rocksdb`; enable the CI V1 guard;
  apply the operator-state decision.

## Risk register

1. **Stateful user `WindowFunction` under async (Path W unknown)** — decides the whole
   approach. Mitigation: Phase-0 spike (a); Path P fully specified as fallback. Conf: Med.
2. **ForSt experimental status** — API/behavior churn, remote-FS bugs. Pin 2.3.0 exactly;
   Phase-A canary; keep V1-on-ForSt-local per-operator fallback until each is validated.
   *Project-health check 2026-07-24:* the backend module in apache/flink is actively
   maintained (releases through May 2026, module commit 2026-07-17); the quiet
   `ververica/ForSt` C++ fork is the normal FRocksDB-style vendored-fork pattern (Flink
   pins `forstjni 0.1.8`, 2025-03). Residual: single-vendor C++ fork outside ASF
   governance (bus factor), frozen RocksDB baseline, native-bug fixes gated on a
   Ververica release cycle. See the parent task's 2026-07-24 (cont. 2) session entry.
3. **State-Processor-API vs V2/ForSt likely unsupported** — blocks the separate byte-level
   state-measurement task (reads savepoints via SPA). Phase-0 spike (c); else measure via a
   debug operator/metrics or defer. Conf: Med.
4. **Byte-exactness of stacks push/pop through buffer indirection** — keep handler body
   unchanged; golden tests + fold oracle diff on a replayed range.
5. **Framework same-key serialization contract** — if the runtime doesn't hold the next
   same-key record until the returned `StateFuture` completes (or the driver forgets to
   return the terminal future), `size/nonce` corrupt silently. Verify + deferred-future test.
6. **Blocking calls illegal under async** — `Thread.sleep(Long.MaxValue)` in
   `UTXOBalanceCalculation`; synchronous balance-proxy HTTP/CH calls in the balance
   operators. Remove the sleep; move proxy to Async I/O or a rare non-async branch.
7. **Per-record vs window performance** — repo chose coarse keys citing "bad performance
   once keys×windows grows." Removing windows collapses that product to key cardinality, and
   ForSt moves state off local disk — likely a net win, **unproven**; benchmark at ETH scale.
8. **Downstream single-partition ordering under async** — sort stages retained, emit sorted
   within one per-key/block callback; validate with the comparison harness.
9. **Re-keying / V1→V2 serializer change** — safe only if jobs replay from Kafka (Phase-0 d).
10. **`isLate` dedup semantics** per-contract → per-(contract,address) — verify on real data.
11. **Emptied-address clearing under per-(contract,address) keying** (added 2026-07-23) —
    the block-boundary head-clearing shipped with lever 2 drains a per-contract
    `pendingClear` map on block advance; after re-keying there is no per-contract map to
    drain. Needs per-key event-time timers (deterministic, but conflicts with this plan's
    "no timers" de-risk) or an equivalent ForSt-era mechanism. Phase-B design point — see
    the 2026-07-23 session-log entries in stacks-working-set-state.md.

## Verification

- **Unit/golden**: async-capable state mocks (`MapStateV2Mock` + deferred-future test);
  byte-exact assertions unchanged; `AllJobGraphsBuildTest` green for every job.
- **XRP equivalence**: `compare_xrp_experimental.py` (L1 + intraday + daily) + `stack_fold.py`
  cross-check (locate first).
- **Cluster / ForSt behavior**: confirm async operators disaggregate to S3 while
  sync/operator-state stays local; measure checkpoint duration, recovery time, and
  local-disk footprint vs the RocksDB baseline per phase.

## ADR

New `etherbi-flink/docs/decisions/forst-async-state-backend.md`, house style of
`configurable-odt-bucketing.md`: At-a-glance / Context / Decision (ForSt sole backend +
all-keyed-async + no-V1-keyed-state) / Alternatives rejected (cohort bucketing as
constant-factor; custom CH-hydration fallback; Option-2 strict-zero-V1) / Consequences
(experimental backend, operator-state exception, Path W/P decision, re-keying needs fresh
replay state) / Enabling & phasing. Cross-link `docs/concepts/stacks.md` and the bucketing
ADR.

## Provenance

Derived from a planning session on 2026-07-16 in `etherbi-flink` (branch
`batchStacksOdt`). Framework facts cited from Flink 2.3 official docs, FLIP-423/424/425/427/
428/488, and the VLDB 2.0 disaggregated-state paper. Full state-access inventory and job
wiring traced against the code at that date.
