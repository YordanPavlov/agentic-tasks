# XRP balances async state migration

Migrate the XRPBalances Flink job to the async state API (state V2 / ForSt), following the
ETH stacks pattern (PR #306), and clean up the design issues surfaced along the way.

**Status (2026-09-14):** Java model committed; correction-funnel fix `29c50449` deployed and
evaluated (~40–80x, checkpoints 7 min → 21 s). The follow-up `Order_balances` heap-buffer fix
is **committed as `76a73cb1` on `adoptJavaTypes`** (not deployed); it is checkpoint-incompatible
with the running job, so Yordan schedules a clean re-deploy (fresh bootstrap). Devops side
(ForSt profile + xrp-balances-v6) committed on `xrpBalancesForst` through `5c05777a5`.

## What the branch contains (oldest first)

1. `effd6c47` — async pipeline first cut: pre-grouper + two async process functions,
   byte-identical twin of the window pipeline.
2. `c08b687b` — single `preGroupFlushIntervalMs` config key shared by stacks + XRP
   (per-job keys dropped; nothing overrode them anywhere, devops incl.).
3. `dee3a257` — **timestamp correction moved upfront** (before the per-address keyBy).
   Corrected timestamps are final once assigned, so the per-address stage caches the last
   (blockNumber, correctedTimestamp) pair per key and emits oldDt itself. This deleted the
   forever-growing block->timestamp map (~107M ledgers; per-key alternative ~33M keys,
   adoption-driven, rides on state that must exist anyway). Final stage is now a pure reorder;
   oldDt > dt structurally impossible.
4. `743ec9f6` — `AsyncBlockDrainProcess` base class: the drain scaffolding (append-only buffer,
   epoch-prefixed sequence, drainedWatermark burst guard, trim/write-back) shared by all four
   async process functions, **including the stacks one** (stacks Tier 2 deploy was reverted,
   redeploy planned — so touching it was safe).
5. `e7845c6f` — late records **fail loudly** instead of warn-and-drop: downstream of
   StreamDeduplicator (event-time windows + late side output over per-event block−1 watermarks)
   lateness is impossible; a silent drop would corrupt delta-key balances forever.
6. `b01a9fba` — ingress no longer splits elements (dead code after fail-fast); split hook
   renamed `takeReadyBlocks`, drain-only.
7. `75c7832b` — **window pipeline deleted**; async is the only implementation. Pure per-address
   function kept in place for git history; comments rewritten standalone; pipeline test asserts
   a 16-record golden output instead of the window oracle.

## Key facts for the review / deploy

- Async timer semantics verified against Flink 2.3 sources: `SERIAL_BETWEEN_EPOCH` hardcoded;
  timers fire after the preceding epoch completes and the watermark is forwarded only after the
  drain's async work — "when the watermark passes" is exact in stream position, not wall clock.
- Operators downstream of a timer-draining stage must bucket by **payload block number**, never
  the stream-record timestamp (burst-collapsed) — documented in the base class.
- Per-key state: last (blockNumber, timestamp) folded into ONE ValueState (Tuple2) — separate
  states would repeat the ~40–70 B string key per column family in ForSt.
- Deploy: fresh bootstrap (state not savepoint-compatible with the old window job — key type and
  layout changed), stateBackend=forst, new uids `correct-block-timestamps` / `order-balances`.
  Same playbook as ETH stacks Tier 2 (see 2026-08-eth-stacks-v12-throughput).
- Output equivalence vs prod `xrp_balances` should be verified after the re-run, like the stacks
  hash-multiset comparison — the golden test covers semantics, not history-scale parity.

## 2026-09-10 — devops review, backend-in-code, first deploy + two incidents, Java data model

### Devops branch `xrpBalancesForst` (committed through `5c05777a5`)

Template grew a generic ForSt profile (metrics trio, 10g per-subtask cache cap, HEAP timers,
S3A pool) + per-job `kubernetes.operator.savepoint.format.type: NATIVE` (operator-scoped: the
format is chosen at trigger time by the operator from the CR — cannot be set in Flink code).
Review outcomes, applied in "ForSt backend settings trim":

- `fs.s3a.connection.maximum` 512 → **256**: endpoint is Hetzner Object Storage (Ceph), limits
  750 req/s per bucket AND per source IP, **256 active TCP sessions per source IP** — 512 could
  never be granted per IP anyway. `fs.s3a.threads.max: 64` dropped — it IS the Hadoop 3.3.x
  default (connection.maximum default 96, exhausted on ForSt cold-cache recovery 2026-08-04).
- HEAP timer comment rewritten honestly: the ~2Gi/h leak attribution was REFUTED 2026-08-03
  (real cause: ForSt ReadOptions leak, patched `a6dbcf9d`); HEAP kept deliberately — bounded
  timer count (watermark envelope), cheaper per op, avoids ForSt's young native timer path.
- `hasKey $ov` guards kept: they prevent duplicate YAML keys when a job overrides via
  flinkConfigOverrides (overrides render into the same map later); currently dormant (no job
  overrides any ForSt key).

### stateBackend moved into code (etherbi-flink, committed + deployed)

Deploy gap found: hprod values pinned image `ade93a08` (predates async work AND the
`stateBackend` config option `2647bf6f`) and applicationConf lacked `stateBackend=forst` →
would have silently run RocksDB. Fix (after discussion, simplified per Yordan): backend is a
**code property per job** — `StreamingScalaJob.stateBackend` defaults "rocksdb", `XRPBalances`
overrides "forst"; deploy-config override REMOVED entirely (checkpoints are backend-specific,
so a config flip could never work mid-life anyway; rollout model = fresh deploys). Config key
+ reference.conf default deleted.

### Autoscaler settings for backfill→head lifecycle (discussed, NOT applied)

Fleet pattern (btc-stacks-v12 et al.): operator autoscaler + adaptive scheduler in
flinkConfigOverrides. For "scale up for backfill, drop to 1 at head, minimal rescales"
(ForSt rescale = cold-cache S3 storm): stabilization.interval 2h, metrics.window 2h,
target.utilization 0.7 boundary 0.3, **scale-down.interval 24h**,
**scale-down.max-factor 1.0** (drop 9→1 in ONE rescale, not the 0.6-factor staircase),
pipeline.max-parallelism 128 (matches derived default at p=9 — state-compatible). Verify key
names against deployed operator version (boundary vs utilization.min/max renamed across
releases).

### First prod deploy: two incidents

1. **NoSuchBucket on `flink-checkpoints-production-xrp-balances-v6`** while `mc ls` saw the
   bucket: NOT credentials (same key + endpoint verified) — Hetzner Ceph propagation lag;
   Ceph tx timestamps show 404s 09:47–09:53 UTC, bucket resolvable (403 anon) by 10:00 in both
   path/vhost styles. Meanwhile the application went terminally FAILED **inside a healthy JM
   pod** (Flink 2.3 application-mode: post-submission failure + cleanup also 404ing wedged the
   shutdown; earlier bootstrap failures crashed the JVM normally). Commit `3951d3fa`
   (Helm-only lifecycle) examined and EXONERATED — job-cancel key only runs on spec-change
   upgrades of a healthy job; rendered values identical. Healed by `make destroy` + `make
   install`. Takeaways: run `make buckets` minutes BEFORE first install (creates+verifies both
   buckets with the cluster's own `flink-hadoop` creds); consider
   `kubernetes.operator.job.restart.failed: true` for validation jobs (operator would redeploy
   terminally-FAILED apps).
2. **`NoSuchElementException: head of empty list`** at ParseBalances on the first
   empty-transaction ledger: Scala 2.13+ `List.isEmpty` is `this eq Nil`; Kryo rebuilds Nil as
   a fresh instance → foreach calls head on "non-empty" Nil. Root cause of exposure: Flink 2
   dropped the Scala API, pushing the whole model onto Kryo (same family as the None/Some
   serializers already in FlinkJob). A NilSerializer fix was drafted, then superseded by the
   structural fix below (and reverted).

### Java-shaped data model (option C — uncommitted, awaiting review)

Decision path: A (Nil serializer) vs C (Java model) discussed; C chosen — Kryo on Scala types
is both ~2-4x slower and identity-fragile, and backward state compat is moot (fresh deploys).
Scope: XRP balances + shared code it touches; `XRPRawBlock` deliberately left (no hazard,
shared source schema).

- 9 Java files under `src/main/java/net/santiment/xrp/`: records `ParsedBlock`,
  `ParsedTransaction`, `ParsedBalanceChange`, `BalanceInternal`, `Transaction`,
  `BalanceChangeBlock`, `BalanceChangeBatch`; mutable POJO `Balance` (logic untouched);
  plain-fields `BalanceForSink` (field order = JSON contract). Lists→arrays, Options→nullable,
  the two Eithers → explicit `blockMarker`/`onlyDelta` flags + static factories on
  BalanceInternal. Escrow's odd `Right(newBalance)` preserved as `delta(newBalance,...)` with
  comment. `Timestamped` can't be implemented from Java (lives in a package object →
  `package$Timestamped`, keyword-invalid) — Balance carries a plain `getTimestamp()`.
- Scala: case classes deleted; decoded views (`address`, `transactionHash`, `primaryKey`) as
  package-object extensions; `Balance.Key` → `xrp.BalanceKey` alias; OrderBalances sorts via
  Java `Balance.PRIMARY_KEY_ORDER`; per-address sort keeps exact Option ordering via
  `Option(x.issuer)`. Shared touches: KafkaSourceModels2 Balance deserializer, KafkaSources2
  key alias, XRPStacks map (compiles unchanged).
- Verification: **KryoFreeModelTest** (recursive TypeInformation walk, fails on any
  GenericTypeInfo — the guard against Kryo regressions); **BalanceForSinkJsonTest** (output
  JSON byte-identical to strings captured from the OLD code before conversion);
  **ParseBalancesEmptyBlockTest** (incident regression, empty ledger across keyBy);
  pipeline equivalence test passes with unchanged expectations. 144/144 green.

### Decision record: why the Scala→Java data-model migration

Written down in full because this decision may need to be revisited/justified later.

**The forcing event.** Flink 2 removed the Scala API (`flink-scala`). On Flink 1.x every stream
type of this pipeline was serialized by dedicated Scala serializers (CaseClassSerializer,
Option/Traversable serializers) — compact positional binary, no Kryo anywhere. On Flink 2 the
Java TypeExtractor analyzes the same Scala case classes, cannot see them as POJOs (immutable
fields, no setters/no-arg ctor: the "cannot be used as a POJO type... processed as GenericType"
log lines), and drops EVERY model type onto the generic Kryo fallback. So the migration to
Flink 2 silently changed the serialization regime of the whole model; nothing in the code
changed and nothing warned beyond INFO logs.

**Why Kryo-on-Scala is not acceptable, with evidence:**

1. *Correctness — singleton identity.* Kryo's default path materializes fresh instances of
   Scala's singleton objects. Two production incidents from one root cause family:
   `scala.None` copies failing match clauses (earlier; answered with NoneSerializer/
   SomeSerializer), and `scala.Nil` copies failing `List.isEmpty` — which in Scala 2.13+/3 is
   literally `this eq Nil` — so `foreach` calls `head` on an "non-empty" empty list:
   the 2026-09-10 xrp-balances-v6 crash on the FIRST empty ledger. The failure is
   data-shape-dependent and latent: a job runs fine until the poisonous value shape arrives.
   Per-singleton serializer registration is whack-a-mole against an open set (every
   identity-sensitive singleton in every transitively reachable Scala type).
2. *Performance.* Kryo's reflective generic path is ~2–4x slower than Flink's POJO serializer
   (Flink's own benchmarks) and writes class metadata per object graph node. The tax lands on
   every chained-operator copy (CopyingChainingOutput serializes even within a chain), every
   keyed shuffle, and every serialized state op — i.e. exactly the per-record hot path the
   pre-grouper exists to relieve.
3. *Evolution & fragility.* Flink supports NO schema evolution for Kryo state; Kryo field
   serialization reflects over Scala's synthetic fields, so even a Scala compiler upgrade is a
   state-compat hazard. POJO/record serialization has documented evolution rules.

**Alternatives weighed:**

- *A — register a NilSerializer* (drafted, reverted): fixes the two KNOWN singletons only;
  leaves the perf tax, the evolution gap, and the open set of future singletons. Kept as the
  fallback recommendation for OTHER jobs still on the Scala model.
- *B — chill-scala AllScalaRegistrar:* proper Scala collection serializers for Kryo, but
  Flink-2/Kryo-5 compatibility unverified and still the Kryo perf profile.
- *C2 — TypeInfoFactory per case class:* non-Kryo and fast, but bespoke per-type Scala plumbing
  that would be discarded if the model ever moves to Java — investment in the wrong layer.
- *C1 — Java records/POJOs for boundary-crossing types (CHOSEN):* the data-model layer is the
  ONLY layer Flink introspects; the logic layer (BalancesCalculation, process functions) is
  plain JVM code Flink never looks inside, where Scala costs nothing. Converting just the model
  captures the entire runtime benefit of "rewrite in Java" (native serializers, no identity
  traps, schema evolution) at a fraction of the cost and risk — no consensus-critical logic was
  transcribed.

**Why the moment was right:** backward state compatibility was a non-issue (fresh-deploy
rollout; v6 was already state-incompatible with v5), and the output contract was pinned
byte-identical by the golden JSON test before the conversion started, so the change is provably
behavior-preserving (pipeline equivalence test expectations unchanged).

**Guard against regression:** KryoFreeModelTest fails the build if any converted type (or a
future field added to one) extracts to GenericTypeInfo again.

**Standing scope note / when to revisit:** only XRP balances is converted. Every other
Flink-2-migrated job still runs its Scala model on Kryo with the None/Some registrations and
the latent Nil hazard (first empty Scala List crossing a boundary). The intended path is to
convert each job's model at its own rerun boundary, XRP-style; if that never happens for a job,
it needs at least the NilSerializer registration. Revisit the Java-model decision itself only
if Flink regains first-class Scala type support upstream — nothing else observed so far argues
the other way.

### Next steps

1. Review + commit the Java-model change on `xrpAsyncState`; then image build.
2. Bump image tag in `hprod/.../xrp/xrp-balances-v6/values.helm.yaml` (current `ade93a08` is a
   known placeholder), redeploy (fresh bootstrap — `make buckets` first if buckets ever
   recreated).
3. After the re-run catches head: output equivalence vs prod `xrp_balances`
   (hash-multiset, as stacks).
4. Parked: autoscaler block for the backfill lifecycle; `job.restart.failed` operator option;
   Kafka tooling unreachable from agent container (separate session planned).

## 2026-09-11 — heap correction buffers + deploy: correction funnel eliminated (~40–80x)

Work now continues on branch `adoptJavaTypes` (continuation of `xrpAsyncState`; contains it).
Committed since the last entry:

- `807f5458` + `d5d02395` — the Java data model (option C above) committed after review;
  `Balance` further converted from mutable POJO to a record.
- `29c50449` — **timestamp correction buffers moved to heap, spilling to state at barriers.**
  The correction funnel (keyBy on a constant key) was the pipeline's throughput ceiling: it
  spent ~1.3 ms/record on the per-record ForSt round trip of the inherited drain buffer
  (put on arrival, get+delete at drain) — measured saturated at 787 records/s on hprod while
  every other subtask idled. Records waiting for their block's correction now sit in a heap
  TreeMap inside a hand-rolled operator (`CorrectBlockTimestampsOperator` replaces
  `CorrectBlockTimestampsProcess`); only records still waiting at a checkpoint barrier are
  written out, as **union operator state** re-claimed on restore by the subtask owning the
  constant key. Steady-state state traffic at the stage is zero; the stage no longer uses
  async state at all. Trade: the alignment envelope (`sourceWatermarkAlignmentBlocks`) is now
  a heap commitment at this operator, and a restore briefly ships every subtask a full buffer
  copy (envelope-bounded to tens of MB).

### Deploy evaluation (hprod, xrp-balances-v6)

Image `29c50449` live since 13:33 UTC. The same morning (07:35–13:15 UTC) ran the pre-commit
build under the same deployment name, and both runs were in backfill catch-up
(throughput-limited), so Prometheus gives a clean A/B:

| Metric | pre-commit (10:00–13:15) | `29c50449` (13:35–13:52) |
|---|---|---|
| `Correct_block_timestamps` output | ~300–800 rec/s (the 787 ceiling) | **30–46k rec/s** |
| correction subtask busy time | pinned at 1000 ms/s | ~130 ms/s, mostly backpressured |
| Kafka sink throughput | ~150–470 rec/s | **~34k rec/s** (~80x) |
| checkpoint duration | 4–7.5 min | **~21 s** |

The checkpoint collapse is the direct signature of the change — per-record ForSt traffic at
the correction stage is gone.

**New bottleneck / the remaining TM CPU skew:** the hottest subtask is now the
`Order_balances → Map → Kafka sink` chain (constant-key subtask, ~885 ms/s busy); everything
upstream is backpressured ~700–830 ms/s waiting on it. Its TM burns ~2.4 cores while the
others sit at ~0.4–0.6 — the skew is structural (parallelism-1 sorted-output contract), not a
scheduling artifact. That stage was only ~160–190 ms/s busy pre-commit; the correction fix
consumed its ~5–6x headroom. It is the next lever (working tree has in-progress edits to
`OrderBalancesProcess` et al., uncommitted).

**Incidental restore-path validation:** one job restart at 13:42 UTC — `taskmanager-1-6`
never started (ECR image-pull i/o timeouts on node `freya`; node-local registry connectivity
issue, worth watching), Flink replaced it with `taskmanager-1-7` and the job recovered at full
rate. First real exercise of the new union-state snapshot/restore path passed.

## 2026-09-14 — Order_balances moved to heap buffers too (`76a73cb1`, awaiting deploy)

Note first: the "in-progress edits to OrderBalancesProcess" mentioned on 09-11 no longer
existed (working tree clean, stash empty; `684237be` only fixed a build warning in that file) —
the fix below was implemented fresh.

The ordering stage was the remaining ceiling (~885 ms/s busy constant-key subtask, upstream
backpressured, its TM at ~2.4 cores vs 0.4–0.6): it still inherited `AsyncBlockDrainProcess`,
i.e. the same per-record ForSt round trip removed from the correction stage by `29c50449`.
Committed as `76a73cb1` on `adoptJavaTypes`:

- **`HeapBlockDrainOperator`** (job/helpers) — the heap-buffer scaffolding extracted from the
  correction operator: TreeMap buffer by block number, drain of complete blocks ascending in
  `processWatermark` before forwarding it, fail-loud no-late check, spill of still-waiting
  records to union-redistributed operator state at barriers, owner-key restore (non-owners shed
  at next snapshot). Counterpart of `AsyncBlockDrainProcess` for the parallelism-1 constant-key
  stages, whose whole buffer fits one subtask's heap.
- **`CorrectBlockTimestampsOperator`** refactored onto the base — persisted state names/layout
  byte-identical to what is deployed; pure structural change.
- **`OrderBalancesOperator`** replaces `OrderBalancesProcess`: `drainBlock` sorts in place by
  `Balance.PRIMARY_KEY_ORDER` and emits with the block number as record timestamp; persists
  `lastForwardedWatermark` (owner-only) so the no-late check stays tight across restores. The
  stage no longer uses async state at all. Heap residency here is a single drain burst — the
  per-address stage releases a block's records only just ahead of the watermark that drains
  them — so barrier-time spill is smaller than at the correction stage.
- Wiring: `keyBy(constant).transform(...)`, **new uid `order-balances-heap`**,
  `enableAsyncState()` dropped at the stage; operator name "Order balances" kept so the
  `Order_balances` metrics stay continuous.
- Tests: pipeline golden expectations untouched; new `OrderBalancesOperatorTest` on the operator
  harness — drain order, snapshot-while-buffered → restore → drain, restored-watermark late
  rejection, fail-loud late path. First harness-level restore coverage for this scaffolding.
  147/147 green.

### Why the running job cannot swap onto this image (analyzed, decided: clean re-deploy)

1. Restore fails by design: the checkpoint's keyed ForSt state under uid `order-balances`
   (block-buffer, drained-watermark, timers) is unclaimed by the new graph.
2. `allowNonRestoredState` would force it but silently drops whatever sat in the ordering
   buffer at the barrier — those blocks' balances never reach Kafka; during backfill the buffer
   is never empty (alignment envelope), so loss is guaranteed.
3. `stop --drain` (flush buffer, then savepoint) poisons the state that IS carried over:
   the correction chain would persist `lastForwardedWatermark = Long.MaxValue` (every record
   after restart trips the no-late check) and the per-address `drained-watermark` would persist
   MAX (burst guard swallows all future timer firings — silent stall). Making drain survivable
   needs migration-only clamping code; not worth it.

Decision: fresh bootstrap with the new image, same playbook as the `29c50449` deploy; if the
current run is near head, finish the output-equivalence check vs prod `xrp_balances` on the old
image first, then the re-run revalidates it on the new one.

### Next steps

1. Image build off `76a73cb1`; bump tag in `hprod/.../xrp/xrp-balances-v6/values.helm.yaml`;
   clean re-deploy (fresh bootstrap — `make buckets` first if buckets ever recreated).
2. After catch-up: output equivalence vs prod `xrp_balances` — **process established and run
   clean against the current (`29c50449`) run, see below**; repeat per
   [qa/qa-output-comparison.md](qa/qa-output-comparison.md). Also re-check the TM CPU
   profile — remaining cost at the ordering subtask is sort + Map + Kafka
   JSON, the irreducible price of the sorted-output contract. If still short, next lever is
   pre-grouping into per-block batches before the constant-key shuffle.
3. Parked (unchanged): autoscaler block for the backfill lifecycle; `job.restart.failed`
   operator option; Kafka tooling unreachable from agent container; `freya` node-local
   registry connectivity.

## 2026-09-14 — output QA: the 29c50449 run matches prod (148/148 months)

The async run's output (`test.xrp_balances_test`, backfill watermark at block 95.8M /
2025-05-01) was compared against prod `xrp_balances` over 2013-01 → 2025-04:
**PASS on every month** — identical key sets, bit-identical non-float columns, float
sums within 1e-9. Full method, reusable sweep/analyzer scripts, and the gotchas
(ReplacingMergeTree dup handling, old table's ULP-level self-inconsistency, monster-IOU
months weakening the float check, the uncleared-first-run duplicates in the test table)
live in [qa/qa-output-comparison.md](qa/qa-output-comparison.md). The upcoming re-run
on the Order_balances heap-buffer image should repeat that runbook once caught up.

## 2026-09-17 — autoscaler rescale wedge took the job down; operator bug identified

The v6 job (deployed Sep-16 09:11 UTC off devops `xrpBalancesForst`) was found down —
FlinkDeployment `CANCELED/UPGRADING`, no JM/TM pods since 04:13 UTC, `RESTOREFAILED`
looping every minute. Not ArgoCD-related (initial hint ruled out: no hprod spoke, no
Application CRD on hprod, trail fully internal to the Flink operator).

**Incident chain (all from operator/JM logs):**
1. Autoscaler + adaptive scheduler rescaled in-place every 1–2 min all night: desired
   per-vertex parallelism (64–128, from jittery backlog estimates) was UNREACHABLE —
   slot ceiling is 18 (`slotmanager.number-of-slots.max`), so effective parallelism
   never moved while `pipeline.jobvertex-parallelism-overrides` flapped ±1 on the
   Parse-balances vertex. `job.autoscaler.stabilization.interval` (2h) does NOT gate
   in-place rescales — it anchors to job restarts, which in-place scaling never causes.
2. 04:12:37 one rescale's REST apply (`updateVertexResources`) ran 18.3s (normal
   ~0.2s; previous rescale still digesting, JM = 1 CPU hard cap) → operator's 10s
   `kubernetes.operator.flink.client.timeout` expired → "Error while rescaling,
   falling back to regular upgrade".
3. Fallback upgrade with `last-state.job-cancel.enabled: true` → suspendMode=CANCEL →
   job cancelled; JM on terminal CANCELED wiped its own ZK HA metadata (by design).
4. Restore then refused forever: "HA metadata is not available to restore from last
   state" — despite `status.jobStatus.upgradeSavepointPath` = chk-455.

**Operator bug (deployed image b40c553 = exactly release 1.13.0; source verified):**
the reconciler decided `JobUpgrade(suspendMode=CANCEL, restoreMode=SAVEPOINT)` and the
async-cancel branch writes `upgradeMode=savepoint` into lastReconciledSpec precisely so
the restore uses the recorded checkpoint without HA metadata. 20s later the restore ran
with `upgradeMode=last-state` anyway → `requireHaMetadata=true` → logs prove the path:
ZK `atLeastOneCheckpoint` probe, "Keeping HA metadata for last-state restore",
"Deploying application cluster requiring last-state from HA metadata" → throw. The
restoreMode=SAVEPOINT decision is lost between cancel and restore (write clobbered or
never patched; exact mechanism not caught statically — every code path reads clean).
NOTE the wedge is PROTECTIVE: the alternative branch (`atLeastOneCheckpoint`=false, no
savepoint opt) was a STATELESS submit — silent empty-state restart. Upstream fixed
adjacent issues post-1.13.0 (FLINK-38077 cancel-only-if-JM-READY, FLINK-39270 status-
savepoint trust under slow JM) but neither covers this path → candidate upstream report,
logs preserved here. Next occurrence: capture `kubectl get flinkdeployment -o yaml`
(check `lastReconciledSpec.job.upgradeMode` vs `jobStatus.upgradeSavepointPath`) +
operator log from "Error while rescaling" on; optionally set logger
`o.a.f.k.o.reconciler` to DEBUG ("Job upgrade available: ...") to make the trace
conclusive.

**Fixes staged on devops `xrpBalancesForst` (chart `flink-job-template`, uncommitted,
awaiting review):** autoscaler-guardrails block, emitted only when a job sets
`job.autoscaler.enabled` — `job.autoscaler.vertex.max-parallelism` derived from the
slot ceiling (job's `slotmanager.number-of-slots.max`, else replicas×slots; renders 18
here) kills the unreachable-target churn; `kubernetes.operator.flink.client.timeout:
"1 min"` makes a slow rescale not count as failed. Decisions: `job-cancel.enabled`
STAYS `"true"` fleet-wide (fresh submission on deploys is wanted; now hasKey-guarded
per-job + residual risk documented in the template comment) — lightweight option chosen
over per-job `upgradeMode: savepoint`; UPGRADING/JM-MISSING Prometheus alert designed
but SKIPPED for now (operator exposes :9999 metrics, nothing scrapes them yet).

**Recovery:** `make redeploy_from RESTORE_PATH=s3://flink-checkpoints-production-xrp-
balances-v6/checkpoints/ha/0717b19886e92ca06cddb866b6b024ca/chk-455` (chk-455 =
04:10:49 UTC, ~6.9 GiB, no discard lines at shutdown; verify `_metadata` via mc first).
Kafka exactly-once txn window: resume within 7 days of Sep-17 04:13 UTC. Plain
`make install`/resume would just re-enter the RESTOREFAILED loop.

## 2026-09-18 — S3A restore livelock diagnosed + fixed; full autoscaler lifecycle validated; scale-down sweep incident (self-healed)

**Second incident (Sep-17 15:53 → Sep-18 morning): restore livelock.** After the chk-455
recovery the guardrails held (zero rescale churn for 4h — previously ~60/h). The first
legitimate rescale (15:53, targets now slot-capped 16/18) applied cleanly in-place, but the
task restart it triggers hit a pre-existing weakness: the ForSt restore/re-open storm
exhausted the S3A pool (256) — `getFileStatus ... Timeout waiting for connection from pool`,
restore fails → global restart → same storm, every ~10 min for hours; compounded by a 6th
16g TM that couldn't schedule (nodes full). KEY SIZING INSIGHT (verified in ForSt source,
`fs/cache/CachedDataInputStream.java`): a pool connection is held by every OPEN remote
stream — ForSt pins its remote S3AInputStream per open file handle — NOT just by active I/O
threads. Cold-restore demand = transfer threads (instances × transfer-thread-num=4 ≈ 60 at
3 slots × 5 stateful ops) + open SST handles (state-layout dependent, low hundreds) × 2
old/new attempt overlap. 256 was exactly marginal; long-lived TM JVMs accumulated stranded
connections over 60+ interrupted attempts (always the 18h-old TM failing). Fleet chart fixes
(committed 4a2b8b6c9): `fs.s3a.connection.maximum` 256→1024 (sizing model in chart comment),
`terminationGracePeriodSeconds` 7200→300 (hung TMs sat 2h in Terminating; healthy Flink
exits in seconds). Recovered via `make redeploy_from` from chk-552; user freed node memory →
job ran at full 18 slots. Also: `make redeploy_from` now auto-discovers
`status.jobStatus.upgradeSavepointPath` on STOPPED jobs (4f95f5a19; refuses on RUNNING —
field is stale there — and on the `KUBERNETES_OPERATOR_LAST_STATE` dummy marker).

**Autoscaler scale-down: 24h→3h** (f85af68e9) — the 24h hold was sized for the restore-storm
era. NOTE: `job.autoscaler.*` keys are operator-side; the operator strips them from the
submitted Flink config, so deploying the change did NOT restart the job, and the running
delayed-scale-down clock (firstTriggerTime 10:35:56Z) survived.

**Third incident (13:39–14:05 UTC, self-healed): exactly-once sink sweep after 18→1 drop.**
Scale-down executed cleanly (TMs released 13:40) but the job sat CREATED / no checkpoints
for 26 min: the surviving sink subtask's KafkaSink init runs the transaction-abort sweep
(INCREMENTING naming: id = prefix-subtask-checkpointId; broker keeps each id's metadata
7 DAYS, so the probe space = union of ALL runs of this job name: 18 old subtasks × counters
to ~1500 × 3 topics), serialized onto ONE subtask at ~10 probes/s. At p=18 the sweep is
parallelized and invisible; the one-step drop to p=1 (scale-down.max-factor 1.0) made it
maximal. Nothing logs progress at INFO; CR shows CREATED; checkpoints abort with "Not all
required tasks are currently running" — looks stuck, is actually grinding. Healed at
14:05:00 the moment the sweep finished; checkpoints metronomic since (chk 722 → 817+,
~3.6 GiB, 2s). Full lifecycle now validated: backfill@18 slots → catch-up → 3h-held
scale-down → single-TM steady state.

**Open mitigations for the sweep class:**
1. `job.autoscaler.vertex.min-parallelism: 3`-ish in job values — keeps future sweeps
   parallel, caps per-subtask id inheritance. Cheap, devops-side.
2. Eliminate the class: flink-connector-kafka 4.x `TransactionNamingStrategy.POOLING`
   (etherbi-flink code change, KafkaSink builder). Reuses a bounded id pool per subtask;
   abort switches from PROBING (the sweep) to LISTING (asks the broker which transactions
   are actually open). Requires Kafka 3.0+ (cp-kafka 7.8.2 ✓), extra read permissions on
   target topics, and a clean migration (checkpoint on connector 4.x INCREMENTING first, or
   any savepoint); switching BACK to INCREMENTING is unsupported. Flink 2.3 jobs are on
   connector 4.x already, so likely a one-line builder change — verify connector version in
   etherbi-flink first.
