# XRP balances async state migration

Migrate the XRPBalances Flink job to the async state API (state V2 / ForSt), following the
ETH stacks pattern (PR #306), and clean up the design issues surfaced along the way.

**Status (2026-09-11):** Java model committed; correction-funnel fix `29c50449` (heap buffers,
union state at barriers) **deployed to hprod and evaluated: ~40–80x throughput, checkpoints
7 min → 21 s**. New bottleneck is the `Order_balances` constant-key stage (explains the
remaining TM CPU skew); in-progress edits on branch `adoptJavaTypes`. Devops side (ForSt
profile + xrp-balances-v6) committed on `xrpBalancesForst` through `5c05777a5`.

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
