# Stacks computed natively in ClickHouse (micro-batch SQL + UDF fold)

## At a glance

- **Idea (Yordan, 2026-07-14):** compute the entire stacks output on a
  dedicated ClickHouse instance — MVs, native primitives, glue code — instead
  of the Flink job. Latency budget: **up to 5 minutes** accepted; a hot table
  for fresh data can tighten the inner loop.
- **Status:** evaluated on paper (this doc); spike not started.
- **Verdict of the initial evaluation:** viable — the fold is expressible,
  semantics can be kept exactly (this is *not* the rejected dt-bucketing),
  and state maintenance can be largely declarative. Real risks are merge
  amplification of the state table, ordering/idempotency discipline in glue,
  and unbenchmarked throughput for Solana-class chains.
- **Sibling task:** [2026-07-stacks-working-set-state](../2026-07-stacks-working-set-state/stacks-working-set-state.md)
  — this is **architecture C** next to its A (ForSt cold tier) and B
  (CH hydration). Insight 2 there (stack ≡ net cohort composition,
  reconstructible from own output) is a load-bearing enabler here. The
  step-1 state-composition measurement feeds both tasks (live-cohort count =
  this design's state-table row count).
- **Motivating appeals:** (1) no specialized framework — SQL + Python glue,
  more team members can contribute; (2) easy state inspection — no Flink
  state decoding, state is a queryable table; (3) columnar compression should
  make state disk footprint small.

## Why the computation fits

Stacks is a sequential stateful fold per `(contract, address)` —
non-associative, so window functions / pure full-history SQL are out. But a
**micro-batch loop** works: every N minutes take new transfers, join current
stack state for touched addresses, apply the fold, emit output rows, update
state.

### Enabler 1 — LIFO pop is a prefix-sum

Consuming amount X from a stack = `arrayCumSum` from the top: segments with
cumsum ≤ X are fully consumed, the boundary segment splits. Vectorized; no
iteration. Only sign *alternations* within one batch for one address remain
sequential — handled by the inner fold:

- `arrayFold` (CH ≥ 23.10), accumulator = (stack, outputs); or
- **executable UDF** (recommended): ~200-line pure function
  `(stack_array, ordered_changes) → (new_stack, output_rows)` in Python/Rust.
  Deterministic, unit-testable — **reuse the Scala `HandlerOneAccountChange`
  tests as golden tests**.

### Enabler 2 — semantics preserved: batching computation ≠ bucketing values

Within a batch, each address's changes are applied in
`(block, txIndex, logIndex)` order; `+5 then −5` in one batch still emits
`+1@dt₁, −1@dt₂` with exact `dt`s. The bucketing ADR's rejected
netting/dt-bucketing (silently removes 5–15% of consumed volume) does **not**
apply. Output rows can be byte-compatible `AccountModelChange`s →
**shadow-run both pipelines and diff with the existing XRP harness**
(`../2026-06-odt-bucketing-xrp/compare_xrp_experimental.py` methodology).

### Enabler 3 — state can be a materialized view over the output

From sibling-task Insight 2: live stack ≡
`sum(sign·amount) GROUP BY (contract, address, odt) HAVING sum ≠ 0`, ordered
by `odt`. So the state table can be a **SummingMergeTree MV over the output
table**, `ORDER BY (contract, address, odt)` — state updates declaratively on
output insert. This shrinks the no-multi-statement-transactions problem to
"insert one deterministic output batch idempotently" (batch versioning /
insert-block dedup). Fallback if MV dedup proves fragile: explicit versioned
state table, same schema.

State inspection becomes `SELECT` — the entire class of pain from the
bucketing campaign (sampling opaque RocksDB state via 1% proxies) disappears.

## Latency & throughput sketch (to benchmark, not assert)

- Per iteration: batch transfer read (already in CH) → primary-index range
  read of touched addresses' state (CH-friendly shape; not point lookups) →
  fold → insert. ETH+ERC20 ≈ 10⁵–10⁶ changes / 5 min through the UDF —
  expected well under a minute; loop cadence can drop toward ~1 min with the
  hot-table split.
- **Catch-up/backfill inverts to a strength**: day-sized batches at full CH
  parallelism vs replaying history through a streaming topology.
- Solana-class throughput is the open benchmark, not a known blocker — the
  fold parallelizes perfectly by address.

## Honest counterpoints / risks

1. **LSM on an LSM.** The state table has merge amplification too — dormant
   rows get rewritten by background merges, same physics as RocksDB
   compaction, though columnar, compressed, observable, tunable (partitioning
   / hot-cold split by address are available levers). "CH makes state cheap"
   must be measured.
2. **Ordering/reorg/idempotency discipline moves into glue.** Watermarks,
   late-data policy, deterministic re-runs — Airflow-style discipline the
   team practices, but now load-bearing for a *stateful* computation.
3. **Appeal #1 is partial** — Flink stays for transfers/balances unless they
   migrate too. Stacks is however the state-heavy job where Flink expertise
   is hardest.
4. **Sub-5-min consumers**: the seam is 5-min-truncated everywhere observed,
   but confirm as a product statement.
5. **Nonce compatibility**: fold must mint deterministic nonces (per-address
   counter in state) to keep output byte-compatible during shadow runs;
   longer-term the sibling task's lever C1 (drop nonce) applies here too.

## Bonus: unification potential

The UTXO/FIFO sibling (`ComputeUTXOAccountSegmentChanges`, excluded from
bucketing) fits the same framework — FIFO pop is the same prefix-sum from the
other end of the array. One CH framework could cover both account and UTXO
models with two small fold functions.

## Spike plan

1. **Fold prototype**: executable UDF (Python first) + the orchestrating SQL;
   golden-test against the Scala handler test vectors.
2. **XRP end-to-end shadow**: run the loop over XRP history on a dev CH
   instance; diff output vs the validated baseline
   (bucketing-campaign harness). Measure: loop latency per batch, catch-up
   throughput, state table size (vs the sibling task's RocksDB measurement),
   merge amplification under steady state.
3. **Idempotency/re-run drill**: kill/rerun batches, verify byte-identical
   output and state (decides MV-derived vs explicit versioned state).
4. **Throughput ceiling test**: synthetic Solana-scale batch through the
   fold path.
5. **Decision point vs architectures A/B** in the sibling task — criteria:
   state footprint, ops complexity, latency, team contribution surface.

## Fresh-session bootstrap

What a new session needs beyond this doc.

**Ground truth to read first** (all in `~/santiment/src/etherbi-flink`):

- Fold semantics: `src/main/scala/net/santiment/job/helpers/HandlerOneAccountChange.scala`
  (push/pop/remainder/liability/odt-bucket-merge — the function the UDF must
  replicate) and `docs/concepts/stacks.md` +
  `docs/decisions/configurable-odt-bucketing.md` (invariants, closed doors).
- **Golden test vectors**:
  `src/test/scala/net/santiment/job/helpers/HandlerOneAccountChangeTest.scala`
  and `src/test/scala/net/santiment/job/ComputeAccountStackChangesTimeWindowTest.scala`
  — port these cases to the UDF test suite before writing the UDF.
- Output row shape: `AccountModelChange` in
  `src/main/scala/net/santiment/package.scala`; raw CH landing table is the
  per-chain `*_stacks` table (schema via the clickhouse skill).

**Environment facts (verified in this container, 2026-07-14):**

- `clickhouse` / `clickhouse-local` **26.6.1** binaries are installed;
  `arrayFold` confirmed working. No docker. So the spike runs against a
  **local clickhouse server** started in the container (needed over
  `clickhouse-local` for MVs + executable UDFs, which require server config:
  `user_defined_executable_functions` XML + scripts dir).
- Prod ClickHouse is **read-only** via the wrapper (santiment-clickhouse-query
  skill): use it for schemas, the XRP baseline (`xrp_stacks`), and pulling
  sample input slices; never write there.

**Input/baseline data (to resolve at session start):**

- Confirm the XRP raw-input table in CH and that it carries the ordering
  fields the fold needs (block, tx position, internal position — the Flink
  job's primary-key pair). If only Kafka has them, pull a bounded slice via
  the santiment-kafka-source-search skill into local CH.
- Baseline for diffing: prod `xrp_stacks` + the comparison methodology in
  `../2026-06-odt-bucketing-xrp/compare_xrp_experimental.py`.

**Decisions needed from Yordan at session start:**

1. Where the code lives (new repo vs `clickhouse-tables` vs task-dir scripts
   until it stabilizes).
2. Whether a writable dev CH server exists to use instead of / after the
   in-container local server (needed anyway for the full-history XRP shadow —
   local disk in the container won't hold full XRP history).
3. Scope confirmation: XRP first, odt bucketing ON (5-min) in the fold from
   day one, or replicate unbucketed behavior first and add bucketing second
   (recommended: unbucketed first — matches the golden tests, then flip the
   knob and diff both, mirroring the Flink validation sequence).

**Suggested starting directory:** `~/santiment/src/etherbi-flink` (ground
truth + tests at hand); the global instructions make any session scan
`agentic-tasks/INDEX.md` and find this doc.

## Session log

### 2026-07-14 — idea raised and evaluated on paper

Raised by Yordan during the working-set-state discussion (triggered by the
CH-hydration idea). Evaluation above distilled from that session: expressibility
solved via prefix-sum + inner fold UDF; semantics guardrails identified
(intra-batch ordering preserved — not the rejected netting); MV-derived state
option found; risk list drawn. No code yet.
