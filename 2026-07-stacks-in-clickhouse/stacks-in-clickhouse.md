# Stacks computed natively in ClickHouse (micro-batch SQL + UDF fold)

## At a glance

- **Idea (Yordan, 2026-07-14):** compute the entire stacks output on a
  dedicated ClickHouse instance — MVs, native primitives, glue code — instead
  of the Flink job. Latency budget: **up to 5 minutes** accepted; a hot table
  for fresh data can tighten the inner loop.
- **Status:** spike iteration 1 **PASSED** (2026-07-14): Python fold replica
  validated byte-exact vs the Flink baseline on a 10-address ETH focus group
  (1338/1338 rows, genesis→2016-03), committed as `d1ce81a9` on branch
  `clickhouseStacks` in etherbi-flink (`clickhouse-stacks/`, incl.
  `doc/architecture.md`). Next: iteration 2 = full-chain shadow against
  **prod** (Yordan: prod eth_stacks should be clean; move there for the next
  comparison). See "Fresh-session bootstrap" below.
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

## Fresh-session bootstrap (updated after iteration 1, 2026-07-14)

Everything a brand-new session needs. The XRP-oriented bootstrap this section
used to hold is obsolete — scope changed to **ETH** and iteration 1 is done.

**Read first, in this order:**

1. `~/santiment/src/etherbi-flink/clickhouse-stacks/doc/architecture.md` —
   the authoritative description of what exists: replicated Flink semantics
   (filters → dedup → compression → fold, step by step with Scala source
   pointers), table design (`ver = block*2 + deleted`, state ≡ output
   re-keyed), validation methodology, stage-vs-prod caveat, known
   limitations. Written for exactly this bootstrap purpose.
2. This journal's session log (below) for the discoveries and their evidence.

**Where the work lives:**

- Repo `etherbi-flink`, branch **`clickhouseStacks`** (created by Yordan off
  master), directory `clickhouse-stacks/`. Commit `d1ce81a9`, **not pushed**
  as of session end — check `git log origin/clickhouseStacks` before assuming
  remote state.
- Code: `stack_fold.py` (pure fold; `python3 test_stack_fold.py` must pass
  14/14 before any change lands), `focus_run.py` (iteration-1 driver;
  `--skip-insert` reruns fold+compare without writing), `chq.sh` (retrying
  clickhouse-client wrapper — use it, the stage LB resets connections
  constantly), `kafka_block_probe.py` (forensic only).
- Stage test tables: 8 tables `*_test_ypavlov*` — full ledger + cleanup DDL
  in `clickhouse-stacks/TABLES.md`. They contain the iteration-1 results
  (1338 output rows, 10 addresses). Constant:
  `assetRefId = cityHash64('ETH_ETH') = 14259145649589866191`.

**Environment facts (verified in this container 2026-07-14):**

- Stage CH `clickhouse.stage.san:30900` is **writable as user `default`**
  (the readonly wrapper only intercepts `*.production.san`); DDL must be
  `ON CLUSTER default_cluster` + Distributed wrappers (LB rotates across
  clickhouse-0/1/2). Python: `clickhouse-driver` is installed and is what
  `focus_run.py` uses; `kafka-python`+lz4/snappy/zstd installed via
  `pip --break-system-packages` (no /opt/kafka CLI in the container).
- Prod CH: read-only via wrapper. The sandbox permission classifier **blocks
  prod queries unless Yordan explicitly directed the prod read in his own
  words** — get that go-ahead before iteration 2 work (he has already said
  "we would move to prod for data comparison on next step", but each session
  should confirm scope).

**Iteration 2 plan (agreed direction):**

1. Sanity-check prod baseline first: prod `eth_stacks` block 55260 must show
   the block reward surviving (mining_block rows + 5 ETH in the miner
   inflow) — prod transfers are post-fix (verified), and Yordan expects prod
   stacks to be clean; this confirms it.
2. Full-chain shadow: read **prod** `eth_transfers` (post-#227 → positions
   unique → `SELECT DISTINCT` dedup is fully deterministic, no oracle, no
   clean-address restriction), fold from genesis, write to the stage
   `_test_ypavlov` tables, compare against **prod** `eth_stacks`.
3. Compare via per-block digests (count + xor/sum of `cityHash64` over
   (address, sign, nonce, odt, amount, coalesce(txID,''))) on both sides,
   then drill only into mismatching blocks — avoids giant joins.
4. Needed build-out for that scale (see architecture.md "Known limitations"):
   micro-batch driver with CH state read-back + resume (dt-window batches —
   blocks never span a dt second; note `eth_transfers` sort key
   `(from, type, to, dt, ...)` means dt-window reads scan month partitions),
   driver-side state cache, lazy top-K stack reads for whales
   (`0x0000…` gains one zero-value EOB segment per block; `mining_block` is
   a liability chain). Start genesis→2016-03 on prod (6.4M rows there too),
   then extend; server-side fold (executable UDF / arrayFold) is a later
   optimization, not needed for validation.

**Standing rules:** new stage tables must carry `test` + `ypavlov` in the
name and be recorded in `TABLES.md`; stage data has anomalies (duplicates,
staleness) — treat surprises as possible data artifacts before suspecting
the fold; never write to prod.

## Session log

### 2026-07-14 (session 2) — spike started: ETH on stage CH; Flink-dedup nondeterminism discovered

Yordan's decisions: code in `etherbi-flink/clickhouse-stacks/` (new repo later);
writable cluster = **stage** (`clickhouse.stage.san:30900`, user `default`; the
readonly wrapper only guards `*.production.san`); scope switched **XRP → ETH**
(consume `eth_transfers`, diff vs `eth_stacks`). Test tables must carry
`test` + `ypavlov` in the name; ledger in `clickhouse-stacks/TABLES.md`.

**Recon findings (all verified against stage):**

- `eth_stacks` stage is **stale** (max dt 2026-05-21); eth_transfers is live.
  Inflows are **unbucketed** (`odt == dt` for fresh pushes) → replicate
  `stacksOdtBucketMs = 0`.
- Amounts land in CH as **raw wei parsed into Float64** (Jackson writes the
  BigInt verbatim; `eth_stacks_mv_v2` maps ts/1000→dt, ots/1000→odt,
  `assetRefId = cityHash64('ETH_'||contractAddress)`).
- Input mapping (ETHTransfersSource → ETHAccountChanges): pre-dedup filter
  (self-transfers dropped unless EOB w/ block>0||ts>0), **dedup keep-first per
  (block, txPos, intTxPos)** in *Kafka arrival order*, post-filter (APPROVE,
  block==0&&ts==0), flatMap → (from,−amt),(to,+amt), window = 1 block,
  per-(contract,address) sort by (txPos,intPos) + same-sign-run compression
  (merged row keeps **last** run element's txID), then the stack fold.
- **Flink ETH baseline is nondeterministic.** Block reward is always at
  position (0,0), colliding with tx0's row in every block with ≥1 tx; second
  uncle sits at (0,1). The topic (`eth_transfers_v3`, old stage Kafka,
  8 partitions) spreads one block's records across partitions (EOB p4, block
  reward p5, uncle p6 for block 45429), so keep-first is a network race.
  Measured on Aug-2015: uncle-vs-reward kept/dropped = 4937/3790; tx-vs-reward
  = 12985/16229 (~coin-flip). ⇒ `eth_stacks` cannot be reproduced byte-exactly
  by any deterministic reimplementation — nor by re-running Flink. Validation
  therefore uses an **oracle-guided dedup**: read per-block winners back from
  `eth_stacks` (`mining_block` / `mining_uncle` row presence; liability deltas
  for the 3.5k two-uncle blocks), taint+exclude the unresolvable remainder.
- All content-differing dedup collisions involve reward rows (count of
  non-reward collisions pre-2016-03: **0**); genesis (block 0) all 8893
  allocations share (0,0) — exactly one survived in the baseline
  (`0x000d836201…`, lowest address). Stage `eth_transfers` also holds literal
  duplicate rows (identical content) — collapse before processing.
- Volume pre-2016-03: 6.4M transfer rows / 1.08M blocks — Python-fold friendly.
- Env notes: stage LB rotates brokers (clickhouse-0/2) and resets connections
  frequently → retry wrapper `chq.sh`; test tables must be ON CLUSTER
  `default_cluster` (+ Distributed). No /opt/kafka CLI in container —
  installed `kafka-python` (+lz4/snappy/zstd) instead; probe:
  `clickhouse-stacks/kafka_block_probe.py`.

**Progress (session 2, checkpoint at user brief):**

1. `clickhouse-stacks/stack_fold.py` — exact-int Python replica of
   `HandlerOneAccountChange` + `groupAndCompress` (incl. optional odt
   bucketing); `test_stack_fold.py` ports the Scala golden tests + edge cases
   (liability chain, remainder, zero-amount EOB, sign-alternation compression)
   — **14/14 pass**.
2. Stage test tables created (see `clickhouse-stacks/TABLES.md` for the
   cleanup ledger): output / per-segment state / meta / batches, each as
   `*_shard` + Distributed wrapper, all named `*_test_ypavlov*`.
   Key design: state rows are exactly the output rows re-keyed
   ((contract,address,nonce) → value String exact, deleted flag,
   ver=block*2+deleted) — per-segment version of Insight 2; meta keeps
   (lastNonce, stackSize).
3. **Focus-group validation PASSED (Yordan's cut for iteration 1):** 10 "clean"
   addresses (never party to a dedup race: no reward records, never from/to at
   positions (0,0)/(0,1); selected by activity from genesis→2016-03), full
   history folded from genesis by `focus_run.py` → **1338/1338 output rows
   byte-exact** vs `eth_stacks` (sign, nonce, dt, odt, Float64 amount, txID all
   equal; zero extra/missing rows either side). Path coverage: 575 fresh
   pushes, 644 pops, 119 remainder re-pushes, 666 blocks; liability path not
   reachable for clean addresses (covered by unit tests). Results also written
   to the `_test_ypavlov` output/state/meta tables (state row = output row
   re-keyed, ver = block*2+deleted — worked as designed).
4. Nondeterminism evidence packaged as two CH queries (collision listing +
   coin-flip classifier); day 2015-08-08 alone: 703 blocks lost the block
   reward, 594 kept it.
   **RESOLVED — stale stage data, not a live bug.** Yordan suspected the
   exporter had already fixed it; confirmed in `san-chain-exporter`:
   - PR #212 (`ff4a1d0`, 2024-12-10) introduced
     `assignInternalTransactionPosition` (key incl. from/to — rewards still
     collided);
   - PR #226 (`9889234`, 2025-04-25) full ordering within a tx (key
     block-txHash — rewards still their own group at intPos 0);
   - **PR #227 (`eea50b3`, 2025-05-14) is THE fix**: assignment key →
     `block-txPos`, chosen explicitly "based on what Flink would deduplicate";
     its unit test is literally mining_uncle vs a tx at the same position.
   Verified on prod (read-only, block 55260): reward (0,0), fee (0,1), all
   positions unique, no duplicate rows → prod-shaped input has NO dedup
   ambiguity. Stage's `eth_transfers_v3` topic / `eth_transfers` /
   `eth_stacks` predate the fix and were never wiped — the races I measured
   are fossils of the pre-#227 exporter. No escalation needed.
   Open question for Yordan: was prod `eth_stacks` recomputed from the
   re-exported (post-#227) topic? If yes, a **full-chain byte-exact shadow
   against prod** (read prod transfers+stacks read-only, write to stage test
   tables) becomes possible with zero oracle — deterministic dedup by
   construction. (Prod-read attempt for that check was blocked pending
   explicit user direction.)
5. Not yet written: full-chain micro-batch driver (dt-window batches, dict
   state + CH read-through), digest-based compare (per-block count+xor-hash
   triage → drilldown).
6. **Session wrap-up:** work committed as `d1ce81a9` on Yordan's new branch
   `clickhouseStacks` (not pushed); `doc/architecture.md` added on his
   request as the reference for humans/agents and for his review of the
   initial code. Yordan: prod `eth_stacks` should be clean → **iteration 2 =
   full-chain comparison against prod** (deterministic post-#227 input makes
   the oracle idea unnecessary there; it stays shelved for stage-only work).
   Bootstrap section above rewritten for the new state.

### 2026-07-14 — idea raised and evaluated on paper

Raised by Yordan during the working-set-state discussion (triggered by the
CH-hydration idea). Evaluation above distilled from that session: expressibility
solved via prefix-sum + inner fold UDF; semantics guardrails identified
(intra-batch ordering preserved — not the rejected netting); MV-derived state
option found; risk list drawn. No code yet.
