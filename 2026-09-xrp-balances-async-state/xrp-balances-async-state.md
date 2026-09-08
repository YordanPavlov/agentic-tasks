# XRP balances async state migration

Migrate the XRPBalances Flink job to the async state API (state V2 / ForSt), following the
ETH stacks pattern (PR #306), and clean up the design issues surfaced along the way.

**Status (2026-09-08):** branch `xrpAsyncState` in etherbi-flink pushed through `75c7832b`,
all 140 unit tests green. **PR not opened yet** — next step is the code review, then draft
the PR description covering the seven commits.

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
