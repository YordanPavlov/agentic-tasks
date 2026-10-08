# ForSt S3 connection leak (Flink 2.3.0)

xrp-stacks-v7 on Hetzner prod stuck in a restart loop since 2026-09-29 17:27: every attempt
failed restoring checkpoint 601 on `xrp-stacks-v7-taskmanager-1-1` (the only TM up 47h) with
`ConnectionPoolTimeoutException: Timeout waiting for connection from pool` in
`ForStResourceContainer.prepareDirectories → exists()`. Root-caused to upstream Flink bugs.

**Status (2026-10-01):** all bugs fixed as `flink-patches` in etherbi-flink and deployed to
xrp-stacks-v7. Waiting to see it through a restart/rescale before calling it validated.

- Branch `forstStreamLeaks` (`59d93bcd` S3 stream fixes, `329e7803` Bug B), based on master,
  merged into `xrpStacksAsyncState` (`ca429d16`). No PR to master yet.
- **The xrp-stacks-v7 deploy runs from `xrpStacksAsyncState`, not `xrpAsyncState`.** The latter
  is an old branch already in master whose `XRPStacks` doesn't select ForSt; an image built from
  it would fail to restore the ForSt checkpoint.
- Redeployed 2026-10-01 08:00 UTC on `etherbi-flink:ca429d16…` (last-state upgrade, restored
  `chk-1012`). Verified with `javap` on a TM that all patched classes are in flink-dist. At
  +13 min: RUNNING/STABLE, checkpoints 1013–1017 completed, no exceptions, **0 `:443` sockets
  on all 3 TMs**.
- Just before the redeploy, TM 1-34 (up 18h, the replacement for TM 1-1) already held ~880 S3
  sockets (~830 CLOSE_WAIT) of the 1024 pool. The count was flat in steady state: the leak
  comes in bursts on cold-cache restores (restarts, rescales), roughly ~100+ connections each
  (low confidence).

## Root cause

**Bug A — S3 streams never closed (exhausts the pool).** Upstream
[FLINK-40644](https://issues.apache.org/jira/browse/FLINK-40644), PR
[#29168](https://github.com/apache/flink/pull/29168), merged to master 2026-10-06 (fix version 2.4.0).
`CachedDataInputStream.close()` closes only the local cache stream, never `originalStream`
(the remote S3 stream), so every S3 stream read through the ForSt file cache keeps an HTTP
connection leased forever. `fs.s3a.connection.maximum` (1024) is a TM-wide pool that survives
job restarts, so the leak accumulates on long-lived TMs. The PR also closes the stream when
`ByteBufferReadableFSDataInputStream.readFully()` fails.

**Bug A2 — closed streams pile up on the heap.** Upstream
[FLINK-40645](https://issues.apache.org/jira/browse/FLINK-40645), PR
[#29169](https://github.com/apache/flink/pull/29169), merged to master 2026-10-07 (fix version 2.4.0). Closed
`CachedDataInputStream`s stay in `FileCacheEntry.openedStreams` until the file is evicted. Our
heap dump's retention chain goes through it.

**Bug B — disposed backends pinned forever (heap leak per ended task attempt).** No upstream
JIRA found. Nothing calls `AsyncExecutionController.close()`, so each ended attempt leaves its
`AsyncRequestBuffer` timeout task on the static, TM-wide `AsyncRequestBuffer.DELAYER`, pinning
`StateExecutionController → disposed ForStKeyedStateBackend → …` until the TM exits. It's
independent of S3 and the file cache: any async-state (State V2) operator is affected. Fix:
close the controller in `close()` of `AbstractAsyncKeyOrderedStreamOperator` and
`AbstractAsyncStateStreamOperatorV2`. Reproduced on a MiniCluster: after 6 attempts, stock
2.3.0 left 12 DELAYER tasks (hashmap backend; 4 on ForSt), patched left 2 (the live ones). It
was not needed to explain the connection leak (the pool doesn't reclaim on GC), only kept the
leaked streams visible in the dump.

Our patched code behaves the same as the two merged upstream PRs applied to release-2.3.0
(comments differ; upstream's A2 goes through a new `FileCacheEntry.unregisterStream()` helper).
Neither is backported to `release-2.3` (2026-10-08), so the patches stay until the bump to 2.4.0.
The Bug B files are still unchanged on apache/flink master. Details and
safety arguments are in etherbi-flink `flink-patches/README.md`.

## Exposure and conditions

- **Bug A affects any ForSt job with remote state and the file cache on, which are both
  defaults:** `state.backend.forst.primary-dir` defaults to the checkpoint dir, and
  `cache.reserve-size` defaults to 256 MB, which enables the cache even without our chart's
  `size-based-limit: 10g`.
- **Config bypass:** set `cache.size-based-limit: 0` and `cache.reserve-size: 0` (all reads
  then go to S3), or `primary-dir: local` (gives up disaggregated state). A bigger pool only
  delays it.
- **The earlier pool exhaustions were most likely this bug:** eth-stacks-v12 (2026-08-04, pool
  96) and xrp-balances-v6 (2026-09-17, pool 256). The chart's `fs.s3a.connection.maximum`
  sizing comment ("open streams pin a connection") is a misdiagnosis.
- **Bug B stopgap:** `execution.async-state.active-buffer-timeout: 0` schedules no timeout task.
  The cost is that buffered requests wait for the next record, watermark or checkpoint when
  input goes quiet.
- **ForSt jobs on Hetzner prod:** xrp-stacks-v7 (patched) and xrp-balances-v6 (image
  `77f79b53`, unpatched, same exposure). Hetzner stage has no ForSt or async-state job.

## Evidence (heap dump of TM 1-1, 2026-09-30)

- Sockets to S3 `:443` on TM 1-1: 999 CLOSE_WAIT + 25 ESTABLISHED = 1024.
- `CPool` leased 1024 / available 0. 22 `OneInputStreamTask` (all `isRunning=false`; a 3-slot
  TM runs ≤6). 10 `ForStKeyedStateBackend`, all `disposed=true`, reachable only via the DELAYER
  thread.
- 323 `CachedDataInputStream`, all `closed=true`, over 323 `S3AInputStream`, all
  `closed=false` with the HTTP stream still set.
- Query results, the thread dump, socket samples and the repro programs are in `artifacts/`
  (see `artifacts/README.md`). The raw dump, MAT and the upstream source copies in
  `~/src/misc-santiment-issues/xrp-stacks-v7-forst-leak/` are no longer needed and can be
  deleted. The retained heap per pinned backend was never measured; that's only possible
  before the dump is deleted.

## Next

1. **Validate on xrp-stacks-v7 after the next restart or autoscaler rescale.** Check that
   per-TM `:443` sockets stay near 0 (count states in `/proc/net/tcp`), the DELAYER/AEC count
   matches live tasks, and the heap doesn't grow per restart.
2. **Patch xrp-balances-v6:** it needs an image with `flink-patches` on top of its code
   (`77f79b53` or newer).
3. **PR `forstStreamLeaks` to master,** so all jobs on new images get the patches.
4. **Upstream:** after validation, review/comment on PRs #29168 and #29169 with our production
   evidence (draft the text first). File a JIRA for Bug B with the MiniCluster repro
   (`artifacts/repro/DelayerLeakCheck.java`).
5. **Chart:** fix the `fs.s3a.connection.maximum` sizing comment; consider lowering the pool
   once validated.
6. Separate issue: TM 1-35 Pending on `Insufficient memory` slows restarts.
