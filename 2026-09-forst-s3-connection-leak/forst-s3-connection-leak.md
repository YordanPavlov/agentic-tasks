# ForSt S3 connection leak (Flink 2.3.0)

xrp-stacks-v7 on Hetzner prod stuck in a restart loop since 2026-09-29 17:27: every attempt
fails restoring checkpoint 601 on `xrp-stacks-v7-taskmanager-1-1` (the only TM up 47h) with
`ConnectionPoolTimeoutException: Timeout waiting for connection from pool` in
`ForStResourceContainer.prepareDirectories → exists()`. Root-caused to two upstream Flink bugs.

**Status (2026-09-30):** root cause confirmed from a heap dump. Mitigation (delete TM 1-1) handed
to Yordan. Next session: implement the fix and assess which jobs are affected.

## Root cause

**Bug A — S3 streams never closed (exhausts the pool).**
`org.apache.flink.state.forst.fs.cache.CachedDataInputStream.close()` sets `closed=true` and
closes only the local cache stream (`closeCachedStream()`); it never closes `originalStream`
(the remote S3 stream). Every remote SST file ForSt opens while the file cache is enabled
(our chart's ForSt profile sets `state.backend.forst.cache.size-based-limit: 10g`) keeps one
HTTP connection leased forever. Streams that get GC'd unclosed still hold the lease; the Apache
HttpClient pool doesn't reclaim on GC. `fs.s3a.connection.maximum` (1024) is a TM-wide pool that
survives job restarts, so the leak accumulates until the longest-lived TM starves. Unfixed on
apache/flink `master` (last change to the file: FLINK-37628, 2025-04).

**Bug B — disposed backends pinned forever (memory leak, and it keeps Bug A's streams reachable).**
`AsyncExecutionController.close()` is never called from production code, only from a test.
`AbstractAsyncKeyOrderedStreamOperator.close()/finish()` only drain in-flight records. So each
task attempt's `AsyncRequestBuffer` periodic task on the static, TM-wide
`AsyncRequestBuffer.DELAYER` (`scheduleAtFixedRate`) is never cancelled, and it pins:
`asyncRequestBuffer-timeout-scheduler-thread-1 → DelayedWorkQueue → ScheduledFutureTask →
AsyncRequestBuffer → StateExecutionController → ForStKeyedStateBackend (disposed=true) →
ForStResourceContainer → ForStFlinkFileSystem → FileBasedCache → FileCacheEntry.openedStreams
→ CachedDataInputStream → S3AInputStream`.

Earlier suspicions that turned out **not** needed to explain it: the `ByteBufferReadableFSDataInputStream.readFully`
exception path (it does leak a stream on a failed read, but that's minor), and the `clearDirectories` NPE on the
failed-build path (just log noise). TM 1-35 Pending on `Insufficient memory` is a separate issue that slows restarts.

## Evidence (TM 1-1 vs fresh TM 1-34)

- Sockets to S3 `:443` on TM 1-1: 999 CLOSE_WAIT + 25 ESTABLISHED = 1024. TM 1-34: 0.
- MAT on the heap dump: `CPool` leased 1024 / available 0. 22 `OneInputStreamTask` (all
  `isRunning=false`; a 3-slot TM runs ≤6). 10 `ForStKeyedStateBackend`, all `disposed=true`,
  all reachable only via the DELAYER thread; the DELAYER queue holds 10 tasks.
- 323 `CachedDataInputStream`: all `closed=true`. The 323 `S3AInputStream` under them: all
  `closed=false`, with the wrapped HTTP stream still set.
- Artifacts: `~/src/misc-santiment-issues/xrp-stacks-v7-forst-leak/`, which holds the heap dump
  (`tm11.hprof*`), the MAT install and the query outputs (`q-*`). **The dump can contain
  credentials: delete it once the fix has been validated.** Re-run a query with:
  `mat/mat/ParseHeapDump.sh tm11.hprof "-command=<cmd>" -format=txt -unzip org.eclipse.mat.api:query`
  (the output goes to `tm11_Query/`).

## Next session

1. **Fix Bug A.** In `CachedDataInputStream.close()`, also close `originalStream`. Ship it as a
   `flink-patches` class in etherbi-flink, the same way as `ForStGeneralMultiGetOperation`.
   Validate on stage: the count of `:443` sockets per TM should stay flat across restarts.
   Stopgap if needed: turn the file cache off (`cache.size-based-limit: 0` and a reserve size of 0),
   which costs read performance.
2. **Fix Bug B.** Close the `asyncExecutionController` in `AbstractAsyncKeyOrderedStreamOperator.close()`.
   This is riskier, because every async-state operator shares that code path.
3. **Exposure.** List every job that runs ForSt with the file cache (eth-stacks-v12, xrp-balances-v6,
   ...). Per TM, compare the count of `:443` CLOSE_WAIT sockets against 1024, and plot its growth against TM age and restart count.
4. File both bugs upstream (FLINK JIRA) and link the issue ids here.
