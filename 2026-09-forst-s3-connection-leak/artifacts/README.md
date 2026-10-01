# Artifacts

Kept from the investigation; the raw heap dump (`tm11.hprof`, TM 1-1 of
xrp-stacks-v7, 2026-09-30) was deleted because it can contain credentials.
Everything here was scanned for credentials before it was committed.

## heap-dump-queries/

Eclipse MAT query results against that dump (text exports; the query
strings were not recorded, the column headers describe each result).

| File | Shows |
|---|---|
| `q-pool.txt` | S3A HttpClient `CPool`: 1024 leased / 0 available / max 1024 |
| `q-cds.txt` | all 323 `CachedDataInputStream`: `closed=true` |
| `q-s3closed.txt` | the 323 `S3AInputStream` under them: `closed=false`, wrapped HTTP stream still set |
| `q-saddr.txt` | addresses of those `S3AInputStream`s |
| `q-spath.txt` | GC-root path of one `S3AInputStream` (via `FileCacheEntry.openedStreams`) |
| `q-s3paths.txt` | the 323 S3 streams all reachable only via `asyncRequestBuffer-timeout-scheduler-thread-1` |
| `q-baddr.txt` | the 10 `ForStKeyedStateBackend`s: all `disposed=true` |
| `q-rdb.txt` | their native ForSt DB handles |
| `q-path.txt` | GC-root path of a disposed backend: `StateExecutionController` → `AsyncRequestBuffer` → DELAYER |
| `q-backend.txt` | the 10 backends all reachable only via the DELAYER thread |
| `q-tasks.txt` | 22 `OneInputStreamTask`s on a 3-slot TM, all `isRunning=false` |
| `q-task.txt` | what keeps the dead tasks reachable (DELAYER thread, finalizer) |
| `q-delayer.txt` | queue sizes of the `ScheduledThreadPoolExecutor`s in the dump |
| `tm11.threads.txt` | thread dump from the heap dump |

## tm-1-34-sockets-2026-10-01.log

`:443` socket counts on TM 1-34, sampled every minute, 07:43–07:53 UTC on
2026-10-01, before the redeploy to the patched image: flat at ~880
(~830 CLOSE_WAIT) of the 1024 pool, accumulated over ~18h of restarts.

## repro/

Standalone checks run against the Flink 2.3.0 `flink-dist` jar, stock vs
patched (etherbi-flink `flink-patches/`). Each fails on stock 2.3.0 and
passes on the patched jar.

| File | Checks |
|---|---|
| `LeakCheck.java` | FLINK-40644/40645: `CachedDataInputStream.close()` closes the remote stream and unregisters from `openedStreams`, also when the close throws |
| `ReadFailCheck.java` | FLINK-40644: `ByteBufferReadableFSDataInputStream.readFully()` closes the stream on a failed read |
| `DelayerLeakCheck.java` | Bug B: MiniCluster job (parallelism 2, async state) failed 5 times, then counts `AsyncRequestBuffer.DELAYER` queued tasks. Stock: 12 (hashmap) / 4 (forst); patched: 2 |

To run them, take `lib/` from `apache/flink:2.3.0-java21` (for the patched
case, `lib/flink-dist-2.3.0.jar` from the etherbi-flink image, or the stock jar
with the patched classes `jar uf`-ed in):

```sh
CP="flink-dist-2.3.0.jar:$(ls lib/*.jar | grep -v flink-dist | tr '\n' :)"
javac -proc:none -cp "$CP" -d out *.java
java -cp "out:$CP" LeakCheck
java -cp "out:$CP" ReadFailCheck
java -cp "out:$CP" DelayerLeakCheck hashmap   # or: forst
```
