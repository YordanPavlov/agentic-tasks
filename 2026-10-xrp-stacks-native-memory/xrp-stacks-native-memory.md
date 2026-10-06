# xrp-stacks-v7 native memory growth (Hetzner prod)

xrp-stacks-v7 TaskManager RSS grows linearly by ~1.9–2.4 GiB/day per TM, with no plateau seen
over the ~38h observed. It is the only job in the cluster doing so: every other TM grew
≤0.65 GiB/day, and xrp-balances-v6 (same backend, ForSt + async state) is flat. If this is a
leak, the 32G pod limit is ~1 week away.

**Status (2026-10-06):** experiment deployed to tell block-cache fill from a native leak.
**Revisit on 2026-10-07 (≥24h after 09:36 UTC).**

## Experiment (deployed 2026-10-06 09:36 UTC)

devops `hprod/k8s-apps/flink-jobs-operator/xrp/xrp-stacks-v7/values.helm.yaml`, still
**uncommitted** in devops:

- `taskmanager.memory.managed.size: "1536m"`, was ~5.5G. That makes the block cache ~0.5G per
  slot instead of 1.65G, so it fills within hours. Pod stays `16000m`, limit 32G
  (`limit-factor: 2`).
- ForSt metrics: `block-cache-usage`, `block-cache-pinned-usage`, `cur-size-all-mem-tables`,
  `estimate-table-readers-mem`. These are cheap to keep: one in-memory property read per
  column family every 5 s.
- `replicas: 3 → 1` (an unrelated change made in the same deploy) means `parallelism.default`
  is now 3 on one TM, instead of 9 on 3 TMs. The autoscaler may scale it back up. Absolute
  RSS is therefore not comparable with the pre-change numbers; compare the slope only.

**Reading the result:** expected RSS ceiling ≈ 5.6G heap + 1.1G direct + 0.2G non-heap +
1.5G cache + memtables/jemalloc slack, so **~9–10G**.

- RSS plateaus there, with `block-cache-usage` flat at its cap: it was cache fill. Restore the
  managed size, or keep it smaller if checkpoints and backpressure looked fine.
- RSS keeps climbing at a steady rate while the cache is flat: it is a native leak. Next step
  is jemalloc heap profiling (`jeprof` is already in the image, as done for eth-stacks-v12):
  `MALLOC_CONF=prof:true,lg_prof_interval:30,prof_prefix:/tmp/jeprof/jeprof` on the TMs, then
  diff dumps a few hours apart.

Also watch checkpoint duration and backpressure, since the smaller cache means more S3 reads.

## What was ruled out (2026-10-06)

- **Our flink-dist patches (high confidence).** All 5 patched classes differ from upstream
  `release-2.3.0` only by added `close()` calls or try-with-resources; nothing allocates native
  memory. The only native-touching patch (ReadOptions) is also on xrp-balances-v6, which is
  flat.
- **JVM-visible memory.** Heap (Xmx 5.6G, ~150 MB live), direct (~1.1G, the network buffers)
  and metaspace are flat. smaps shows the growth in anon memory outside the Java heap.
- **ForSt native object lifecycle.** The async write batch, iterator and map-check paths close
  their native objects; multiGet is patched. On the disposed backends, the DB, LRUCache,
  WriteBufferManager, DBOptions and ColumnFamilyOptions are closed.
- **Table readers.** `cache_index_and_filter_blocks: 1` with a partitioned index and no filter,
  so index blocks are charged to the block cache.
- **glibc fragmentation.** jemalloc is `LD_PRELOAD`ed on the TMs.
- **Stacks-specific code.** Only MapState get/put/remove and an lz4 Kafka sink (the same sink
  creator as balances); nothing native of our own.

## Open points

- **Leading hypothesis (medium-low confidence): block cache filling slowly.** The cache is
  1.65 GB per slot, uncapped (`strict_capacity_limit: 0`), against ~3.7 GB of state per
  subtask (33.5 GB total). TM-2-2 and TM-2-3 were still under the 3 × 1.65G ceiling, and their
  slope eased from ~0.1 to ~0.065 GiB/h.
- **TM-2-1 is unexplained.** Its native memory was ~8G against the ~4.9G cache ceiling. It is
  the TM that ran the p=3 attempt before the 18:10 rescale on 2026-10-04, but those backends'
  native resources were verified closed.
- **No ForSt stats in the LOG** (`stats_dump_period_sec: 0`); the metrics above replace them.

## Side findings

- **The forst-s3-connection-leak patches are validated in prod** (see
  `2026-09-forst-s3-connection-leak`). The p=3 → p=9 rescale on 2026-10-04 disposed 3 backends
  on TM-2-1, and none was held by the DELAYER. All 790 open `CachedDataInputStream`s belonged
  to live backends, and the leased S3 connections (~330/1024) were SST readers that are open
  but idle.
- **New, bounded heap retention:** the Avro-generated `AccountStackHead.READER$` and
  `StorageSegments.READER$` keep a `SpecificDatumReader.creator` → the old task thread → `Task`
  → the disposed backend. That's at most one task per Avro class, heap only (native already
  closed). Not worth fixing.
- **xrp-balances-v6 is still on image `77f79b53` (unpatched)** and shows the S3 leak signature:
  857 `CachedDataInputStream` closed over 1147 open `S3AInputStream`, and 143 leased
  connections all in CLOSE_WAIT with unread data. Its TM has been up since 2026-10-04 18:04.
  Needs a redeploy on `ca429d16` or later.

## Method notes

- Exec into a TM (pid 1): `jcmd 1 GC.class_histogram`, `jcmd 1 GC.heap_dump -gz=1` (live heap
  is ~150 MB, so dumps are ~55 MB and take <1 s), `/proc/1/smaps` (heap range vs other anon),
  `/proc/net/tcp*` (:443 = `01BB`, CLOSE_WAIT = `08`, rx_queue >64B = unread body vs TLS
  close_notify).
- Memory history: Prometheus at `monitoring/prometheus-operated:9090` via
  `kubectl get --raw /api/v1/namespaces/monitoring/services/prometheus-operated:9090/proxy/api/v1/query_range?...`,
  `container_memory_rss` (about 2 days of retention).
- ForSt LOG: the largest UUID-named file under
  `/tmp/tm_*/tmp/<job>/op_*/db/`. The 10g file cache is **per operator**, so ~30G of local disk
  per 3-slot TM is expected.
