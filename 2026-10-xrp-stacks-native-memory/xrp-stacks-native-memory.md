# xrp-stacks-v7 native memory (Hetzner prod)

xrp-stacks-v7 TaskManager RSS grew by ~1.9–2.4 GiB/day per TM (2026-10-04/06), the only job in
the cluster doing so; xrp-balances-v6 on the same backend (ForSt + async state) was flat.

**Status (2026-10-07):**

- **The steady daily growth was the block cache filling (high confidence), not a leak.**
- **A real native leak was found: every job restart on a surviving TM leaks about the block
  cache content of the backends it disposes (high confidence it accumulates; medium-high that
  it's cache content).**
- **Root cause found and reproduced (high confidence):** ForSt's rescale restore makes a native
  `DBOptions` copy per temporary DB and never closes it. The copy holds the slot-shared
  `WriteBufferManager`, which holds the shared block cache, so each scale-in restore pins the
  restored backend's whole cache past its disposal (section 4). Unfixed on `apache/flink`
  master (2026-10-07). A 3-line patch fixes it in a local MiniCluster repro. Next: add it to
  etherbi-flink `flink-patches`, file upstream.
- **xrp-stacks-v7 currently runs the test config** (unmanaged ForSt memory, jemalloc profiling,
  NMT, `slots: 4`) and needs reverting after the investigation.

## 1. Steady growth = block cache filling

The cache was 1.65 GB per slot (`strict_capacity_limit: 0`) against ~3.7 GB of state per subtask,
so it filled slowly toward 3 × 1.65G per TM. After cutting `taskmanager.memory.managed.size` to
1536m (~0.5G cache per slot, deployed 2026-10-06 09:36), native memory went flat for 16h+, at
±40 MiB per hour, even though the autoscaler moved the whole job onto one TM at 13:38.

Ruled out on the way:

- **Our flink-dist patches:** they only add `close()` calls.
- **JVM memory:** heap, direct and metaspace are flat.
- **glibc fragmentation:** jemalloc is preloaded.
- **ForSt native object handling:** the async read, write and iterate paths and the dispose path
  close their objects.
- **Table readers:** index blocks are charged to the cache.

## 2. Leak on every restart (in-place rescale or failover with surviving TMs)

**In-place restart experiment (2026-10-07, one TM, 3 slots, ~0.5G cache per slot):** the job was
restarted 4 times via the adaptive scheduler's `PUT /jobs/<id>/resource-requirements`, with
10 min of settling each. "Unexplained" = native anon RSS outside the heap − live ForSt
`block-cache-usage` − memtables.

| Restart | Backends disposed | Their cache when disposed | Unexplained native | Step |
|---|---|---|---|---|
| baseline p=1 | – | – | 3,130 MiB | |
| p=1 → 3 | 1 | full, ~420 MiB | 3,576 MiB | **+446** |
| p=3 → 1 | 3 | young, ~100–200 MiB | 3,604 MiB | +28 |
| p=1 → 3 | 1 | full, ~413 MiB | 4,040 MiB | **+436** |
| p=3 → 1 | 3 | young, ~100–200 MiB | 4,087 MiB | +47 |

- **Each step ≈ the disposed backends' cache content, and nothing comes back.** That's
  +957 MiB over 4 restarts.
- **The same pattern explains TM-2-1's ~3G excess** after the 2026-10-04 p=3 → p=9 rescale.
- **Rough impact:** at the default memory profile (~1.65G cache per slot), a restart could leak
  up to ~5 GiB per 3-slot TM, so a restart loop of a few attempts can OOMKill a TM.
- **Deploys don't accumulate it** (fresh submission replaces the pods), nor does a TM pod restart.

**jemalloc on the live TM** (JVMTI agent, `artifacts/jeagent.c`): allocated 2.76 GiB, resident
3.16 GiB, **dirty only 34 MiB**. The leaked memory is live allocations, not freed pages that
jemalloc keeps (jemalloc 5.3.0, `background_thread: false`, 128 arenas).

**Java side of disposed backends (heap dump, TM-2-1):** `RocksDB`, `LRUCache`,
`WriteBufferManager`, `DBOptions` and `ColumnFamilyOptions` are all closed (`owningHandle_=false`).
So the native cache is still referenced from the native side.

## 3. Private per-column-family caches don't leak (2026-10-07)

**Deployed 2026-10-07 09:43 UTC** (devops values, uncommitted):

- **`state.backend.forst.memory.managed: "false"` + `block.cache-size: "128mb"`.** With no
  fixed per-slot or per-TM memory, `ForStSharedResourcesFactory.from` returns null, so each
  column family gets a private cache owned by its table factory.
- **Also in the deploy:** `MALLOC_CONF=prof:true,lg_prof_sample:20`,
  `-XX:NativeMemoryTracking=summary`, `slots: 4`.
- **Side effect of unmanaged memory:** the partitioned-index-in-cache setup only happens on the
  shared-resources path. Without it, each table reader loads its full index
  (`BinarySearchIndexReader`) outside the cache (~170–200 MiB here, the
  `estimate-table-readers-mem` metric), which `measure.sh` now subtracts too.

| | Live cache | Memtables | Table readers | Native minus all ForSt |
|---|---|---|---|---|
| before p=2 → 4 (11:24 UTC) | 114 MiB | 13 MiB | 168 MiB | 1,891 MiB |
| after (11:37 UTC) | 0 | 0 | 197 MiB | 1,892 MiB, **+1** |

- **Two warm backends (~295 MiB of ForSt memory) were disposed and all of it was freed.** Under
  the shared cache, the same restart left ~440 MiB behind. So the native holder is in the
  slot-shared path (`SLOT_SHARED_MANAGED` → `ForStSharedResources`), not in ForSt/RocksDB
  instance teardown. One clean data point.
- **The earlier p=4 → 2 "+187 MiB" was an artifact:** table-reader memory wasn't measured yet.
- **jemalloc profiling works:** `before.heap` symbolizes locally (see Method notes).
  - ForSt's live native memory is mostly `RetrieveBlock → UncompressBlockData` (cache blocks),
    `TableCache::GetTableReader → BinarySearchIndexReader::Create` (index blocks), and
    compaction/flush buffers.
  - The JVM's own `os::malloc` (~1.3G) is mostly direct network buffers.

**Incident caused by the experiment (2026-10-07 11:25–13:50 UTC):** `cycle.sh` set *all*
vertices, sources included, to p=4. The source topics have 3 partitions, so source subtask 3
had no split, and its watermark stayed at `Long.MIN_VALUE`. That held back all event-time
logic (pre-grouping, per-block drain), and xrp-stacks-v7 sent nothing to Kafka for ~2.5h. Fixed
by setting the source back to 3; the backlog drained (no loss: checkpoint restore,
exactly-once sink). `cycle.sh` now leaves `Source:` vertices untouched. The autoscaler and the
job code cap sources at the partition count, so only manual rescales can hit this.

## 4. Root cause: unclosed `DBOptions` copy in the rescale restore

`ForStIncrementalRestoreOperation.restoreTempDBInstance` (release-2.3.0 line 964, unchanged on
master 8171d96, 2026-10-07):

```java
DBOptions dbOptions = new DBOptions(this.forstHandle.getDbOptions());  // native copy
dbOptions.setDbLogDir("");
RocksDB restoreDb = ForStOperationUtils.openDB(..., dbOptions);       // never closed afterwards
```

- **What the copy holds:** RocksJava's copy constructor copies the native `DBOptions`,
  including its reference to the slot-shared `WriteBufferManager`. The WBM in turn holds the
  shared `LRUCache` (its memtable reservations are charged there).
- **Why the cache outlives the backend:** when the backend is disposed, the DB, options, WBM and
  cache *Java handles* are all closed (matching the heap dump), but the leaked copy keeps the
  native WBM, and so the cache with all its blocks, alive for the life of the JVM.
- **When it happens:** temp DBs are only used for multi-handle restores (all scale-ins, some
  scale-outs; `innerRestore`). That explains the alternation in section 2:
  - restores after p=3 → 1 pinned the new p=1 cache, which leaked in full (~440 MiB) at the
    next restart;
  - the p=1 → 3 restores used single handles, so those backends freed everything (+28/+47).
- **Same-parallelism failovers don't leak.** Neither does unmanaged ForSt memory: there's no
  WBM, so the leaked copy pins only a few KB (section 3).
- **Fix:** `RestoredDBInstance` takes the copy and closes it after its DB
  (`artifacts/repro/ForStIncrementalRestoreOperation-close-temp-dboptions.diff`). Flink's
  RocksDB backend doesn't have this copy.

**Reproductions** (`artifacts/repro/`, forstjni 0.1.8 = Flink 2.3.0's, jemalloc with decay 0):

- **`DbOptionsCopyLeak.java` (JNI only):** shared `LRUCache` + `WriteBufferManager(cache)` +
  `DBOptions` with the WBM, cache filled to 308 MiB, then everything closed. Retained:
  94 MiB with no copy, **422 MiB with an unclosed copy**, 90 MiB with the copy closed.
- **`ScaleInLeakCheck.java` (MiniCluster, prod's patched flink-dist 2.3.0):** a ForSt
  async-state job restored alternately at p=2 and p=1 from native savepoints, 1g managed
  memory, 2 slots. RSS after each run's cluster is gone:

| Run | Stock | Step | Patched | Step |
|---|---|---|---|---|
| base | 2,150 | | 2,151 | |
| 0: p=2 fresh | 2,301 | +151 | 2,302 | +151 |
| 1: p=1 (scale-in) | 2,733 | **+432** | 2,347 | +45 |
| 2: p=2 | 2,755 | +22 | 2,367 | +20 |
| 3: p=1 (scale-in) | 3,167 | **+412** | 2,393 | +26 |
| 4: p=2 | 3,171 | +4 | 2,395 | +2 |
| 5: p=1 (scale-in) | 3,591 | **+424** | 2,409 | +14 |

  Stock leaks about one slot's cache per scale-in (+1,290 MiB over 3 cycles). Patched: +107 MiB
  in total, shrinking (JVM warm-up).

## Related changes (devops, uncommitted)

These came out of the investigation: in-place scale-downs inside one pod free no capacity and
each costs a restart, i.e. a leak event.

- **Chart:**
  - autoscaled jobs get `job.autoscaler.vertex.min-parallelism = slots`;
  - rendering fails unless slots divide 128 (the autoscaler rounds up to divisors of the
    key-group count);
  - `taskManager.replicas` defaults to 1 for autoscaled jobs (it only sets the starting
    parallelism in native mode);
  - `slotmanager.number-of-slots.max` is now required for autoscaled jobs.
- **The 9 autoscaled values files:** `slots: 3 → 4`, explicit `replicas` removed.

## Side findings

- **The forst-s3-connection-leak patches are validated in prod** (see
  `2026-09-forst-s3-connection-leak`): after the 2026-10-04 rescale there were no
  DELAYER-pinned backends and no leaked S3 streams.
- **Bounded heap-only retention:** Avro `AccountStackHead.READER$` / `StorageSegments.READER$`
  → `SpecificDatumReader.creator` (the old task thread) → disposed backend. At most one task per
  Avro class. Not worth fixing.
- **xrp-balances-v6 still runs `77f79b53` (unpatched)** and has the S3 stream leak signature.
  Bump to `ca429d16`+.

## Method notes

- **Native vs heap RSS:** take the heap range from `jcmd 1 GC.heap_info`, then sum `Rss:` of
  anonymous mappings in `/proc/1/smaps` outside it (`artifacts/measure.sh`). Prometheus
  `container_memory_rss` mixes in heap touch-up, so don't use it to judge native growth.
- **ForSt metrics:** JM REST `/jobs/<job>/vertices/<stack vertex>/subtasks/<n>/metrics`, ids
  `Create_stack_changes.<cf>.rocksdb_*`. A shared cache reports the same value on every CF.
- **jemalloc stats in a live JVM:** gdb isn't available and `ptrace_scope=1`, so load
  `artifacts/jeagent.so` with `jcmd 1 JVMTI.agent_load /tmp/je/jeagent.so "stats:<out>"`.
  Modes: `stats`, `purge`, `dump` (needs `prof:true`). Built against glibc ≤2.38; the TM has
  2.39. Copying it into a prod pod needs a manual step (the auto-mode classifier blocks it).
- **Heap dumps:** `jcmd 1 GC.heap_dump -gz=1` (~55 MB, <1 s).
- **jeprof symbols:** the TM image has `jeprof` but no binutils. Copy the `.heap` file,
  `/usr/bin/jeprof`, and the libraries listed in the profile (`libjvm.so`,
  `libforstjni-linux64.so` from `/tmp/tm_*/tmp/rocksdb-lib-*/`, libc, libstdc++, libjemalloc,
  `bin/java`) into a local dir mirroring their paths, then run
  `perl jeprof --lib_prefix=<dir> --text --cum --show_bytes <dir>/opt/java/openjdk/bin/java x.heap`.
  The `operator delete[]` line is really `operator new[]`: jeprof attributes a return address
  to the nearest symbol.
- **ForSt LOG:** the largest UUID-named file in `/tmp/tm_*/tmp/<job>/op_*/db/`. It has no stats
  (`stats_dump_period_sec: 0`). The 10g file cache is per operator.
