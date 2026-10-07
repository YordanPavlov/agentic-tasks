# xrp-stacks-v7 native memory (Hetzner prod)

xrp-stacks-v7 TaskManager RSS grew by ~1.9–2.4 GiB/day per TM (2026-10-04/06), the only job in
the cluster doing so; xrp-balances-v6 on the same backend (ForSt + async state) was flat.

**Status (2026-10-07):**

- **The steady daily growth was the block cache filling (high confidence), not a leak.**
- **A real native leak was found: every job restart on a surviving TM leaks about the block
  cache content of the backends it disposes (high confidence it accumulates; medium-high that
  it's cache content).**
- The owner is not identified yet. The next test (private per-column-family caches instead of
  the slot-shared one) is prepared in devops but **not deployed**.

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

## Next test (prepared, not deployed)

devops `hprod/.../xrp/xrp-stacks-v7/values.helm.yaml` (uncommitted):

- **`state.backend.forst.memory.managed: "false"` + `state.backend.forst.block.cache-size:
  "128mb"`.** With no fixed per-slot or per-TM memory, `ForStSharedResourcesFactory.from` returns
  null, and every column family gets a private cache owned by its table factory instead of the
  slot-shared `ForStSharedResources` cache.
  - **No steps across warm-cache restarts:** the leak is in the shared-cache lifecycle (ForSt
    code), which is what to fix or report upstream.
  - **Steps remain:** it's in RocksDB/ForSt native teardown, independent of sharing.
- **Also in the same deploy:**
  - `MALLOC_CONF=prof:true,lg_prof_sample:20` and `-XX:NativeMemoryTracking=summary`, so a
    `jeprof --base` diff around one restart can attribute the leaked allocations;
  - `slots: 4` (from the chart change below).
- **Procedure:**
  1. Let the caches fill (~30–40 min).
  2. Run `PER_CF=1 artifacts/cycle.sh <p> 600`, alternating p=4 → 2 → 4 so every restart
     disposes warm caches. `PER_CF=1` makes `measure.sh` sum the per-CF caches.
  3. The autoscaler floor is 4, but its 2h stabilization after each restart leaves room for
     manual cycles.

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
- **ForSt LOG:** the largest UUID-named file in `/tmp/tm_*/tmp/<job>/op_*/db/`. It has no stats
  (`stats_dump_period_sec: 0`). The 10g file cache is per operator.
