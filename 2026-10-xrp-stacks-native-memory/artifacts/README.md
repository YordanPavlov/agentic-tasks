- `measure.sh <tsv> <label>`: one sample of xrp-stacks-v7 native memory (heap vs native anon RSS, live ForSt cache and memtables). `PER_CF=1` when ForSt memory is unmanaged.
- `cycle.sh <p> [settle_s]`: in-place restart via `PUT /jobs/<id>/resource-requirements`, waits for RUNNING, settles, measures.
- `restart-samples-2026-10-07.tsv`: the 4-restart run (shared cache, ~0.5G per slot).
- `restart-samples-private-cache-2026-10-07.tsv`: fill + 2 restarts with unmanaged ForSt memory (the 11:04 row predates the table-readers column).
- `jeagent.c`: JVMTI attach agent for jemalloc stats/purge/prof dump inside a live JVM. Build:
  `gcc -O1 -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -fno-stack-protector -fPIC -shared -o jeagent.so jeagent.c`
  (check `objdump -T jeagent.so` needs no GLIBC newer than the target's).
- `repro/DbOptionsCopyLeak.java`: JNI-only check that an unclosed `DBOptions` copy pins the shared block cache (via the WBM). `javac -cp forstjni-0.1.8.jar`; run `none|leak|closed` with `LD_PRELOAD=libjemalloc.so.2 MALLOC_CONF=dirty_decay_ms:0,muzzy_decay_ms:0`.
- `repro/ScaleInLeakCheck.java`: MiniCluster job restored alternately at p=2/p=1 from native savepoints; prints RSS after each run. Classpath: `flink-dist-2.3.0.jar` + log4j jars (prepend the patched classes for the fix). Same jemalloc settings, plus `-Xms2g -Xmx2g -XX:+AlwaysPreTouch`.
- `repro/ForStIncrementalRestoreOperation-close-temp-dboptions.diff`: the fix against release-2.3.0.
- `repro/scale-in-{stock,patched}.txt`: the two runs' results.
