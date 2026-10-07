- `measure.sh <tsv> <label>`: one sample of xrp-stacks-v7 native memory (heap vs native anon RSS, live ForSt cache and memtables). `PER_CF=1` when ForSt memory is unmanaged.
- `cycle.sh <p> [settle_s]`: in-place restart via `PUT /jobs/<id>/resource-requirements`, waits for RUNNING, settles, measures.
- `restart-samples-2026-10-07.tsv`: the 4-restart run (shared cache, ~0.5G per slot).
- `jeagent.c`: JVMTI attach agent for jemalloc stats/purge/prof dump inside a live JVM. Build:
  `gcc -O1 -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=0 -fno-stack-protector -fPIC -shared -o jeagent.so jeagent.c`
  (check `objdump -T jeagent.so` needs no GLIBC newer than the target's).
