# FLINK-40327 upstream PR — drafts

Branch: `FLINK-40327` in `~/src/flink` (fork YordanPavlov/flink, based on
`upstream/master` @ aa30930db94). Yordan commits/pushes; agent prepares.

## Commit message

```
[FLINK-40327][state/forst] Close ReadOptions in ForStGeneralMultiGetOperation to fix native memory leak

ForStGeneralMultiGetOperation.process() creates a native ReadOptions per
async read batch and never closes it, leaking native memory proportional
to async read volume. Wrap it in try-with-resources: multiGetAsList is
synchronous within the executor lambda and returns byte[] copies, so
nothing referencing the ReadOptions outlives the batch.
```

## PR title

```
[FLINK-40327][state/forst] Close ReadOptions in ForStGeneralMultiGetOperation to fix native memory leak
```

## PR body (Flink template)

### What is the purpose of the change

`ForStGeneralMultiGetOperation.process()` allocates a `new ReadOptions()`
(a JNI object owning native memory) for every executed async read batch,
and no code path closes it. On a ForSt + async State V2 job the resulting
native memory leak is proportional to read volume. On a production
workload we measured TaskManager RSS growth of 2.2–3.9 GiB/h, leading to
container OOM kills every ~10–14 hours. jemalloc allocation profiling
showed `Java_org_forstdb_ReadOptions_newReadOptions` dominating the
live-allocation diff.

This PR closes the per-batch `ReadOptions` with try-with-resources. This
is safe because:

- `db.multiGetAsList(readOptions, ...)` is synchronous within the executor
  lambda and returns `byte[]` copies — nothing referencing the
  `ReadOptions` outlives the batch;
- all early returns and exception paths are covered by try-with-resources;
- the sync ForSt backend already treats `ReadOptions` as a managed,
  closeable resource (`ForStResourceContainer#getReadOptions`, registered
  in `handlesToClose`).

### Brief change log

- Wrap the per-batch `ReadOptions` in `ForStGeneralMultiGetOperation#process`
  in try-with-resources so it is closed after the batch completes.
- (if we add the test) Add a regression test asserting all `ReadOptions`
  created by `process()` are closed when the returned future completes.

### Verifying this change

- Existing `ForStGeneralMultiGetOperationTest` covers the functional
  behavior of `process()` for value/list/map states.
- (if we add the test) New test `testReadOptionsClosedAfterProcess`
  asserts closure via `isOwningHandle()`, following the pattern of
  `ForStResourceContainerTest`.
- Verified in production (Flink 2.3.0, ForSt async state backend, patched
  class): TaskManager RSS growth dropped from 2.2–3.9 GiB/h to ~20 MiB/h
  (flat over 19+ hours) with unchanged job output.

### Does this pull request potentially affect one of the following parts

- Dependencies: no
- Public API: no
- Serializers: no
- Performance-sensitive code paths: yes (async state read path; change is
  one native object close per batch, negligible vs the multiGet itself)
- Deployment or recovery: no
- File system connectors: no

### Documentation

- Does this pull request introduce a new feature? no
- If yes, how is the feature documented? not applicable

## Process checklist

- [x] JIRA FLINK-40327 filed + assigned (Yordan)
- [x] Fork + branch FLINK-40327 off upstream/master
- [x] Fix ported (one-line try-with-resources), spotless clean
- [x] Module build green (deps built, -DskipTests -Dfast)
- [x] Module test suite green 2026-08-17 (979 run, 0 failures, 5 skipped)
- [ ] Decide: include regression test (agent recommends yes; seam =
      package-private createReadOptions() + test subclass, isOwningHandle
      assertion)
- [ ] Yordan: commit on branch, push to fork, open PR vs apache/flink:master
- [ ] Ask in PR/JIRA about backport to release-2.3
- [ ] After merge: note fixed version in flink-patches/README.md; drop
      Dockerfile injection on the version bump that contains the fix
