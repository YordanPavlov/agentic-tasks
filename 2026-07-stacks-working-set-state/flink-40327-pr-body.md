## What is the purpose of the change

`ForStGeneralMultiGetOperation.process()` allocates a `new ReadOptions()` (a JNI object owning native memory) for every executed async read batch, and no code path closes it. On a ForSt + async State V2 job the resulting native memory leak is proportional to read volume. On a production workload we measured TaskManager RSS growth of 2.2–3.9 GiB/h, leading to container OOM kills every ~10–14 hours. jemalloc allocation profiling showed `Java_org_forstdb_ReadOptions_newReadOptions` dominating the live-allocation diff.

This PR closes the per-batch `ReadOptions` with try-with-resources. This is safe because:

- `db.multiGetAsList(readOptions, ...)` is synchronous within the executor lambda and returns `byte[]` copies — nothing referencing the `ReadOptions` outlives the batch;
- all early returns and exception paths are covered by try-with-resources;
- the sync ForSt backend already treats `ReadOptions` as a managed, closeable resource (`ForStResourceContainer#getReadOptions`, registered in `handlesToClose`).

## Brief change log

- Wrap the per-batch `ReadOptions` in `ForStGeneralMultiGetOperation#process` in try-with-resources so it is closed after the batch completes.

## Verifying this change

This change is already covered by existing tests: functional behavior of `process()` for value/list/map states is exercised by `ForStGeneralMultiGetOperationTest` (full `flink-statebackend-forst` suite green locally: 979 tests, 0 failures). No new test asserts the closure itself — the `ReadOptions` is local to the executor lambda, and we preferred not to add a test-only seam to the production code; happy to add one if preferred.

Additionally, verified in production (Flink 2.3.0, ForSt async state backend, with this class patched): TaskManager RSS growth dropped from 2.2–3.9 GiB/h to ~20 MiB/h (flat over 19+ hours) with unchanged job output.

## Does this pull request potentially affect one of the following parts

- Dependencies (does it add or upgrade a dependency): no
- The public API, i.e., is any changed class annotated with `@Public(Evolving)`: no
- The serializers: no
- The runtime per-record code paths (performance sensitive): yes — async state read path; the change adds one native object close per read batch, negligible relative to the multiGet itself
- Anything that affects deployment or recovery: no
- The S3 file system connector: no

## Documentation

- Does this pull request introduce a new feature? no
- If yes, how is the feature documented? not applicable
