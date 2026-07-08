# Flink testability: job-graph build harness + CI gate (test-hardening step #3)

**Repo:** `etherbi-flink` · **Status:** not started — this doc is the starting point for a future session · **Drafted:** 2026-06-29

> This is **step #3** of a small test-hardening sequence that came out of a real production incident (an XRP job that compiled fine but threw `InvalidTypesException` at Flink graph-assembly time). It is a *plan*, not a record of done work.

---

## The incident that motivates this

Deploying XRP stacks failed at startup, not at compile:

```
InvalidTypesException: The return type of function 'apply(XRPStacks.scala:22)' could not be
determined automatically, due to type erasure. ... The generic type parameters of 'Tuple2' are missing.
```

`XRPStacks.scala:22` passed a **bare Scala lambda** to `.map(...)` producing a `Tuple2`. Flink's Java `TypeExtractor` runs at **graph-assembly time** (inside `job.build(config)` → `apply(config, env)`) and cannot recover a lambda's erased generics. It is **invisible to both the Scala compiler and every existing unit test** — the failure is in graph *wiring*, a layer below unit logic and above a full integration run, and that layer currently has **zero test coverage** (no test calls `.build` or constructs a `StreamExecutionEnvironment`).

## The sequence

1. **Job-graph smoke test (the idea).** A test that calls `job.build(testConfig)` and asserts no throw — it walks the exact `Main → build → apply` path and forces all type extraction, no data or Kafka needed.
2. **Make the bug class structurally impossible — ✅ DONE.** Replaced the `XRPStacks` lambda with an anonymous `MapFunction[In, Out]` (generics live in the class signature). This mirrors the working pattern in `ETHAccountChanges` and the `.returns(TypeHint)` pattern in `KafkaSourceModels2`. Structural, but **not durable on its own**: nothing stops a new bare lambda from creeping back in.
3. **← THIS DOC. Reusable build harness + parametrized "all jobs build" test + CI gate.** Generalize #1 across every job and run it in CI, so a graph that won't assemble can never be merged or deployed. This is the durable backstop that #2 alone doesn't give.

---

## Why #3 is worth it (and cheap)

`StreamingScalaJob.build(config)` (`FlinkJob.scala`) creates the env (local when `config.localEnvironment`) and calls `apply(config, env)`. **The whole graph — every `.map/.flatMap/.keyBy/.window/.sinkTo` and all type extraction — is assembled there, before `execute()`.** Flink Kafka sources are **lazy** (they register a transformation; they connect only at task runtime), so `build` is **offline-safe** given a valid `Config`. That means a test that merely calls `build` (and never `execute`) reproduces graph-assembly failures like the XRP one with no infrastructure. **Confidence: high** — the incident exception originated inside `build`.

---

## Job inventory (what the parametrized test must cover)

15 `FlinkJob` implementors today (`grep "extends StreamingScalaJob"`):

| job | notes / type-erasure surface |
|---|---|
| `job/ETHAccountChanges` | anonymous `FlatMapFunction` (good pattern) + `keyBy` lambda |
| `job/ETHAccountChangesExact` | anonymous `FlatMapFunction` |
| `xrp/job/XRPStacks` | **was** the failing one (now fixed) — keep as explicit regression |
| `xrp/job/XRPBalances`, `xrp/job/XRPExtractor` | inspect for lambda-typed operators |
| `utxo/job/UTXOAccountChanges`, `utxo/job/CardanoAccountChanges` | UTXO account-change jobs |
| `utxo/job/ComputeUTXOBalances`, `utxo/job/ComputeCardanoBalances` | balance jobs |
| `job/ERC20AddressBalances`, `job/ERC20AddressBalancesExact`, `job/ETHAddressBalances` | balances; talk to archive node / ClickHouse **at runtime only** (build stays offline) |
| `job/KafkaTopicCopier`, `job/KafkaTopicSorter` | simple plumbing jobs |

Jobs are selected at runtime by the `class` config key (`Config.classToRun = params.getString("class")`, dispatched reflectively in `Main.scala`). **There is no central registry of jobs** — that is the first design decision below.

---

## Design

Three pieces, smallest-first:

1. **Harness** in `src/test/scala/net/santiment/testutil/flink/` (next to `InMemorySink`, `Collectors`):
   ```scala
   def buildsCleanly(job: StreamingScalaJob[Config], config: Config): Unit
   // calls job.build(config) with localEnvironment=true; optionally env.getStreamGraph to force any
   // residual lazy extraction; asserts no exception. Never calls execute().
   ```
2. **Parametrized test** that iterates every production job and asserts `buildsCleanly`. Keep an explicit XRP case as a named regression test.
3. **CI gate** — run the suite on every PR so a non-assembling graph blocks merge (the repo already has GitHub workflows; add a test step there).

---

## Hard parts / open questions to resolve in the session

1. **How to enumerate jobs.** Options, pick one:
   - **(a) Explicit list** in the test — simple, but can drift from reality (a new job added without a test).
   - **(b) Reflective classpath scan** for `FlinkJob`/`StreamingScalaJob` subclasses — no drift, but needs a scanning lib and care to exclude abstracts/test doubles.
   - **(c) Drive from real deployment configs** (the `*.conf` that set `class = …`). **Highest fidelity** — tests the *actual* job+config pairs that get deployed, and catches config-key wiring bugs too. **The deploy configs live outside this repo** (deploy/helm repo) — locating them and deciding whether to vendor a copy/fixture into the test is the main open question.
2. **Config fixtures.** `Config(params: com.typesafe.config.Config)` needs valid params; each job reads different keys (topics, kafka servers, `localEnvironment=true`, …). Decide minimal-permissive fixture vs. real deploy-config fixtures (option 1c). `ConfigSpec.scala` and `reference.conf`/`application.conf` are the starting points.
3. **Per-job offline-safety.** Confirm no job does network I/O *at build time* (sources are lazy; the AddressBalances jobs' archive-node/ClickHouse access is runtime-only — verify). Any genuine build-time dependency needs a stub.
4. **Local env setup.** `build` with `localEnvironment` loads `flink-conf.yaml` via `FLINK_CONF_DIR`; make the test self-contained (set the flag, avoid depending on a cluster conf).
5. **Don't execute.** Assert on graph assembly only (`build`, optionally `getStreamGraph`); never `env.execute`.

---

## Acceptance criteria

- A `buildsCleanly` helper in `testutil/flink`.
- A parametrized test covering **all** production jobs (however enumeration is resolved), green in CI.
- An explicit `XRPStacks` regression case (would have caught the original incident).
- CI fails the PR if any job's graph does not assemble.
- A short note in the repo (e.g. `docs/`) on the convention from step #2: *no bare lambda into `.map/.flatMap/.keyBy/.process` returning a tuple/generic — use an anonymous function class or `.returns(TypeHint)`* — so the harness and the convention reinforce each other.

---

## Anchors

- `src/main/scala/net/santiment/FlinkJob.scala` — `StreamingScalaJob.build` (the entry the test drives) and `apply`.
- `src/main/scala/net/santiment/Main.scala` — reflective dispatch via `config.classToRun`.
- `src/main/scala/net/santiment/Config.scala` — `Config`, `classToRun`, `localEnvironment`.
- `src/main/scala/net/santiment/job/ETHAccountChanges.scala:42` — the *correct* anonymous-`FlatMapFunction` pattern to imitate.
- `src/main/scala/net/santiment/common/sources2/KafkaSourceModels2.scala` — `.returns`/`TypeHint`/`getProducedType` examples.
- `src/test/scala/net/santiment/testutil/flink/` — where the harness belongs.
- Incident & fix context: the `XRPStacks.scala` `.map` change (lambda → `MapFunction`), this session.
