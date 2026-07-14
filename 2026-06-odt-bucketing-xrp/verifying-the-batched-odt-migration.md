# Verifying the batched-`odt` migration — runbook (XRP first)

> **Master guideline for the whole migration:** the ADR [`etherbi-flink/docs/decisions/configurable-odt-bucketing.md`](../../src/etherbi-flink/docs/decisions/configurable-odt-bucketing.md). Read it first — it carries the *why*, the bucket-size decision (hourly), the precision derivations, and the cross-repo seam. This runbook is the operational *how-to-verify*; that doc is the source of truth for every expectation below.
> **Background:** the concept doc [`etherbi-flink/docs/concepts/stacks.md`](../../src/etherbi-flink/docs/concepts/stacks.md) — what stacks are and how they're consumed.
>
> *(These two in-repo docs are the authoritative reference. They were distilled from the original `santiment-cheatsheets` analysis notes `bounding-flink-stack-cohort-state-with-5min-buckets.md` / `stacks-from-construction-to-usage.md`, which are not checked out in this container and carry only the deeper cross-repo derivation. Links are relative to a local sibling checkout of both repos under `~/santiment` — they do not resolve on GitHub, since `tasks` and `etherbi-flink` are separate repos.)*

**Author:** Yordan + Claude analysis session · **Started:** 2026-06-29

---

## 0. What we're verifying

The Flink stacks job gained a configurable hourly `odt` bucket (default off; enabled for XRP as the first chain). Verification has **two layers**:

1. **Source data** — does the batched raw stacks table (`xrp_stacks_experimental`) carry the same information as the non-batched production table (`xrp_stacks`), modulo the intended `odt` quantization? (§3)
2. **Computed metrics** — when the age-based metrics are recomputed from the batched stacks, do the deviations stay inside the bounds the hourly bucket predicts? (§4)

Layer 1 is a pure data-equivalence check and is the gate for enabling more chains. Layer 2 is the customer-facing impact and is where the *expected* (non-zero but bounded) deviations live.

---

## 1. Environment

| | |
|---|---|
| Cluster | AWS production — `clickhouse.production.san:30900` |
| Connect | **always `-u readonly`** (only permitted connection) |
| Non-batched (baseline) | `xrp_stacks` → `Distributed` over `xrp_stacks_shard_v8` |
| Batched (hourly `odt`) | `xrp_stacks_experimental` → `Distributed` over `xrp_stacks_shard_v9` |
| Schema (both) | `sign, dt, contractAddress, blockNumber, nonce, odt, amount, address, assetRefId, txID` |
| Raw-table key (`ORDER BY`) | `(contractAddress, address, sign, dt, nonce)` |

---

## 2. Preconditions — do **not** trust a comparison until these hold

These three bit us on the first pass (2026-06-29) and produced wildly drifting numbers. They are not optional.

1. **Ingestion must be complete.** While the Flink job + Kafka→CH ingestion are still running, the experimental table is a moving target: the `dt` frontier advances and fixed-window sums change between queries. **Gate:** `max(dt)` is stable, *and* a `count()` over a fixed behind-frontier window is identical across two reads minutes apart.
2. **Deduplicate with `GROUP BY` the ORDER BY key — never `FINAL`.** Both tables are `ReplacingMergeTree`; production carries ~39% exact duplicate rows (job re-runs). **`FINAL` does not dedup across shards on a `Distributed` table**, so it leaves cross-shard dupes and gives wrong sums. Dedup correctly by grouping on `(contractAddress, address, sign, dt, nonce)` and taking one row per key (`any(...)`); this is also immune to in-progress merges.
3. **Use a behind-frontier window, bounded identically in both tables.** Pick `[A, B)` well below the current frontier so neither table is still gaining rows in it.

> `readonly` cannot use `clusterAllReplicas(...)` / `cluster(...)` / `remote(...)` (no `REMOTE` grant), so `system.parts` / `system.merges` only show the local broker's shard — treat as a representative sample.

Throughout, let **`W`** = `dt >= 'A' AND dt < 'B'` (your chosen frozen window).

---

## 3. Layer 1 — source stacks equivalence

### 3.1 Same input universe
```sql
SELECT uniqExact(address) AS addrs, uniqExact(contractAddress) AS currencies
FROM xrp_stacks_experimental WHERE W;   -- compare to xrp_stacks over the same W
```
**Expect:** identical address & currency counts. (2026-06-29 snapshot: 22,467 / 1,280 in both.)

### 3.2 `odt` is hourly-bucketed
```sql
SELECT countIf(odt!=0 AND toUnixTimestamp(odt)%3600 != 0) AS not_hour_aligned,
       uniqExact(odt) AS distinct_odt
FROM xrp_stacks_experimental WHERE W;
```
**Expect:** `not_hour_aligned = 0`; `distinct_odt` far smaller than in production (we saw ~105× fewer: 425k → 4k). Liability rows (`odt = 0`) are exempt.

### 3.3 No ORDER BY-key collisions (the ADR open gate)
```sql
SELECT countIf(cnt>1 AND amt_spread>1e-9) AS collide_amount,
       countIf(cnt>1 AND odt_spread>0)     AS collide_odt
FROM (SELECT count() cnt, max(amount)-min(amount) amt_spread, max(odt)-min(odt) odt_spread
      FROM xrp_stacks_experimental WHERE W
      GROUP BY contractAddress,address,sign,dt,nonce);
```
**Expect both = 0.** Same-hour installs differ in `dt`/`nonce`, so they stay on distinct keys and survive `ReplacingMergeTree`. A non-zero here means bucketing collapsed rows → data loss. *(Confirmed 0 on 2026-06-29; this closes the master doc's §9 open item for XRP.)*

### 3.4 Conservation — deduped net per currency
```sql
WITH
  prod AS (SELECT contractAddress, sum(v) s FROM
            (SELECT contractAddress, any(sign*amount) v FROM xrp_stacks WHERE W
             GROUP BY contractAddress,address,sign,dt,nonce) GROUP BY contractAddress),
  exp  AS (SELECT contractAddress, sum(v) s FROM
            (SELECT contractAddress, any(sign*amount) v FROM xrp_stacks_experimental WHERE W
             GROUP BY contractAddress,address,sign,dt,nonce) GROUP BY contractAddress)
SELECT count() currencies,
       countIf(abs(prod.s-exp.s) > 1e-6) AS net_mismatch,
       max(abs(prod.s-exp.s)) AS max_abs_diff
FROM prod FULL JOIN exp USING contractAddress;
```
**Expect:** `net_mismatch = 0`. Bucketing relabels `odt`; it never changes signed amount, so net held per currency must match exactly (float jitter aside). Any real mismatch is a bug — investigate the specific currency.

### 3.5 Seam-equivalence — **the central check**
The downstream interface table is `sum(sign*amount) GROUP BY (asset, dt, odt)`. Bucketing must be invisible there: production truncated to the hour must equal experimental cell-for-cell.
```sql
WITH
  P AS (SELECT dt, toStartOfHour(odt) AS odtb, sum(v) AS s FROM
          (SELECT dt, odt, any(sign*amount) v FROM xrp_stacks WHERE W
           GROUP BY contractAddress,address,sign,dt,nonce,odt) GROUP BY dt, odtb),
  E AS (SELECT dt, odt AS odtb, sum(v) AS s FROM
          (SELECT dt, odt, any(sign*amount) v FROM xrp_stacks_experimental WHERE W
           GROUP BY contractAddress,address,sign,dt,nonce,odt) GROUP BY dt, odtb)
SELECT count() AS cells,
       countIf(abs(ifNull(P.s,0)-ifNull(E.s,0)) > 0.01) AS mismatched_cells,
       max(abs(ifNull(P.s,0)-ifNull(E.s,0))) AS max_abs_diff
FROM P FULL JOIN E ON P.dt=E.dt AND P.odtb=E.odtb;
```
**Expect:** `mismatched_cells = 0`, `max_abs_diff ≈ 0`. *(On a partial-data snapshot 2026-06-29 this already matched exactly over Jan–Feb: 0/18,208 cells, max diff 0. Re-confirm on the full settled 6 months.)* This is the strongest single evidence that **every metric downstream sees identical input**.

### 3.6 The win — row/cohort reduction
```sql
SELECT count() FROM xrp_stacks_experimental WHERE W;  -- vs xrp_stacks (deduped) over W
```
**Expect:** experimental has materially fewer rows (fewer cohorts → fewer pop/remainder rows). We saw ~2.83M vs ~5.08M over the first 6 months. This is the state-bounding payoff, not a correctness signal — don't alarm on it.

---

## 4. Layer 2 — computed metrics, and what to expect

Once Layer 1 passes on settled data, recompute the age-based metrics from `xrp_stacks_experimental` (into experimental metric tables) and compare to production for the XRP asset. Resolve `asset_id` / `metric_id` via `metric_metadata` / `asset_metadata` (see the `clickhouse-metrics-navigation` notes).

**Only age-based metrics that read `distribution_deltas_5min` can deviate** (master doc §16): `circulation_intraday`, `realized_cap_intraday` (→ `mean_realized_price_usd_intraday`, and composite `mvrv_usd_intraday`), `stack_age_consumed_intraday`, `stack_price_consumed_intraday`, `dormant_circulation`, `stack_age_bands`, `creation_timestamp` (+ dollar/interval variants), `transaction_volume_profit_loss`, `supply_in_profit`, and the `_1day` rollup. Everything else (transfers, DAA, exchange/label balances, DEX, prices, fees, …) reads other sources and is **unaffected**.

### Expected deviation, by time window (hourly bucket)

The mechanism: a coin's `odt` snaps back to its hour-start, so its age-out fires up to one hour early. The level bias and sawtooth amplitude both scale linearly with `bucket / window` (master doc §14).

| window | time-avg level bias `≈ bucket/(2T)` | step amplitude `≈ bucket/T` | verdict |
|---|---|---|---|
| **intraday 1d** | **~2.1%** | ~4.2% | **small, expected deviation** — visible sawtooth on `circulation_intraday_1d` / `realized_cap_intraday_1d` |
| intraday 7d | ~0.3% | ~0.6% | sub-1%, borderline visible |
| intraday 30d | ~0.07% | ~0.14% | within noise |
| intraday ≥90d | <0.03% | <0.06% | effectively none |
| **all daily metrics** | **0% (exact)** | 0% | daily circulation/dormant/age cancel on `toDate(odt+period)`, invariant to a sub-day `odt` shift |

So the headline expectation: **a small (~2%) deviation on the 1-day intraday metrics, shrinking to near-zero by 7–30d and essentially nothing on long windows; daily metrics unchanged.** If 1d deviations are ≫ a few percent, or long-window/daily metrics deviate at all, something is wrong.

### Two extra nuances (master doc §11, §15, §17)
- **Acquisition price (realized-cap family).** A coarser `odt` attaches a slightly staler ASOF price: ~0.17–0.27% on volatile assets, ~0.007% on stablecoins, per cohort (hourly). Affects `realized_cap`/`mrp`/`mvrv` magnitude, not circulation.
- **Ratios partly self-cancel.** `mean_realized_price_intraday = realized_cap/circulation` — the level bias divides out, leaving mostly the price-ASOF term. `mvrv_usd_intraday` (composite of `price_usd / mean_realized_price`) likewise inherits the price-ASOF error but **not** the level bias.

### Comparison shape
For each affected metric and window: pull the experimental and production series over the settled window, compute per-point relative deviation, and assert `median`/`p99` deviation are within the row above. Tabulate by window so the 1d-vs-long-window gradient is visible — that gradient *is* the signature of a correct hourly bucket.

---

## 5. Gotchas learned (2026-06-29)
- **Live ingestion = moving target.** Fixed-window sums changed between queries while the load ran; the `dt` frontier advanced Jul→Sep mid-session. Always confirm settled (§2.1) first.
- **`FINAL` ≠ correct on `Distributed`.** It dedups within a shard only. Production had ~39% cross-shard exact dupes that `FINAL` left in. Use `GROUP BY` the ORDER BY key.
- **`readonly` blocks cluster/remote functions** — per-shard system-table reads are local-only.
- The drift was **not** a `ReplacingMergeTree` merge backlog (0 duplicate keys, 0 running merges observed) — it was incomplete ingestion. Don't confuse the two.

---

## 6. Sign-off criteria

**Enable the bucket for the next chain when:**
1. Layer 1 §3.1–§3.5 all pass on the full, settled 6-month window (universe identical, `odt` hourly, 0 key collisions, 0 net mismatch, 0 seam mismatch).
2. Layer 2 deviations match the §4 table: 1d intraday ~2% and bounded, 7d <1%, ≥30d within noise, no deviation on unaffected metrics.
3. **Coverage — both metric tables recomputed and diffed, not just intraday.** The Layer-2 re-run must populate **and** compare *both* `intraday_metrics_experimental` **and** `daily_metrics_v2_experimental` for the asset, over the pristine window. The daily half is a required empirical gate, **not** an analytical waiver: the §14 "daily is invariant to a sub-day `odt` shift" argument is a hypothesis to *confirm with numbers*, not a reason to skip the daily jobs. Concretely:
   - **Intraday:** ≥ the stacks-affected metric families in §4 present in `intraday_metrics_experimental`, per-metric_id median/p99 within the §4 bounds.
   - **Daily:** the daily stacks chain must actually run — first the daily emitter `xrp-age-distribution-1day-deltas` (populates the daily seam row, `metric_id 270`, in `distribution_deltas_5min_experimental`; today only the intraday emitter `162` is present), then its consumers: `circulation`, `realized-cap`, `dormant-circulation`, `stack-age-consumed`, `stack-price-consumed`, `creation-timestamps`(+intervals/dollar), `spent-coins-age-bands`, `transaction-volume-profit-loss`, `supply-in-profit`, daily cumsums + composites (`mvrv`/`mrp` daily). Then diff `daily_metrics_v2_experimental` vs prod per metric_id. **Expected: exact within float noise** (the §4 "0% (exact)" row) — but this must be *shown*, and any daily metric that deviates is a finding, not noise.
   - **Report a metric count**, per table: "N intraday metric_ids compared (all within §4 bounds); M daily metric_ids compared (all exact within float noise)." A sign-off that leaves either table empty or uncompared is incomplete.

Any failure routes back to the master doc ([`etherbi-flink/docs/decisions/configurable-odt-bucketing.md`](../../src/etherbi-flink/docs/decisions/configurable-odt-bucketing.md)) — re-check the design assumption it violates before touching code.

> **Master/background docs now live in-repo** under `etherbi-flink/docs/`: [`docs/decisions/configurable-odt-bucketing.md`](../../src/etherbi-flink/docs/decisions/configurable-odt-bucketing.md) (the ADR, incl. the §3.3 pre-enable gate) and [`docs/concepts/stacks.md`](../../src/etherbi-flink/docs/concepts/stacks.md) (the load-bearing invariant). The `santiment-cheatsheets` analysis notes remain the deeper derivation.

---

## 7. Session log — Layer 2 harness setup (2026-06-30)

Goal this session: stand up the **Layer 2 re-run** so XRP age-metrics can be recomputed from `xrp_stacks_experimental` and diffed against prod. Reviewed the etherbi-flink change first (branch `batchStacksOdt`: `Config.stacksOdtBucketMs`, `HandlerOneAccountChange` inflow merge via `bucketOf`/`peekTop`/`replaceTop`, wired through to `XRPStacks`) — logic is sound: default-off is byte-identical, the install/deletion `odt` invariant holds, liability rows never merge, merged emits mint fresh nonces. No code concerns.

### How the XRP re-run works (simpler than the LTC migration)
The DMF methodology is **unchanged** — we only swap the raw stacks *source*. No new job functions, no new metric ids.
- **Re-source:** `DAILY_SOURCE_TABLES='{"xrp_stacks":"xrp_stacks_experimental"}'` (resolved via `context.source_table()`, `job_functions/xrp_stacks.py`; not hardcoded). JSON must be **single-quoted** in the env file or bash strips the inner quotes.
- **Redirect every write** to its `_experimental` variant (see `.env.dev`). **Metadata stays on prod** (read-only) — unlike LTC, XRP adds no metric ids, and `main.py` doesn't write metadata. The acquisition-price ASOF lookup (`intraday_metrics_historic_optimization`, read-only in `age_distribution_intraday_job`) also stays on prod → real, stacks-independent prices.
- XRP `asset_id = 2053`, asset spec `xrp/2019-01-01` (labels `stacksMetrics`+`xrp`).

### Blocker found + workaround (the loader can't run `dags/` metrics)
The XRP intraday jobs live only in `specs.d/dags/intraday-metrics-xrp.yaml` as `kind: AirflowDags`, which `job.from_yaml` does **not** load (the `from_airflow_dag_yaml` flattener was **removed in commit `f8e65f9f`, 2026-06-17**). `main.py`'s `DAILY_DAG`/`--dag` arg does **not** run dag-defined metrics either — it's wired only for `export_dependency_graph`. In prod these jobs run because Airflow (`~/santiment/src/docker-airflow`, e.g. `dags/intraday_metrics_xrp.py`) invokes `main.py` per-job in a pod; we are deliberately taking **Airflow out of the test loop** and running locally.
- **Proper fix (escalated to colleagues):** restore `from_airflow_dag_yaml` + have `fetch_jobs` pass `allowed_dags={config.dag}` so `DAILY_DAG=intraday-metrics-xrp` selects a dag's jobs. ~25 lines; touches shared loader → needs coordination.
- **Workaround used (this session):** `daily_metrics/specs.d/cronjobs/xrp_intraday_experimental_DO_NOT_MERGE.yaml` — 10 standalone `kind: DailyMetricsCronJob` copies of the stacks chain. **Must not merge** (a no-`dag` cronjob would double-run in prod). Mirrors the LTC precedent.

### Files created (on clickhouse-tables branch `batchStacksOdtXRP`)
- `daily_metrics/specs.d/cronjobs/xrp_intraday_experimental_DO_NOT_MERGE.yaml` — jobs: `xrp-age-distribution-intraday-deltas` (emitter, where the experimental source enters), `xrp-circulation-intraday-deltas`, `xrp-realized-cap-intraday-deltas`, `xrp-stack-age-consumed-intraday`, `xrp-stack-price-consumed-intraday`, `xrp-network-profit-loss-intraday`, `xrp-cumulative-sums-intraday-metrics`, `xrp-cumulative-sums-intraday`, `xrp-intraday-prices` (unaffected; feeds MVRV/MRP composites), `xrp-composite-intraday-metrics`. All 10 validated against `from_parsed_yaml`'s assertions.
- `.env.dev` (repo root) — source/write redirects, `DAILY_ASSETS=xrp/2019-01-01`, window **2013-01-01 → 2018-08-01**, `DAILY_DRY_RUN=true` (safety latch). Sources cleanly; source-tables JSON parses.

### Data state observed
- `xrp_stacks_experimental` is a **genesis backfill still in progress**: frontier `max(dt)` advanced **2018-08-28 → 2019-01-12 during this session** (906M → 977M rows). Window end 2018-08-01 is genesis-anchored (cumsums need it) and ~5 months below the frontier → settled. Don't chase the frontier while ingestion is live; re-confirm §2.1 right before running. Could extend end to ~2018-12-01 once the frontier is stable.
- Partition math: 2013-01..2018-08 = 68 monthly partitions < the default `max_partitions_per_insert_block=100`, so no chunking needed for the limit (this branch lacks the LTC `max_partitions=0` patch). `default_simple_job` recomputes the whole range per call with no cursor → a 5.5y single pass may be slow; chunk yearly if it times out, keeping cumsum/composite starts at genesis.

### Tables truncated clean (operator ran, 2026-06-30; verified 0 rows)
`distribution_deltas_5min_experimental`, `intraday_metrics_experimental`, `daily_metrics_v2_experimental`, `intraday_delta_futures_experimental`, `daily_delta_futures_experimental`, `intraday_metrics_dt_optimization_experimental` — all `ReplicatedReplacingMergeTree`, truncated `ON CLUSTER default_cluster`. (Note: `intraday_metrics_experimental` had held 1.55M LTC rows `asset_id=2462`; operator chose to wipe those too.) Source `xrp_stacks_experimental` untouched.

### Gates remaining before the run (next session)
1. **Prod-write authorization** — `main.py` connects via `clickhouse_driver` as the password-less writable prod user (bypasses the readonly `clickhouse-client` wrapper); the `_experimental` writes are real prod writes. Needs explicit OK.
2. **No `uv`/`.venv` in the agent container** — `main.py` can't be executed until the Python env is provisioned (only `pip install --break-system-packages pyyaml` was available for static validation).
3. **Layer 1 (§3) not yet run on full settled data** — it's the documented gate before Layer 2 and needs zero writes; run it first.

## 8. Session log — review of clickhouse-tables PR #2270 (2026-07-02)

Reviewed [PR #2270 "Parse AirflowDag at startup"](https://github.com/santiment/clickhouse-tables/pull/2270) (WonderBeat, branch `parse-airflow-dag`) — it is a **byte-for-byte revert of `f8e65f9f`** (restores `job.from_airflow_dag_yaml` + the `context.job_specs` wiring added in `22707c03` and removed 5 days later), plus cosmetic reformatting in `context.py`.

**Verdict: yes, it replaces our hack — with two caveats.**

- **Works for our local run (verified empirically):** the parser correctly flattens `specs.d/dags/intraday-metrics-xrp.yaml` → 21 `DailyMetricsCronJob`s, all 10 jobs we need, each with `script` present and `dag: intraday-metrics-xrp` injected. In a plain git checkout there are no duplicates (once our hack file is deleted).
- **Caveat 1 — `DAILY_DAG` still does NOT select jobs.** `main.py` → `fetch_jobs` → `fetch_scripts(context)` uses the default `filter_by_dag=False`; the injected `dag` key is ignored on the run path (only `export_dependency_graph` passes `filter_by_dag`/`allowed_dags`). We must select our 10 jobs explicitly via `DAILY_JOBS=<comma-separated names>` (config `jobs`, `split_by_commas`). That's fine for us.
- **Caveat 2 — likely prod double-run regression (flag on the PR).** CI (`Jenkinsfile` stage "Cronjob DSL", since 2024-10) runs `airflow-dsl`, which **appends** flattened `DailyMetricsCronJob` specs (with `spec.dag`) to `specs.d/cronjobs/cronjobs-airflow-dsl.yaml` *before* `docker build`; the image keeps the `specs.d/dags/*.yaml` sources too (`COPY . /app`, not dockerignored). With the PR, a prod pod loads every dag job **twice** (generated spec + runtime flattener). Traced the consequence: `fetch_scripts` yields two factories; `asset_metric_graph.insert_v` doesn't raise on same-name claims (overwrites); `factory_graph` dedupes vertices by name; but `_combined_factory_list` re-adds the other copy via identity `not in` → **the job executes twice per pod invocation** (second time after everything else, out of dependency order). This is exactly why `f8e65f9f` called the parser "unneeded" — the DSL covers the image; the PR fixes local runs but regresses images. Pre-existing evidence duplicates run silently: `icrc-payment-count` is copy-pasted twice in `specs.d/cronjobs/payment_count_cronjobs.yaml` today.
  - **Suggested fix:** make the flattener additive-only — skip dag cronjobs whose name already exists in `out` (the generated specs win). One-liner in `context.job_specs`; no-op in CI images, full fix locally.
  - **Reproduced end-to-end on a dev Airflow cluster (2026-07-02 19:11, image `clickhouse-tables:parse-airflow-dag`, generated file confirmed present, 160959 bytes):** pod running the dag-defined `xrp-transaction-volume-intraday` logged `/* Starting job xrp-transaction-volume-intraday/TransactionVolumeIntradayJob([331], [3053]) */` **twice** (19:11:53.781 and 19:11:54.981) inside one `Starting run … Finished run` block — identical payloads, sequential execution. Control jobs defined only in `specs.d/cronjobs/*.yaml` (`erc20-circulation-deltas`, `arb-erc20-stack-price-consumed`, `erc20-creation-timestamps-deltas`) ran exactly once on the same image. Logs archived at `~/tmp/logs/af-4.log` (dup) and `af-log*.log` (controls).
  - **Empirically confirmed (2026-07-02, operator ran kubectl):** live prod Airflow job pod `bch-balance-changes-delta-intraday-hourly-*` (image `clickhouse-tables:production`) contains `/app/daily_metrics/specs.d/cronjobs/cronjobs-airflow-dsl.yaml` (160 KB, built Jun 30). Colleague's counter-argument ("we copy no AirflowDags/CronJobs to our envs, only graphml to S3") is true for the *airflow-sync* delivery path (its DSL output at `airflow-sync/Jenkinsfile` ~L82 is discarded; only graphml ships, L149) but doesn't cover the *image* path: clickhouse-tables' own `Jenkinsfile:29-32` runs the same DSL generation into the workspace before `docker buildx` (`Dockerfile:64 COPY . /app`), and pods read specs from the image (`docker-airflow/dags/utils.py:349` `cd /app/daily_metrics; python3 main.py`, default `specs_path=specs.d`, `config.py:113`). So the double-load with PR #2270 is real in prod pods.
- **When adopting:** rebase `batchStacksOdtXRP` onto the PR (or cherry-pick), **delete `xrp_intraday_experimental_DO_NOT_MERGE.yaml`** — keeping both would double-run our 10 jobs locally (same names in both sources), doubling the experimental writes.

## 9. Session log — Layer 1 on the full history (2026-07-02)

**The backfill is done.** `xrp_stacks_experimental` caught up to the live frontier — both tables at `max(dt) = 2026-07-02 10:44` (same minute); exp 7.34B rows vs prod 10.16B raw. The batched job is now tailing real-time data, so Layer 1 runs over the **full 13.5-year history**, not just the first 6 months.

- **Window:** `W = [2013-01-01, 2026-06-01)` (a month behind frontier). **Stability gate passed:** counts identical across two reads minutes apart (exp 7,235,385,391; prod 10,050,574,223).
- **§3.2 PASS (full window):** 0 non-hour-aligned `odt`; 116,938 distinct `odt` ≈ hours in 13.4y; 11.84M liability rows (`odt=0`, exempt).
- **§3.1 currencies exact (239,663 both); addresses differ by 7** (exp 8,711,784 vs prod 8,711,777). Investigated to root cause:
  - 7 addresses exist **only in experimental**; prod `xrp_stacks` has **zero rows for them anywhere in history**. All cluster in Jun 2025 (17th/26th/30th), native XRP, tiny amounts (nets ~1.2–131 XRP, ~250 total), 2–5 rows each, real blockNumbers/txIDs, coherent install→pop→remainder sequences.
  - All 7 are present in **prod `xrp_balances`** (verified `Distributed` over `xrp_balances_shard_v8`, i.e. the same non-batched generation) at the **identical timestamps** → the source carried the transactions; **prod v8 stacks dropped them**. Pre-existing production gap, not a bucketing defect — experimental is *more* complete. Addresses: `rQ97fZmLpUFdAD3aS3giV4J1XKYiBawpSQ, rUdLCE28YZJMbQnBjJBK7povD4vreDP5Uv, raTzvXMy1dbJQ8SYnTa52R6y2rXmDtatvt, rs2GS2cPN4EQDGeTX25eTbn1vrUzbUF3jF, rdCY2m7RmGW2ohjkvKGtfbF1RwCMp5ikp, rNzysNMrk1GfjdEJMLgKFeDRY6UpUJ1yNt, rsg67vyAxth8b4R1Ug7pWWyzv4RTATVyzJ`.
  - **Consequence:** expect small *explained* residuals in §3.4/§3.5 confined to the 2025-04→07 chunk (≤ ~131 per cell / ~250 XRP net). Any other mismatch is unexplained and must be investigated.
  - **Escalation (later same day): the 7 addresses are the tip — prod v8 stacks is missing 508 whole blocks.** Yearly `uniqExact(blockNumber)` (derived for the table_qa tests) matches exactly 2013–2024 but differs in 2025: exp +508 blocks (+29,396 txIDs, +52,734 (dt,address) changes), localized to **Jun 2025 (+472, from block 96867745) and Dec 2025 (+36, up to block 101038226)**. All 508 blocks are present in prod `xrp_balances` (v8) → source had them; the **non-batched v8 stacks job dropped them** (two ingestion incidents?). Experimental (v9) is the more complete table. **Raise with the team** — this is a pre-existing prod data loss, independent of the bucketing change.
- **§3.3–§3.5 running chunked by `dt`** (exact — `dt` is in the dedup key): 26 chunks, yearly 2013–18, half-yearly 2019–23, quarterly 2024→. Join-free prod-minus-exp formulation (UNION ALL with ±sign, one GROUP BY) to avoid FULL JOIN memory. `readonly` **can** set `max_bytes_before_external_group_by` / `distributed_aggregation_memory_efficient` — used for spill. Conservation uses a relative tolerance (`abs(d) > 1e-9·gross + 1e-6`) since full-history float jitter on native XRP can exceed the old absolute 1e-6.
- **Seam flagged-cell analysis (drilled down while sweep ran): all flags are float-ULP artifacts on absurd-scale meme IOUs; native XRP has zero flagged cells.** Flag counts grow after 2021 (4,792 cells in 2021-H2, dozens-to-hundreds elsewhere), but per-currency breakdown shows they concentrate ~entirely on `rUhCz5…/KISHU` (amounts 1e19–1e21) plus a few other joke tokens (ADVENT, one 1e96-ceiling token). Row-level proof (KISHU, dt 2021-11-03 12:16:31): prod pops four separate hour-03 installs individually; exp merged them into one entry whose float64 sum rounds at ULP(1.9e19) ≈ 2048 — all flagged |d| values (8192, 16384, 198873, 2.3e77) are ≤ ~1 ULP of the token's amount scale, i.e. relative ≤ ~1e-14. Mechanism: the merge's float sum shifts pop/liability cut points by ULP-sized slivers (some volume flips between the `odt=0` liability bucket and real hours). **Bounded by ULP — cannot cascade into whole-entry misattribution.** Native XRP (amounts ≤ 1e11, ULP ≤ 1.6e-5) is structurally incapable of tripping the 0.01 threshold and indeed never appears — so the XRP-asset metrics pipeline sees zero seam difference. Verdict: seam equivalence **holds exactly in real arithmetic and to ~1e-14 relative in float64**; only sub-ULP noise on tokens nobody computes metrics for.
- **§3.5 as written in this runbook is wrong at full-history scale — amended.** XRP IOU amounts reach ~1e80; summing `sign*amount` across currencies per `(dt, odt-hour)` cell with an absolute 0.01 threshold flags pure float noise (first pass: 52 "mismatched" cells in 2013 with max abs diff 4.4e79 but max *relative* diff 1.6e-14). Amended check: group by `(contractAddress, dt, odt-hour)` — the true downstream interface includes the asset — and flag on `abs(d) > 1e-9·gross + 0.01`. This is simultaneously stronger (no cross-currency cancellation can hide a real loss) and immune to the 1e80-scale noise.
  - Script: `scratchpad/layer1_full.sh`; results: `scratchpad/layer1_results.tsv` (session-local); findings to be folded in below when complete.
- **Layer 1 locked in as `table_qa` tests** (repeatable for future re-runs / other chains): `table_qa/test/test_xrp_stacks.py` on branch `batchStacksOdtXRP`, template `test_ltc_stacks.py`, helpers from `utxo_stacks.py` (+ new generic `get_unique_currencies_per_year`, `get_odt_misaligned_count`). Categories: blocks / txs / unique (dt,address) changes / unique currencies per year (2013–2025, old-table-derived, exact match 2013–2024; 2025 hardcoded from v9 per the 508-block finding above), hourly-`odt` alignment (must be 0), and a 16-row spot check (`rMvjSnyQyuZR7JeskfeBJjsYWhEtsfSAoB`, identical to v8 modulo hour-truncated `odt`). Run: `NEW_TABLE=xrp_stacks_experimental CLICKHOUSE_HOST=clickhouse.production.san CLICKHOUSE_PORT=30900 python3 -m pytest table_qa/test/test_xrp_stacks.py`.

### Layer 1 final verdict (2026-07-02, full sweep complete) — **PASS**

All 26 chunks × 3 checks over `W = [2013-01-01, 2026-06-01)` finished (2 conservation chunks retried after contention OOMs; final table complete, `scratchpad/layer1_results.tsv`):

| check | result |
|---|---|
| §3.3 collisions | **0 / 0 in every chunk** over 7.235B deduped keys — the ADR pre-enable gate is closed for XRP on full history |
| §3.4 conservation | **0 net mismatches in every chunk** (max relative diff ≤ 7.1e-14 — float noise) |
| §3.5 seam (per-currency) | 14,300 flagged of **1.9027B cells (7.5e-6)** — every flagged cell falls into one of three fully-explained classes below |

**The three seam flag classes (all benign for us; two are prod defects):**
1. **Float-ULP noise on absurd-scale meme IOUs** (all 2014–2024 flags: KISHU 1e19–1e21, one 1e96-ceiling token, etc.) — merged installs' float64 sum rounds at the token's ULP; every |d| ≤ ~1 ULP of the amount scale (relative ≤ 1e-14). Native XRP structurally cannot trip this (ULP ≤ 1.6e-5 < 0.01 threshold).
2. **The 2025 prod gaps themselves** — one-sided cells from the 508 blocks missing in v8 (Jun 17 / Jun 26 / Dec 2025).
3. **Prod stack-state cascade after the gaps** (the 2,935 flagged XRP cells in 2025-Q2, |d| up to 561k XRP): paired equal-and-opposite cells at the same `dt` across adjacent (or up to 2-days-apart) `odtb` hours — e.g. `2025-06-26 11:01:02` shows d = −561k @ odtb 11:00 and +561k @ odtb 10:00. Both tables record the same pop; **prod attributes it to the wrong (older) installs because its stack is missing the gap blocks' installs**. v9 (complete data) is the correct attribution. Conservation still nets to ~0 (−2.0 XRP over all flagged XRP cells).

**Consequences:**
- **Sign-off criterion §6.1 is met** — universe identical (modulo prod's own gaps, where v9 ⊇ v8), `odt` hourly everywhere, 0 collisions, 0 conservation mismatch, seam exact up to float-ULP + prod's own defects. Nothing found anywhere where v8 has data v9 lacks.
- **Escalation (stronger than the 508-block note above):** prod v8 stacks is not merely missing 508 blocks — its **LIFO state is corrupted from 2025-06-17 onward** for accounts touched by the gaps: later pops misattribute up to ~561k XRP per event by hours-to-days of age. Prod's age-based XRP metrics have been subtly wrong since mid-Jun 2025. The batched re-run *fixes* this.
- **Layer 2 guidance:** for the clean bucketing-signature comparison (§4 table), end the comparison window at **2025-06-17 00:00** — after that, prod is not a precision baseline (deviations mix the expected sawtooth with prod's gap corruption). A secondary post-gap comparison is still useful but interpret v9-vs-v8 deltas there as *including prod's error*.

### table_qa lock-in — pytest verdict (2026-07-02)

`table_qa/test/test_xrp_stacks.py`: **all 66 tests pass** against `xrp_stacks_experimental` (Opus-4.8 agent verification; one test hit a cluster-contention `MEMORY_LIMIT_EXCEEDED` mid-sweep and passed on isolated retry; runtime ~37 min + 9 min retry). No assertion mismatches — including the 2025 rows hardcoded from v9 and all 13 hourly-alignment invariant checks.

### Run + compare (when gates clear)
```bash
set -a; . ./.env.dev; set +a
cd daily_metrics && python3 main.py            # flip DAILY_DRY_RUN=false to write; uv run … once uv exists
```
Confirm on the dry run that the emitter sorts before its consumers and deltas before cumsum/composite (LTC hit a missing `dependsOn`). Then:
```bash
~/src/agentic-tasks/2026-06-ltc-stacks-deprecation/compare_ltc_experimental.py \
  --asset xrp --start 2013-01-01 --end 2018-08-01 --table all --meta-table metric_metadata
```
Expectations per §4: ~2% bounded on 1d intraday, →0 by 7–30d, daily exact, unaffected metrics unchanged.

---

## 10. Layer 2 plan — recompute metrics from the batched stacks (drafted 2026-07-03)

Layer 1 passed (§9) → the gate to Layer 2 is open. State changes since §7 was written: the backfill is **complete** (exp tails live), **`uv` is now installed** in the container (gate 2 clearable), and **PR #2270 is not merged** (origin/master tip = #2269), so the `DO_NOT_MERGE` yaml + `DAILY_JOBS` harness stays the run vehicle. Master also gained a configurable CH user defaulting to `backend` (#2269, `b17c2ec3`/`4e2cf020`) — our branch predates it; verify which user `main.py` actually connects as before writing, and prefer rebasing onto master to pick up the explicit user config.

### Phase 0 — preflight (no writes; can run before write-authorization)
1. **Provision the env:** `uv sync` at repo root (through iron-proxy); smoke-test `cd daily_metrics && python3 main.py` importability under `DAILY_DRY_RUN=true`.
2. **Re-verify the 6 `_experimental` output tables are still 0 rows** (truncated 2026-06-30 — confirm nothing wrote since).
3. **Pick the compute window.** Start stays genesis `2013-01-01` (cumsums). End: **2026-06-01** — matches the Layer-1 window, a month behind frontier, and covers both comparison regimes below. Re-run the §2.1 stability gate on the final month before computing it.
4. **Update `.env.dev`:** extend `DAILY_END_DATE` (currently 2018-08-01, a leftover of the in-progress-backfill era); keep `DAILY_DRY_RUN=true` until the run is authorized.
5. **Dry run over a short window** (e.g. 2013 only): assert (a) job order — emitter → delta consumers → cumsums → prices → composite (LTC hit a missing `dependsOn`); (b) every generated SQL reads `xrp_stacks_experimental` and writes only `*_experimental` tables (grep the dry-run SQL for table names — this is the safety assert).
6. **⛔ STOP gate: explicit operator OK for prod writes.** `main.py` connects via `clickhouse_driver` as a writable prod user, bypassing the readonly wrapper. Nothing past this line runs without it.

### Phase 1 — compute (writes only to `_experimental`)
Two operational decisions, both pre-answered by the LTC run (`ltc-stacks-deprecation.md`, 2026-06 execution log):
- **Partition guard:** 2013-01→2026-06 ≈ 161 monthly partitions on `intraday_metrics`/`distribution_deltas` (`toYYYYMM(dt)`) > the default `max_partitions_per_insert_block=100`. **Cherry-pick the LTC fix** — `max_partitions_per_insert_block: 0` on the CH client in `context.py` (local-only, proven on the LTC full backfill). Fallback: keep chunks ≤ 8 years.
- **Run shape (split, as LTC did; refined 2026-07-03 after the dry run):**
  1. **Chunked pass — emitter + prices + the 5 delta/flow jobs** (`xrp-age-distribution-intraday-deltas`, `xrp-intraday-prices`, `circulation-`/`realized-cap-intraday-deltas`, `stack-age-`/`stack-price-consumed`, `network-profit-loss`): `default_simple_job` has no cursor and recomputes the whole `[start,end]` per invocation — a 13.5y single pass will time out (LTC did over 15y). Loop `DAILY_START_DATE`/`DAILY_END_DATE` in **yearly chunks** (halve any chunk that times out/OOMs), `DAILY_JOBS` restricted to these 7. **Prices must be in the chunked pass** (not the final pass as first drafted): `network_profit_loss_job` INNER-JOINs `price_usd` read from `intraday_metrics_experimental` over its *own* window — with prices deferred, every NPL chunk would silently join against nothing. The topological sort already orders prices before NPL within one invocation (verified on the dry run: emitter → prices → … → NPL).
  2. **Single genesis-anchored pass — cumsums, then composite** (`xrp-cumulative-sums-intraday-metrics`, `xrp-cumulative-sums-intraday`, `xrp-composite-intraday-metrics`) over the full window: cumulative metrics accumulate from the first row and must not be chunked mid-history; composites (MVRV/MRP) divide cumsum outputs, so they come after. One invocation; topo sort handles the order (verified: composite sorts last).
  - **Sequencing decision (operator question, 2026-07-03): time-outer, not family-outer** — each yearly chunk computes *all* stage-1 metrics, then we move forward in time; we do NOT run one metric family over the full history before starting the next. Rationale: (a) intra-chunk dependencies (emitter→prices→NPL→deltas) are re-resolved by the framework per invocation, so each chunk is self-consistent; (b) NPL's in-window price dependency makes family-ordering a constraint anyway — time-outer gets it for free; (c) fail-fast: a systemic defect (bad redirect, wrong dedup, broken seam read) surfaces in the *first* chunk across every family, instead of after a full 12.5-year pass of family #1; (d) incremental validation: delta metrics are directly comparable to prod per-window without cumsums, so after each chunk we can diff that year across all families while the next chunk runs; (e) the chunk's seam reads (`distribution_deltas_5min_experimental` over that year) are shared by all consumers in one invocation (cache locality). Stage 2 is family-ordered by necessity (cumsums are un-chunkable; composites depend on them).
- After each chunk: per-month row count in `distribution_deltas_5min_experimental` — no gaps/overlaps at chunk seams (dt < end convention → boundaries contiguous). Re-runs are safe (ReplacingMergeTree; the compare dedups via `argMax(value, computed_at)`).

### Phase 2 — output sanity (read-only, before the verdict compare)
1. **Coverage:** per-month, per-metric_id counts in `distribution_deltas_5min_experimental` + `intraday_metrics_experimental`; continuous 2013→2026-06; only `asset_id = 2053`.
2. **Bucket signature at the metric seam:** the emitter's `value` (odt) column is hour-aligned (liability rows exempt); distinct-odt count ≈ hours in range.
3. **Conservation integral:** deduped Σ `stack_age_consumed` (daily) == Σ its `_5min` (LTC lesson: a non-deduped sum manufactures a phantom 2×).

### Phase 3 — comparison (the Layer-2 verdict)
1. **Tool:** copy `compare_ltc_experimental.py` → `compare_xrp_experimental.py` with two changes:
   - **`distribution` table join must hour-truncate prod's odt** (`toStartOfHour(value)`) and re-aggregate `sum(measure)` per `(metric_id, dt, odt_hour)` before diffing — exp cells are hourly *by design*; the LTC join on raw `(metric_id, dt, value)` would report phantom 100% divergence. This is §3.5 replayed on the metric seam table.
   - **Add p50/p99 of the symmetric rel-diff per metric** (the §4 bounds are median/p99; `mean_rel` alone is skewed by the sawtooth).
   - Keep `--meta-table metric_metadata` (prod ids; XRP adds none).
2. **Primary window `[2013-01-01, 2025-06-17)`** — per §9, prod stops being a precision baseline at the 2025-06-17 gap corruption. All §6.2 sign-off asserts run on this window only.
3. **Assert per window suffix** (the §4 table; the 1d≫7d≫30d≈0 gradient *is* the pass signature):
   | metric family | expectation |
   |---|---|
   | `*_1d` intraday (circulation, realized-cap deltas→levels) | median rel ≈ 2.1%, p99 ≲ 4.2%; exp **below** prod on circulation (odt snaps back → earlier age-out) |
   | `*_7d` | < 1% |
   | `*_30d` | ≤ ~0.14% |
   | `*_≥90d`, all-time | ≤ ~0.06% (noise) |
   | daily / `_1day` rollups (`daily_metrics_v2`) | exact (float noise only) |
   | `mean_realized_price`, `mvrv` (all windows) | level bias divides out → only the price-ASOF term, ~0.2% flat across windows (NOT ~2% on 1d) |
   | realized-cap magnitude | window bias + price-ASOF ~0.17–0.27% extra |
   | unaffected (`price_usd` intraday from `xrp-intraday-prices`) | exact |
4. **Sawtooth visual:** one sample week of `circulation_intraday_1d` prod-vs-exp at native resolution — hourly sawtooth, amplitude ≈ 4.2%, resetting each hour. This is the qualitative fingerprint of the mechanism, complementing the aggregate stats.
5. **Secondary window `[2025-06-17, 2026-06-01)` — informational, not gating:** deviations here mix the bucket sawtooth with **prod's own LIFO corruption** (up to ~561k XRP misattributions, §9); v9 is the correct side. Document, don't alarm.

### Phase 4 — sign-off + wrap-up
- Results → new §11 session log; §6.2 verdict gates enabling the bucket on the next chain.
- Escalate (separate thread, already flagged in §9): prod v8's 508 missing blocks + post-2025-06-17 LIFO corruption.
- Cleanup: PR the `table_qa/test/test_xrp_stacks.py` lock-in; delete `xrp_intraday_experimental_DO_NOT_MERGE.yaml` only if/when PR #2270 (with the additive-only flattener fix) lands; `.env.dev` stays local.

### Open decisions for the operator
1. **Prod-write OK** (Phase 0.6) — the only hard STOP. → **GRANTED 2026-07-03**, conditional on writes targeting only `_experimental` tables (verified, §11).
2. Compute end: → **operator chose 2025-06-01** (entirely clean-baseline; nothing to learn past the 2025-06-17 prod corruption). `.env.dev` updated.
3. Partition guard: → **patch applied** (local `context.py`, marked DO NOT MERGE), §11.

---

## 11. Session log — Phase 0 executed (2026-07-03)

**Phase 0 complete; all preflight asserts pass. Ready for Phase 1 (the real write run).**

- **Venv provisioned** (gate 2 closed), with container workarounds: `uv sync --native-tls` (iron-proxy MITM cert breaks uv's bundled trust store) + managed Python 3.11 (repo pins `<3.13`; system is 3.12.3, and `lru-dict==1.2.0` has no cp312 wheel). **No C compiler in the container** (no root/sudo) → `lz4==2.2.1` (metrics-hub pin, 2019, no cp311 wheel) cannot build; worked around with `uv sync --no-install-package lz4` + `uv pip install lz4>=4.3` (wheel). The lz4 shim is load-bearing: `context.py` sets `compression="lz4"` on the CH client (lz4.block API is stable 2.x→4.x). **Env-improvement note: preinstall build-essential (or a prebuilt venv) in the agent image.**
- **All 6 `_experimental` tables re-verified 0 rows** before the run.
- **`.env.dev`:** `DAILY_END_DATE` 2018-08-01 → **2025-06-01** (backfill complete; operator decision).
- **The BTC-precedent daily-price seed is NOT needed for this run.** The operator's earlier manual copy (`daily_metrics_v2` → `_experimental`, metric_ids 2,3,4,5,73,75,76,77 = daily OHLC/marketcap/volume, asset 1452 = bitcoin) served jobs that read daily prices — e.g. `prices.TotalSupplies`, which queries `context.config.daily_metrics_table` (redirected → empty experimental). **None of our 10 XRP intraday jobs read the daily table**: prices come from `asset_prices_v3` (non-redirected prod source, via `prices.Prices`), NPL/composite read `price_usd` from the *intraday* experimental table that `xrp-intraday-prices` itself populates. Confirmed empirically: the dry-run SQL contains **zero** references to `daily_metrics_v2`. If a later phase adds daily jobs, revisit with `asset_id = 2053`.
- **Dry run (2013-Q1 window) verdicts:**
  - **Job order correct:** age-distribution emitter first → prices second → circulation deltas (cancels, then deltas) → age-consumed → NPL → realized-cap deltas → price-consumed → cumsums → composite last. The LTC mis-ordering does not recur.
  - **Write surface 100% `_experimental`** (the prod-write condition): persistent INSERTs go only to `distribution_deltas_5min_experimental`, `intraday_metrics_experimental`, `intraday_delta_futures_experimental`. Reads: `xrp_stacks_experimental` (emitter), the experimental seam/intraday tables, and intended prod read-only sources (`intraday_metrics_historic_optimization` ASOF prices, `asset_prices_v3`, `asset_metadata`, `metric_metadata_versioned`, `contract_addresses`). **Zero references to bare `xrp_stacks` / `intraday_metrics` / `daily_metrics_v2` / `distribution_deltas_5min`** (grep-asserted).
  - **Ephemeral prod writes to expect** (framework-standard, not `_experimental`): scratch tables CREATE/INSERT/DROPped in the `default` db per invocation (`tmp_metric_table`, `tmp_delta_futures`, `XRPStacks_tmp_asset_mapping_*`, `Prices_tmp_asset_mapping_*`, `tmp_composite_metric_intraday_*`); plus `test.debug_cumsum_*` snapshots **only if** a negative intraday cumsum is detected (`cumulative_sum_job.py` debug hook).
  - **Two dry-run-only crashes, both artifacts of `DAILY_DRY_RUN=true`** (skipped DML → later *real read* of a never-created tmp table): the prices job (`fill_gaps_with_last_known_value` reads the asset-mapping tmp table) and the cumsum debug hook (`SELECT count() FROM tmp_metric_table`). Neither affects the real run, where the tmp tables exist. Job-level SQL was still captured for every job (jobs run/verified piecewise).
- **Partition-guard patch applied:** `max_partitions_per_insert_block: 0` in `context.py` client settings (working-tree only, commented DO NOT MERGE) — 2013→2025 spans ~149 monthly partitions, over the default-100 INSERT guard; same fix as the LTC full-history run.
- **Job→metric mapping observed** (for the comparison phase): emitter 162; circulation deltas 337–348 → cumsums 325–336; realized-cap deltas 300–311 → cumsums 288–299; age-consumed 95 (cumsum 177→178); price-consumed 103; NPL 223; prices 181; other cumsums 637→638, 801/802→803/804; composites 273–284, 312–324, 498/499, 1114.

**Next (Phase 1):** flip `DAILY_DRY_RUN=false`; stage-1 yearly chunk loop (7 jobs) 2013→2025-06, then the stage-2 genesis pass (2 cumsum jobs + composite); per-chunk seam row-count checks as we go.

---

## 12. Session log — Phase 1 stage-1 running; first-year (2013) analysis (2026-07-03)

**Run mechanics:** chunk runner `scratchpad/run_chunk.sh` (sources `.env.dev`, overrides `DAILY_DRY_RUN=false` + window + the 7 stage-1 jobs; the on-disk latch stays `true`). 2013 chunk: **178 s** for all 7 jobs (9 sub-jobs); 2014: ~7 min. Sequential loop 2014→2025-06 launched (aborts on first non-zero rc). Comparison tool: `~/src/agentic-tasks/2026-06-odt-bucketing-xrp/compare_xrp_experimental.py` (hour-truncated distribution rollup; p50/p99; verdict on median).

**Chunk-boundary note (validates time-outer ordering as *necessary*):** the futures/cancel rows written by a chunk only cover its own window — cancels from year-Y events landing in Y+1 are computed by the Y+1 chunk reading the already-present year-Y seam. Chunks must therefore run in time order.

### 2013 comparison results

- **Seam (162 `age_distribution_5min_delta`): exact MATCH.** p50 = 0, p99 = 2.3e-8, sum_ratio = 1.0 after hourly rollup. The ~795k prod-only cells all carry measure ≤ 1.3e-11 — within-hour churn nets to zero, so the merged experimental stack rightly emits no row while prod's offsetting 5-min cells roll up to a float-zero. 160 exp-only cells, max 2.5e-11 — same artifact mirrored. **Beware in one-sided-cell analyses: prod's distribution table also holds metric 270 (`age_distribution_1day_delta`), which our job set doesn't compute** — filter `metric_id` or its (huge, midnight-paired) cells masquerade as missing data.
- **Generation-drift worry retracted for 2013:** prod's 162 rows have `computed_at` Aug–Dec 2020, yet match the v9-derived seam to 1e-8 at hourly rollup → 2020-era stacks ≡ v8 ≡ v9 here.
- **price_usd (181): exact** (p99 = 7e-7) — unaffected metric confirmed unaffected.
- **age/price-consumed (95/103): integrals conserved** (sum_ratio 0.9999 / 0.9908); pointwise p50 0.4% / 0 with heavy tails — the expected within-hour timing reallocation on flow metrics.
- **Circulation deltas (337–348): the §4 gradient is already visible in the deltas** — mean_rel falls monotonically 0.86 (shortest window) → 5.6e-7 (long windows); sum_ratio 0.84 → 1.0000. Pointwise DIVERGE on the 1d delta is expected (sign-flipping 5-min deltas); levels are judged after stage-2 cumsums.
- **Realized-cap deltas (300–311):** long windows sum_ratio 0.991; shorter windows 0.61–0.84 (delta-timing artifacts + price-ASOF; judge at level stage). first_div everywhere = 2013-08-04 = the start of XRP price data, as expected.

### ⚠ Finding: network_profit_loss (223) — fully root-caused (REVISED 2026-07-03, second pass)

Observed: exp NPL ≡ exp all-time realized-cap delta **pointwise to ~10 significant digits**; sum_ratio vs prod 2013 = 24.2.

**Mechanism of today's code (correct, and subtler than it looks):** `network_profit_loss_job` computes `Σ(p_now − acq)·(−measure)` over **all** seam rows with no pops-only filter. It doesn't need one: acq is ASOF at `toStartOfFiveMinute(odt)` and `p_now` at `toStartOfFiveMinute(dt)`, and a fresh install has odt≈dt → **acq ≡ p_now exactly (same 5-min price point) → install terms cancel identically**, leaving pops-only realized P/L. Additionally, on a conserving chain (per-slot inflow = outflow), **pops-only P/L ≡ all-time rc-delta is a mathematical identity** — so prod's own current NPL equals prod's rc311 to ~1e-4 (verified on 2026-06-25 prod data). Not a bug; a theorem.

**Prod history has two regimes in dt-space** (quarterly bisect of |223−311| median rel-diff): pre-2022 ≈ 15–27% apart (genuinely distinct series); from 2022-Q1 ≈ 1e-4 (identity regime). The flip matches commits `352af466` (2022-03-09: shared source acq ASOF `toStartOfHour(odt)` → `toStartOfFiveMinute(odt)`) and `46646997` (2022-03-11: NPL price join hour→5-min). The **old** hourly-grid code is the one where installs didn't cancel cleanly (price@hour-start ≠ hourly-avg price), i.e. pre-2022 stored values ≈ P/L + hour-scale drift noise on gross inflows. Old dts were never recomputed → prod 2013 baseline = old-regime values (today's code also *cannot* recompute them from prod's seam: stored `acquisition_price` is NULL in 2020-era rows).

**Migration-relevant consequence (the real Layer-2 test for NPL):** hourly odt bucketing **breaks the exact install cancellation** — a bucketed fresh install has acq = price@hour-start ≠ p_now → exp NPL = true pops P/L **+ Σ_installs (p_now − p_hour_start)·amount**, i.e. the bucket reintroduces intra-hour price-drift noise on *gross inflow volume* into NPL (this is why exp NPL ≡ exp rc305: both carry the same install terms, consistently). Zero-mean-ish noise, but the gross-volume multiplier can rival or exceed the P/L signal on churny slots.
- **Gate accordingly:** compare exp 223 vs prod 223 only on **dt ≥ 2022-04** (both sides current-code regime; prod there = clean pops-only P/L). The deviation there is pure bucketing noise — quantify its magnitude (pointwise + daily/weekly aggregate, where drift should largely cancel) and take an accept/reject decision on NPL specifically. Pre-2022 dts: prod baseline is old-regime, not comparable — exclude.
- 2013–2021 sum_ratio numbers for 223 in the yearly tables are **not meaningful** (regime mismatch), don't read them as bucketing error.
- **Team note (softened from the earlier escalation):** NPL ≡ all-time-rc-delta on conserving chains means the metric is redundant there today; also the identity breaks on non-conserving events (mints/burns land as `p_now·Σm`). Worth a heads-up, not a defect report.

### Stage-1 progress — 2015–2022 analyzed (2026-07-03, later same day)

Chunks 2013–2022 complete (rc=0, 9/9 sub-jobs each); 2023 in flight. Chunk runtimes grow with data (2013: 3 min → 2022: tens of minutes).

**Seam (162), yearly 2015–2022: cell-level exact MATCH every year** — p50 = 0, p99 ≤ 7.6e-8 after hourly rollup.

**One-sided seam cells with real measure (2021–2022) — root-caused to prod fossil rows, NOT bucketing.** 2022 (worst): ~11.5k prod-only + ~13.2k exp-only cells > 0.01 XRP, max 1.93M, yearly net residual ≈ 1M XRP (≈1e-5 of supply). Forensics on the top cell (dt 2022-12-07 08:00:01, a ~325M XRP internal shuffle):
- **Raw v8 ≡ v9 exactly** at that second (every odt-hour cell identical to the last digit; no liability cell in either).
- **Exp seam ≡ current raw** cell-for-cell (e.g. −764,065.139 @ 2020-11-26 02:00).
- **Prod seam** carries an extra −1.93M @ odt=1970 (liability) matching *neither* current raw table; its rows were computed **2022-12-12** — at the then-frontier, from the raw generation live at that time, never recomputed after the raw table was rebuilt (v8 backfill).

Conclusion: prod `distribution_deltas_5min` history is a **generational patchwork** — frontier-time computations from whatever raw table existed then (same class as the 2013 NPL baseline and the 2020-era computed_at). Wherever prod seam ≠ current raw, **exp is the correct side** (it mirrors raw, which Layer 1 proved v8 ≡ v9). Metric-level comparisons inherit this prod noise; effect on levels is ~1e-5 relative — document, don't gate on it.

**Intraday 2015–2022 (full-span compare):** delta metrics saturate pointwise (p50 → 2 on active years — within-hour timing shifts flip 5-min delta signs constantly; expected and not gating), while **integrals hold**: sum_ratio 0.98–1.01 on short windows → 1.000–1.001 on long; both families keep the monotone window gradient (p50 falls 2 → 1e-10 across 337→348 and 2 → 0.116 across 300→311). `price_usd` p99 0.17% (minor source-revision noise in `asset_prices_v3` vs prod's frozen copies). 347/348 sum_ratio 0.947 with pointwise-exact match = telescoping-cancellation sensitivity of near-zero net sums, not divergence.

**NPL bucketing-noise quantification (clean current-code window, 2022-04-01→2022-12-31):** median symmetric rel-diff **17.5% @ 5-min → 3.1% @ daily → 1.1% @ weekly → 0.51% on the 9-month total**. Exactly the predicted zero-mean intra-hour price-drift residue on gross inflows: large pointwise, cancels under aggregation. This is the number for the product accept/reject call on intraday NPL under hourly bucketing (daily+ consumers unaffected).

### Stage 1 COMPLETE (2013→2025-06, all rc=0) + 2023–2025 seam; ⚠ new prod-defect finding (2026-07-03)

**Seam 2023/2024: exact MATCH, nets identical.** 2025 (Jan–May): cell-level exact (p99 = 3.5e-10) but a ~35M XRP one-sided net residual, fully localized: **prod's seam is MISSING pop events that prod's own raw v8 contains** — 124 cells/−12.9M in Apr, 47 cells/−22.6M in May 2025; zero prod-only cells. All top cells are −2,000,000.00 XRP pops draining the same **2013-02-17 22:00 cohort** (a 12-year-old whale moving in 2M chunks: Apr 21, May 12, May 28…). Raw spot-check at 2025-04-21 14:38:01: **v8 ≡ v9 to the last digit, both contain the pop**; prod seam has no row and no alternative attribution.
- Mechanism: live-emitter gap at the frontier (seam computed before raw rows arrived / window miss; never recomputed) — prod seam ≠ prod raw, exp ≡ raw. Same fossil class as the 2022 finding but as *omissions*, and product-visible: **prod's XRP age metrics (dormant circulation, age bands, age consumed) missed ~35M XRP of ancient-cohort movements in real time from 2025-04-21** — exactly the events those metrics exist to surface. **Escalate with the other prod findings.**
- **Comparison guidance:** the pristine baseline window for seam-derived *levels* ends **2025-04-01** (not 2025-06-17 as §9 estimated from raw alone); Apr–May 2025 carries the ~3.5e-4-of-supply omission noise on prod's side (exp is the correct side there).

Stage 2 (genesis cumsums + composite, 2013→2025-06, single invocation) launched.

---

## 13. Layer 2 verdict (2026-07-03) — **PASS** (§6.2 satisfied for the harness scope)

Stage 2 completed (cumsums 483s + 0.6s, composite 372s; 78 metric ids, 101M exp rows). Sign-off comparison on **levels** over the pristine window `[2013-01-01, 2025-03-31]` (`compare_xrp_experimental.py --table intraday`):

| family | §4 bound (median) | measured p50 | verdict |
|---|---|---|---|
| circulation 1d (325) | ~2.1% | **0.46%** | PASS (4× better than predicted) |
| circulation 7d (326) | <1% | 0.035% | PASS |
| circulation 30d (327) | ≤0.14% | 0.0063% | PASS |
| circulation ≥60d (328–336) | <0.03% | 1e-5–4e-5 | PASS |
| realized-cap 1d (288) | ~2%+ASOF | 0.53% | PASS |
| realized-cap 7d→20y (289–299) | →0 | 0.11%→0.04% | PASS |
| MRP all windows (273–284) | ~0.17–0.27% flat | 0.06–0.10% flat | PASS (windows-flat as predicted) |
| MVRV all windows (312–323) | ASOF-term only | 0.07–0.12% flat | PASS |
| mvrv_long_short_diff (324) / nvt (1114) | — | 0.41% / 0.43% | PASS |
| price_usd (181, control) | exact | p50=0, p99=0.15% (upstream price revisions) | PASS |
| age/price-consumed integrals (95/103) | conserved | sum_ratio 1.000 / 1.001 | PASS |
| seam (162), all 13 years | exact | p50=0, p99≤1e-7 | PASS |

**The heavy 1d tails are the design contract, verified mechanically.** p99 on 1d levels is 15–22% (circ) / up to 40% (rc), stable across ALL years — whale-burst cohorts, not sparse-era noise. Case study (2024-03-02, worst March spike): prod ≡ exp to 1e-13 at 17:55 → exp ages out a **1.01B XRP cohort at 18:00** (odt at hour-start) while prod ages the same cohort out 18:50–18:55 (true install seconds) → re-agree to **1e-12** at 18:55. Deviation = transient ≤55 min, exact reconvergence, zero residual. The §4 "amplitude ≈ 4.2%" row assumed uniform flow; correct statement: transient amplitude = (burst cohort)/(level), unbounded relatively, but **duration-bounded to ≤1 bucket and residual-free**. Product note for 5-min-granularity consumers of `*_1d` intraday metrics; invisible at hourly+ sampling.

**Excluded/absent from the gate:** NPL 223 (see the §12 finding + quantification); pointwise delta metrics 300–311/337–348 (saturate under timing shifts; integrals 0.99–1.01 — deltas are inputs, levels are the product); ids 177/178/637/638/801–804 (active-addresses) + 498/499 (exchange flows) — **not stacks-dependent**, deliberately outside the harness (cumsum no-op'd on them, composite NaN-dropped).

**Residual (not empirically diffed in this run):** daily metrics (analytically invariant to sub-day odt shifts, master doc §14 — no daily jobs in the harness) and the other seam consumers (`dormant_circulation`, `stack_age_bands`, `creation_timestamp*`, `supply_in_profit`, `transaction_volume_profit_loss`, `_1day` rollups) — their input equivalence is proven via the seam (exact all years); metric-level spot-checks are an optional follow-up if the team wants belt-and-suspenders before non-XRP rollouts.

**Prod-side escalation bundle accumulated during validation** (all independent of bucketing; in every forensic, exp ≡ current raw and prod's derived data was the wrong side):
1. Raw v8: 508 missing blocks + LIFO corruption from 2025-06-17 (§9).
2. Seam fossils: frontier-time rows from older raw generations, e.g. the 2022-12-07 liability misattribution (§12).
3. Seam omissions: ~35M XRP of 2013-cohort pops missing Apr–May 2025 → prod's live dormant/age metrics missed them (§12).
4. NPL ≡ all-time rc-delta identity since 2022-03 on conserving chains (§12).

**Bottom line: both §6 sign-off criteria are met for XRP.** The hourly bucket is safe to enable for the next chain per the runbook process (Layer 1 §3 on that chain's tables first, incl. the ADR §3.3 collision gate).

---

## 14. Savings quantification (2026-07-03)

Measured (ClickHouse, readonly; shard-local `system.parts` ×3 shards for cluster estimates; seam/futures tables are broker-replicated so per-replica figures multiply by replica count):

| what | prod (non-batched) | experimental (hourly odt) | saving |
|---|---|---|---|
| `xrp_stacks` rows (global, physical) | 10.168 B | 7.339 B | **−27.8%** |
| `xrp_stacks` compressed bytes (per shard) | 222.4 GiB | 170.3 GiB | **−23.4%** (≈ −156 GiB across 3 shards, × replication) |
| `xrp_stacks` uncompressed (per shard) | 488.7 GiB | 363.9 GiB | −25.5% |
| seam rows (metric 162, XRP, dt < 2025-06) | 2.147 B | 1.102 B | **−48.7%** |
| seam compressed bytes/row | 11.69 | 9.77 | −16% (coarser odt compresses better) |
| seam XRP compressed est. (per replica) | ~23.4 GiB | ~10.0 GiB | **−57%** |
| `intraday_delta_futures` rows (XRP) | 6.07 B | 2.65 B | **−56%** ¹ |

¹ prod futures counts include re-run duplicates and history outside our window — indicative, not exact. XRP is ~7.6% of the whole prod seam table (2.15 B of 28.35 B rows), so chain-by-chain rollout compounds.

Physical-vs-logical note: prod v8 carries ~39% exact-duplicate rows (job re-runs); on deduped data the stacks row reduction is ~28% (Layer 1: 10.05 B vs 7.24 B over W). The dupes are physically real, so the deployed savings are the table above.

**Flink state (front 1) — measurement protocol (savepoint comparison, agreed 2026-07-03):**
1. Trigger **full savepoints** (canonical format) on both jobs at ~the same wall-clock time — both are tailing live, so state covers the same event history; canonical savepoints rewrite RocksDB and remove incremental-checkpoint/compaction noise from the comparison. Headline = savepoint size in S3.
2. **Per-operator breakdown** via Flink REST (`/jobs/:id/checkpoints`, state size per operator/subtask): isolate the stacks keyed state (`HandlerOneAccountChange`/`ComputeAccountStackChanges…`) — that operator is where the win lives; source/sink operators should be ~equal and serve as the control.
3. **Steady-state churn**: record N consecutive incremental checkpoint sizes + durations + alignment times on both — the operational win (checkpoint stability, recovery time) is separate from resting bytes.
4. If exposed, **RocksDB `estimate-num-keys`** (or Flink state-entry metrics): live segment *count* is the purest measure of cohort merging, independent of serialization/compression.
5. Fairness: same Flink version, state backend, serializer and compression config on both jobs (true — same branch, config knob only). Compare per-operator, not only job totals, in case ancillary state differs.

**Other fronts (front 4):** Kafka output topic volume (the emit stream is the ~28% fewer rows that land in CH → topic bytes/day + retention savings; measurable by comparing topic offsets/day between the two output topics); CH insert/merge CPU and replication traffic (proportional to the row reductions); DMF consumer scan cost (every seam reader scans ~half the XRP rows — visible as faster age-metric jobs); backups/page-cache pressure (proportional).

---

## 15. Session log — RocksDB gauges enabled in both deploys + state-cleanup analysis (2026-07-03)

### Helm template analysis (devops repo, branch `batchStacksOdtXRP`)

Neither deploy exposed RocksDB native metrics; both templates have values passthroughs, no chart edits needed:
- **Old** (`hprod/k8s-apps/flink-jobs/xrp/xrp-stacks-v6`, `common-flink-chart`): the chart's `configmap-flink.yaml` lists all rocksdb metrics **commented out** (~line 256+, incl. `estimate-num-keys` at 265) — documentation only. The real switch is `.Values.flink.config` (raw multi-line string rendered into flink-conf via `| indent 4`, present in **both** branches of the chart's config conditional → reaches TMs). Prometheus reporter already configured.
- **New** (`hprod/k8s-apps/flink-jobs-operator/xrp/xrp-stacks`, `global/flink-jobs-operator/flink-job-template`): FlinkDeployment template merges `.Values.flinkConfigOverrides` (map<string,string> — values must be quoted strings) into `flinkConfiguration`; the xrp values already use the block (adaptive scheduler/autoscaler). Prometheus reporter in the template.

**Edits applied (working tree, both values files):** enabled `state.backend.rocksdb.metrics.{estimate-num-keys, estimate-live-data-size, total-sst-files-size}` — commented as temporary for the state measurement, safe to drop after. Rollout: `make install` on v6 (restarts from checkpoint), operator rolls v7 on spec change. Read `estimate-num-keys` only a day+ after rollout (fresh-backfill tombstones inflate it until compaction settles).

### Does the code clean state when an address fully drains? (asked re tombstones)

Read `HandlerOneAccountChange.scala` + `ComputeAccountStackChangesTimeWindow.scala` (branch `batchStacksOdt`):
- **Segment arrays (the heavy state): yes.** Stack = MapState `account-change-store`, fixed-size batch arrays per (contract,address,batchIndex). `decreaseArraySegmentUsed()` removes each batch as it drains; `commit()` removes the last index-0 array when `lastSize == 0`. Fully-consumed address ⇒ zero segment bytes. Pre-existing behavior, untouched by the bucketing diff.
- **`nonce` MapState entry: permanent by design.** `commit()` always writes `(lastSize, lastNonce)`, nothing ever removes it — the monotone per-address nonce must survive emptiness (resetting could collide on the raw `(contract,address,sign,dt,nonce)` key at equal `dt`; ReplacingMergeTree would silently replace history). State floor grows with distinct-addresses-ever-active, identical in both jobs.
- **Over-consumption** doesn't empty the stack — a liability segment (`ots=0`, negative remainder) persists until offset.
- **Gauge-reading implications:** two MapStates = two RocksDB column families. `nonce` CF should be ≈equal across v6/v7 → free control (mismatch ⇒ stop and root-cause). `account-change-store` num-keys counts batch *arrays* (1 per address until stack > batch size) → **understates** the merging win; `estimate-live-data-size` is the honest per-CF gauge, savepoints remain ground truth.

### Wrap-up state
- clickhouse-tables branch `batchStacksOdtXRP`: DO_NOT_MERGE yaml + context.py partition patch (both marked), `.env.dev`, table_qa tests; experimental tables populated (validated, §13).
- devops branch `batchStacksOdtXRP`: two values-file edits (above), uncommitted.
- Pending decisions/actions: savepoint comparison + gauge readout (protocol §14); prod-defect escalation bundle (§13); NPL product call (§12 quantification); PR #2270 adoption; rollout to next chains.

---

## 16. ⚠ Finding — daily metrics were never recomputed; §13 sign-off covered intraday only (2026-07-06)

**What the §13 sign-off actually measured, on inspection:** intraday metrics only. Confirmed against the live experimental tables:

| table | rows | metric_ids | assets | range |
|---|---|---|---|---|
| `intraday_metrics_experimental` | 100.98 M | 78 | XRP 2053 | 2013-01-01 → 2025-05-31 |
| `distribution_deltas_5min_experimental` | 1.102 B | **only 162** (intraday emitter) | 2053 | 2013 → 2025-05 |
| `daily_metrics_v2_experimental` | **0** | — | — | **empty** |

**Root cause:** the harness (`xrp_intraday_experimental_DO_NOT_MERGE.yaml`, §7) is intraday-only *by construction* — it re-expresses just the 10 intraday jobs. No daily job ever ran, so (a) the daily seam emitter never fired and (b) no daily consumer wrote. Prod's XRP seam holds **both** `metric_id 162` (intraday 5-min, 2.15 B rows) **and `270` (`age_distribution_1day_delta`, the daily emitter, 3.35 M rows)**; experimental has only 162. This is the same "our job set doesn't compute 270" note from §12, now understood as a *coverage gap*, not a filtering footnote.

**Daily metrics DO depend on stacks** — they read the same `distribution_deltas_5min` seam we already populated. §13's clean bill for daily rested on the §14 *analytical* invariance argument (hourly bucketing keeps `odt` within the same calendar day → `toDate(odt)` exactly preserved → day-windowed metrics cancel), never on computed numbers. That argument is sound for pure `toDate(odt+period)` cancellation but is **unverified** and does not self-evidently cover `creation_timestamp`, `supply_in_profit`, `spent_coins_age_bands`, or the `_1day` rollups. **§6.3 (added this session) now makes the daily comparison a required empirical gate.**

**XRP daily stacks chain to run** (generic label-selected cronjobs in `specs.d/cronjobs/*`, `xrp-*` entries; selector `stacksMetrics + xrp`, `distributionFunction: distribution_deltas.XRPDistribution[WithAcquisitionPrice]`), in dependency order:
1. **Emitter:** `xrp-age-distribution-1day-deltas` (`age_distribution_1day_job`) → writes `metric_id 270` into `distribution_deltas_5min_experimental`.
2. **Consumers:** `xrp-circulation-deltas`, `xrp-realized-cap-deltas`, `xrp-dormant-circulation`, `xrp-stack-age-consumed`, `xrp-stack-price-consumed`, `xrp-creation-timestamps-deltas` (+`-intervals`, `-dollar-…-intervals`), `xrp-spent-coins-age-bands`, `xrp-transaction-volume-profit-loss`, `xrp-supply-in-profit`.
3. **Rollups/composites:** `xrp-cumulative-sums`, `xrp-composite-metrics`, `xrp-composite-delta-metrics` (daily `mvrv`/`mrp`).
   - Same run vehicle as intraday: add these to `DAILY_JOBS`; they need the daily-metrics writes redirected to `daily_metrics_v2_experimental` (verify `.env.dev` covers the daily table) and the daily emitter must precede its consumers. Excluded (not stacks-dependent): DAA/network-growth/payment-count/transaction-count/-volume/whale/exchange-supply/holders-distribution/std-dev/daa-divergence.

**Status change:** the XRP sign-off is **INCOMPLETE** against the amended §6 — §6.1 (Layer 1) and §6.2 (intraday) hold; **§6.3 (daily coverage) is unmet** until the daily chain is computed into `daily_metrics_v2_experimental` and diffed against prod. Expectation per §4: daily deviations exact within float noise — but this must be *shown*.

**Next action:** run the daily chain above (yearly-chunked like stage-1, then genesis cumsum/composite), Phase-2 coverage check on `daily_metrics_v2_experimental`, then extend `compare_xrp_experimental.py` to diff the daily table per metric_id and report the daily metric count + max deviation.

### 16.1 Daily run config (drafted 2026-07-06)

**Redirect coverage — already complete.** `.env.dev` sets `DAILY_DAILY_METRICS_TABLE=daily_metrics_v2_experimental` **and** `DAILY_DELTA_FUTURES_TABLE=daily_delta_futures_experimental` (plus the seam + dt-optimization redirects). **No new redirect needed** — only `DAILY_JOBS` changes; window (2013-01-01 → 2025-06-01) and `DAILY_DRY_RUN` latch stay as-is. The `DAILY_SOURCE_TABLES` (raw `xrp_stacks`→experimental) redirect is a **no-op for daily** — no daily job reads raw stacks; the `1day` emitter and all consumers read the *seam* (`distribution_deltas_5min_experimental`, already redirected). Harmless to leave set.

**No `DO_NOT_MERGE` hack needed (unlike intraday).** All 15 daily jobs are real `kind: DailyMetricsCronJob` docs in `specs.d/cronjobs/*` (label-selected `stacksMetrics + xrp`) → directly loadable by `main.py` + `DAILY_JOBS`. Reuse `scratchpad/run_chunk.sh` with the job lists below.

**Seam dependency map (verified in code, 2026-07-06):** `XRPDistribution`/`…WithAcquisitionPrice` both default to `age_distribution_5min_delta` (**162, present**) — so every consumer except one rolls up the existing 5-min seam. **`supply_in_profit_job` alone reads `age_distribution_1day_delta` (270, ABSENT)** → the `1day` emitter must run first. Acquisition-price consumers (`realized-cap`, `stack-price-consumed`, `transaction-volume-profit-loss`, `supply-in-profit`) use the prod read-only ASOF source (stacks-independent), *not* the daily table.

**Stages (separate invocations — none of the daily jobs declare `dependsOn`, so do NOT rely on intra-invocation ordering; mirror the intraday stage split):**

- **Stage A — `1day` emitter, yearly-chunked** (reads 162 → writes 270 into `distribution_deltas_5min_experimental`):
  `DAILY_JOBS=xrp-age-distribution-1day-deltas`
- **Stage B — daily delta/flow consumers, yearly-chunked** (read 162/270 → write `daily_metrics_v2_experimental` + `daily_delta_futures_experimental`):
  `DAILY_JOBS=xrp-circulation-deltas,xrp-realized-cap-deltas,xrp-dormant-circulation,xrp-stack-age-consumed,xrp-stack-price-consumed,xrp-creation-timestamps-deltas,xrp-creation-timestamps-deltas-intervals,xrp-dollar-creation-timestamps-deltas-intervals,xrp-spent-coins-age-bands,xrp-transaction-volume-profit-loss,xrp-supply-in-profit`
- **⛔ Seed gate (before Stage C) — daily prices.** `composite_metric_job` reads its deps `FROM daily_metrics_table` (= the empty experimental daily table); daily MVRV/MRP need daily `price_usd` (+ marketcap) that no stacks job produces. Seed from prod (operator write, same class as the BTC precedent seed, §11):
  ```sql
  INSERT INTO daily_metrics_v2_experimental
  SELECT * FROM daily_metrics_v2
  WHERE asset_id = 2053 AND metric_id IN (2,3,4,5,73,75,76,77) AND dt < '2025-06-01';
  ```
  (Confirm the exact ids the composites declare as `free_dependencies` on the dry run; the 8-id bundle is the safe superset. Prod XRP has all 8, 2009→2025-05.)
- **Stage C — genesis-anchored, single pass** (cumsums must start at genesis; composites divide cumsum outputs → run after):
  `DAILY_JOBS=xrp-cumulative-sums,xrp-composite-metrics,xrp-composite-delta-metrics`

**Dry-run asserts (before flipping `DAILY_DRY_RUN=false`), extending §11:**
1. Every generated SQL reads only experimental/intended-prod-read-only tables and **writes only `daily_metrics_v2_experimental` / `daily_delta_futures_experimental`** (grep the dry-run SQL — the safety assert).
2. **Ordering:** confirm the topo sort places `xrp-age-distribution-1day-deltas` (270 producer) before `xrp-supply-in-profit` (270 consumer). If it does *not* (the 270 edge is a raw-SQL `dictGet`, may be invisible to the factory graph), Stage A being a separate prior invocation already guarantees it — so keep A and B split, do not merge.
3. Partition guard: the `max_partitions_per_insert_block: 0` context.py patch (§11) is global on the CH client → covers `daily_metrics_v2` (~149 monthly partitions) too. No extra change.

**Then:** Phase-2 coverage on `daily_metrics_v2_experimental` (per-month, per-metric_id counts; only asset 2053; continuous 2013→2025-06), and extend `compare_xrp_experimental.py` with a `daily_metrics_v2` diff path (per metric_id p50/p99 + max abs/rel deviation) on the pristine window `[2013-01-01, 2025-03-31]`. Expectation per §4: **exact within float noise** — report the daily metric count and the worst deviation; any non-noise daily deviation is a finding.

### 16.2 Dry-run verdict (2026-07-06) — all preflight asserts PASS; staging simplified to 2 stages

Prices seeded by operator (asset 2053, all 8 ids, full history — confirmed present). Dry run: full 15-job daily chain, window 2013-01-01→2013-04-01, `DAILY_DRY_RUN=true`, **exit 0, no crashes** (cleaner than the intraday dry run, which hit tmp-table read crashes — the daily chain doesn't). Log: `$CLAUDE_JOB_DIR/tmp/daily_dryrun.log` (session-local).

- **Write surface = 100% experimental** (the prod-write safety condition): persistent INSERTs go only to `daily_metrics_v2_experimental` (78), `daily_delta_futures_experimental` (24), `distribution_deltas_5min_experimental` (1, the 270 emitter). Ephemeral scratch (framework-standard): `tmp_metric_table`, `tmp_delta_futures`. **Zero** INSERT/FROM/JOIN against any bare `daily_metrics_v2` / `distribution_deltas_5min` / `intraday_metrics` / `xrp_stacks` (grep-asserted).
- **Read surface** = `distribution_deltas_5min_experimental` (seam, incl. `acquisition_price` for the WithAcquisitionPrice consumers → no `intraday_metrics_historic_optimization` needed), `daily_metrics_v2_experimental` (composite deps + delta reads), `intraday_metrics_experimental` (P&L-family `price_usd`, like NPL §12), + read-only prod `metric_metadata_versioned` / `asset_metadata`. All redirected correctly.
- **Ordering proven in a SINGLE invocation** — the topo sort placed `xrp-age-distribution-1day-deltas` (produces **270**) before `xrp-supply-in-profit` (consumes 270), and `xrp-cumulative-sums` before `xrp-composite-metrics`. The 270 edge and the cumsum→composite edge are both caught by the factory graph. **So the emitter does NOT need a separate prior invocation** — the "keep A and B split" caveat above is retired.
- **Seed confirmed used:** the composite reads price ids **2, 73, 75** (`daily_closing`/`avg_price_usd`, `avg_marketcap_usd`) from `daily_metrics_v2_experimental`. The 8-id seed is a sufficient superset.

**Revised staging (mirrors the intraday stage-1/stage-2 split exactly):**
1. **Stage 1 — chunked yearly 2013→2025-06** (`DAILY_START_DATE`/`DAILY_END_DATE` loop; halve any chunk that OOMs/times out): emitter + all 11 delta/flow consumers **in one `DAILY_JOBS` list** — ordering is handled per-invocation, and each yearly chunk is self-contained (its emitter writes that year's 270 before its supply-in-profit reads it; cross-year cancels are computed by the later chunk reading the earlier seam, as intraday §12).
   `DAILY_JOBS=xrp-age-distribution-1day-deltas,xrp-circulation-deltas,xrp-realized-cap-deltas,xrp-dormant-circulation,xrp-stack-age-consumed,xrp-stack-price-consumed,xrp-creation-timestamps-deltas,xrp-creation-timestamps-deltas-intervals,xrp-dollar-creation-timestamps-deltas-intervals,xrp-spent-coins-age-bands,xrp-transaction-volume-profit-loss,xrp-supply-in-profit`
2. **Stage 2 — genesis-anchored single pass 2013→2025-06** (cumsums un-chunkable; composites divide cumsum outputs → after):
   `DAILY_JOBS=xrp-cumulative-sums,xrp-composite-metrics,xrp-composite-delta-metrics`

**Daily metric_ids in scope for comparison** (from the dry-run job headers): circulation 21–32, realized-cap 49–60, stack-age-consumed 8, stack-price-consumed 102, dormant 405–412/788/789, creation-timestamp 90/173 (+intervals 1320–1325, dollar 1330–1335), spent-coins-age-bands 1254–1265, tx-volume-P/L 1203/1204, supply-in-profit 786, composite-delta 98; cumsum levels + daily composites (MVRV/MRP families) per the Stage-2 headers. **Only gate remaining: operator OK to flip `DAILY_DRY_RUN=false`** (fresh prod writes, `_experimental`-only — verified above).

### 16.3 Why prod is not a valid daily baseline — fossil root-cause, localized (2026-07-06)

**Question raised:** the transactions are immutable, so a 2015 metric *should* recompute to the same value regardless of when/what code — where does prod's stored value actually diverge from a fresh recompute? Localized it to a single node with a controlled experiment (all read-only; daily run already complete):

**The pipeline is deterministic for non-price metrics.** Recomputing `stack_age_consumed` (8) *today* from **prod's own current seam** reproduces prod's **stored** value **exactly — `recompute/stored = 1.000`, every year 2013–2024** (p50=p99=0). `computed_at` shows the seam (162) and the metric are the *same generation* (both frozen ~2020–2021; seam per dt-year: 2013→2020-08 … 2020→2021-01, then frontier-time onward). So where nothing changed, "semantically identical" **holds** — prod fossils are faithful to their inputs. This is the control.

**The divergence is isolated to the acquisition-price step, and it's a dated methodology + architecture change — NOT the bucketing.** `stack_price_consumed` (102) is byte-identical to metric 8 except one factor:
- 8:  `sum(−amount · (dt−odt))/86400`  → reproduces prod at **1.000**
- 102: `sum(−amount · **acquisition_price** · (dt−odt))/86400` → reproduces prod at **0.000** every year 2013–2024

The only structural difference is `× acquisition_price`, so the entire 102 non-reproducibility is attributable to acquisition-price. Root cause, two stacked changes:
1. **Resolution:** commit `352af466` (2022-03-09, "Add transaction profit loss metrics") changed the ASOF grid `toStartOfHour(odt) → toStartOfFiveMinute(odt)`; `46646997` (2022-03-11) reworked the NPL/price-consumed/tx-volume price joins. Hourly-avg acquisition price → 5-min-avg.
2. **Architecture:** acquisition price moved from *on-the-fly* (the 2022 join to intraday prices, inside the metric job) to a *precomputed seam column*. That column is **NULL for 100% of prod's historical seam rows** (measured: 0 of 18.9M in 2018; exp seam is 99.9% populated), so current code cannot rebuild the price metrics from prod's seam at all (hence `recompute/stored = 0`; 2025 = 0.54 as the column only started being persisted recently). Prod's stored 102 was produced by the old on-the-fly hourly path (computed 2020–2021) and frozen.

**Scope of the fossil:** exactly the acquisition-price family — `realized_cap` → `mean_realized_price`/`MVRV`, `stack_price_consumed`, `network_profit_loss`, `transaction_volume_profit_loss`, `supply_in_profit`. Every **pure-age** metric (circulation, dormant, age-consumed, creation-timestamp, age-bands) is fossil-free and reproduces exactly; its exp-vs-prod gap is bucketing-only (~1e-4). This matches the earlier contaminated prod-diff (metric 8 p50 6.3e-5 vs 102 p50 1.2e-3 vs P&L ~90%).

**Consequence (the reason for the `_nb` baseline):** prod's stored **price-weighted** daily metrics are fossils of a pre-2022 methodology and are *not reproducible* without re-deriving acquisition price. So prod stored values cannot serve as the bucketing baseline for that family. The clean comparison must recompute both batched and non-batched sides under **today's** code — the `_nb` recompute (§16.2 discussion). Pure-age daily metrics *can* be compared against prod directly (they're deterministic), but for uniformity the `_nb` set covers both.

**Team one-liner:** *Non-price stack metrics recompute from prod's own seam bit-exactly (proven on `stack_age_consumed`, 0 divergence all years). Price-weighted metrics don't (0.000) — the acquisition-price computation changed resolution (hourly→5-min, commit `352af466`, 2022-03-09) and moved on-the-fly→seam-column (NULL for all historical prod rows). Prod's historical price metrics are pre-2022-methodology fossils; that's why the bucketing baseline must be a fresh non-batched recompute, not prod.*

### 16.4 Third mechanism — PR #2132 INNER-JOIN data loss (CONFIRMED 2026-07-06); corrects the §12/§13 "frontier gap" attribution

A third, distinct root cause, surfaced by the team (PR [#2132](https://github.com/santiment/clickhouse-tables/pull/2132) "Use LEFT ASOF join instead of INNER"):

**Bug:** the seam emitter attached acquisition price via an **INNER JOIN** between stack cohorts and the price grid on `acquisitionTime = toStartOfFiveMinute(odt)`. A cohort whose acquisition 5-min bucket has **no price row is dropped entirely** (measure and all) — silent source-data loss, not a staler price. Fix = `LEFT ASOF JOIN` to the nearest prior price.
- **Introduced:** `84ec2023` "Add acquisition price to distribution deltas", **2025-03-19**. **Fixed:** `d678ad9a` / #2132, **2026-03-02**. Live ~11.5 months (NOT "a few years"). Affects only price-carrying seam rows **computed** in that window (frontier dts ~2025-03→2026-03, plus any backfill run then).
- **Our exp run has the fix:** branch `batchStacksOdtXRP` contains the merge; XRP's emitter `age_distribution_intraday_job` uses `LEFT ASOF JOIN` (line ~126). So exp is clean; prod seam rows computed in the buggy window are not.

**Systematic footprint:** drops any cohort whose acquisition time has no price — i.e. everything acquired **before the asset's price-data start** (XRP: **2013-08-04 18:50**). For XRP that's the huge genesis/pre-price cohort; every pop of those ancient coins processed during the buggy window vanished from the seam.

**Confirmation (measured 2026-07-06):** seam rows for dt ∈ [2025-04-01, 2025-06-01), cohorts with `odt < 2013-08-04`:

| seam | cells | Σ\|measure\| |
|---|---|---|
| exp (LEFT ASOF, fixed) | **197** | **35,481,653** |
| prod (INNER, buggy window) | 11 | 10,846 |

Prod dropped ~186 cells / **35.48M XRP** of pre-price-cohort movement — **this is precisely the "~35M XRP of 2013-cohort pops missing Apr–May 2025" that §12/§13 flagged.**

**⚠ Correction to §12/§13:** those sections attributed the Apr–May 2025 seam omissions to a *"live-emitter frontier gap (seam computed before raw rows arrived / window miss)."* **That is wrong — the cause is PR #2132's INNER JOIN** dropping pre-price-data cohorts. The raw table had the rows (§12's spot-check: v8 ≡ v9, both contain the pop); the seam emitter dropped them at the price join. (The §9 *508-missing-blocks* + *LIFO corruption from 2025-06-17* remain a separate, raw/Flink-level defect — that one really is upstream of the seam.)

**Implications:**
- **Prod-wide, all chains:** any price-carrying seam / age-price / realized-cap / dormant / supply-in-profit value computed 2025-03-19→2026-03-02 is missing pre-price-data cohorts. Historical data computed in that window stays wrong until recomputed. Escalate alongside the §13 bundle (this one is *our* bug, already fixed forward, but historical values need a rebuild).
- **`_nb` baseline design:** reading prod's seam as the non-batched baseline **inherits #2132's drops for any dt computed in the buggy window**. Safe for the pristine window `[2013, 2025-03-31]` (those seam rows were computed pre-2025-03 per `computed_at`); for anything later the `_nb` seam must be **recomputed from raw with the fix** (gold-standard path). The pristine window already ends before the damage, so the primary Layer-2 comparison is unaffected.

**Three-mechanism taxonomy (all dated, all distinct):**
| # | mechanism | commit / date | what it explains |
|---|---|---|---|
| 1 | acq-price resolution + on-the-fly→seam-column | `352af466` 2022-03-09; `84ec2023` 2025-03-19 | historical price-family fossils (recompute 0.000) |
| 2 | INNER→LEFT ASOF (drops pre-price cohorts) | `84ec2023` 2025-03-19 → #2132 2026-03-02 | 2025 seam omissions (~35M XRP), pre-price genesis cohorts |
| 3 | raw v8 Flink defect (508 blocks + LIFO) | ~2025-06-17 onward | raw-level pops absent from balances-confirmed source |

## 17. Session log — fossil reproduction, baseline decision, daily Layer-2 verdict (2026-07-14)

Session goals: (1) prove what produced the fossils; (2) decide HEAD-vs-fossil accuracy, accept a baseline; (3) diff the batched daily metrics against it. All three closed this session. Artifact: `phaseA_daily_exp_vs_prod_2026-07-14.tsv` (per-metric diff, 190 shared ids).

### 17.1 Goal 1 CLOSED — fossils reproduced EXACTLY with the 2019–2022 methodology

Fossil-era code (commit `0cbcd503`, 2020-12): acquisition price was attached **on-the-fly** inside `DistributionBase(with_price=True)` — `toStartOfHour(odt)` grid, `avg(price_usd)` per hour from `intraday_metrics`, **INNER JOIN**. Two corollaries:
- **Pre-price cohorts were dropped from price metrics since 2019** — the INNER-drop semantics predate #2132 by six years; #2132's bug era (2025-03→2026-03) merely moved the same semantics into the seam emitter. The 2026-03 LEFT-ASOF fix changed genesis-cohort *methodology* for the first time ever (drop → include at price 0).
- Controlled reproduction (read-only, prod's own seam + old SQL): `recompute/stored` for `stack_price_consumed` (102) = **p50 1.000000000 every year 2013–2021** (integral ≤1.003); 2022–24 (stored under 5-min-era code) p50 ~1e-4 / p99 ~0.6% = precisely the hourly↔5-min resolution difference. Together with §16.3's metric-8 control, the fossils are *fully explained*: prod's stored price-family values = same seam × hourly-avg on-the-fly INNER-JOIN acquisition price.
- XRP's hourly price grid is complete from 2013-08-04 on (18 gap hours total, all 2013) → for XRP the INNER-drop reduces to genesis-cohort-only.

### 17.2 Goal 2 DECIDED — master HEAD accepted as baseline, fossils rejected

Verdict on four axes: (1) **reproducibility** — fossils cannot be regenerated by any current code path (prod seam `acquisition_price` NULL historically); (2) **resolution** — 5-min strictly refines hourly (measured: p50 ~1e-4, p99 ~0.6% on daily 102); (3) **no silent data loss** — LEFT ASOF never drops cohorts; (4) **genesis treatment** — the only contested axis: fossil silently drops, HEAD includes at cost basis 0. For sum-style metrics (realized cap, 102) ×0 ≡ dropped, numerically identical. Divergence confined to P&L classifiers (223/1203/1204/786/787). Genesis exposure quantified (exp seam): pre-price cohorts = **98% of 2013 / 46% of 2014 consumed volume**, <1% from 2016; age-integral share up to 37% (2020). Cost-basis-0 for a premine is economically defensible and explicit; the fossil's treatment is the same 0 minus the volume rows. → **Flag 223/786/787/1203/1204 pending the [genesis-acquisition-price-valuation] product call; never prefer the fossil.**

Baseline design adopted: **pure-age family → prod stored directly** (bit-exact reproducible, proven); **price family → fresh nb recompute under HEAD semantics** (prod seam raw odt + emitter-emulated 5-min LEFT ASOF acq price, in-query — no `_nb` tables/DDL needed on the pristine window `[2013-01-01, 2025-03-31]`).

### 17.3 Goal 3 EXECUTED — daily Layer-2 comparison (pristine window)

**Phase A (exp vs prod, all 190 shared ids — TSV in task dir):**
- **Pure-age family** (circulation levels/deltas, dormant, creation-timestamps, age-consumed, spent-coins bands, liveliness): p50 0–6e-5, yearly integrals 1±2e-4 → **daily bucketing deviation ≈ intraday findings; PASS**. Tails are (a) relative error on near-zero deltas, (b) band-boundary day flips (odt moves ≤59.9min across an age-band edge) — §4-predicted. `liveliness`/`cumulative_age_consumed` p99 0.96 is early-2013 near-zero-level artifact; per-year integrals = 1.0000.
- **Realized-cap / mean-realized-price levels + hodl-waves**: p50 0.06–0.11%, integrals ~1.0003 — fossil gap at level scale is sub-0.1%.
- **Anomaly triage** (three classes, all resolved):
  1. *Genesis-localized* (mvrv 2013–14, supply-in-profit 2014–15): product-call scope, not bucketing. **BUT** most of MVRV's early gap turned out to be a **prod defect**: `daily_marketcap_usd` (783) has **frozen zero days — 150 in 2013, 319 in 2014** — composite computed 2023-02-09, its price input recomputed 2023-02-27, never refreshed (stale-composite; composite `computed_at` < input `computed_at`). Exp matches prod to 3e-7 where prod is nonzero; MVRV/61–72, 97, nvt-family pre-2015 contaminated in prod.
  2. *Windowed dollar-age family* (1330–1353): **prod side is garbage** — mean-age-of-90d-window values up to **6,583 days** (invariant: ≤90); backfilled after acq-price moved to the (NULL) seam column. Exp values sane (≤84d). New prod-defect for the escalation bundle.
  3. *tx-volume-P/L (1203/1204/1205)*: **genuine bucketing casualty** — see Phase B.

**Phase B (exp batched vs nb non-batched, both HEAD semantics):**
- **`stack_price_consumed` (102), per-year 2013–2024**: p50 0.08–0.25%/day, p99 1–2.6%, yearly integrals within ±1.2% (mostly ±0.3%). This is the price-family bucketing cost: ~20× the pure-age deviation, driven by acquisition price sampled at hour-start vs 5-min-of-odt. Bounded and integral-conserving → acceptable.
- **`transaction_volume_profit/loss` — FAIL under bucketing.** 2019 full-year: exp replica 14.53B ≈ exp stored 14.42B (replication verified); **nb all-legs ≡ nb consumption-only = 71.65B exactly** (proves the invariant: on raw odt, addition legs have acq price = current price → never classify); prod stored 70.9B ≈ nb (prod's 2022 backfill is near-HEAD here, not fossil junk). **Batched loses ~80% of classified profit volume** — addition legs carry hour-start acq price vs 5-min current price, so every intra-hour price move injects `-amount` misclassifications. A consumption-only (`amount<0`) job fix recovers to 53B (still −26%: knife-edge classification of the young consumed cohort amplifies ≤59-min acq-price staleness). Phase-A per-year ratios (0.16–0.52) show all years affected. → **Hold 1203/1204/1205 from the batched rollout pending job redesign or acceptance decision; `supply_in_profit` (786/787) is the mild cousin (standing supply, old cohorts off the knife edge): persistent ~0.3% — tolerable, flag in ADR.**

**Incidental finding (corrected 2026-07-15 — it's ALL chains, not just XRP):** `distribution_deltas_5min` stores EVENT-resolution `dt`/`odt`; there is no 5-min pre-aggregation before insert (measured 2019-06-12 prod: BTC ~100%, ETH 66%, ADA ~100%, XRP 90% of rows not 5-min aligned; exp XRP 98.8%). The emitter's only aggregation is `GROUP BY (asset_id, dt, odt)` at raw stacks resolution — it collapses the *address* dimension, not time; `toStartOfFiveMinute` appears only transiently on `odt` for the acquisition-price lookup. The "5min" in the table/metric name is the consumer-side contract (price-attach grid, intraday output slots), upheld by consumers truncating at read time. Any comparison/query joining the seam to a 5-min price grid MUST `toStartOfFiveMinute(dt)` first (this produced a false 48× discrepancy mid-session before being caught).

### 17.4 §6.3 verdict + escalation additions + next actions

**§6.3 daily gate: PASS with two carve-outs.** Pure-age daily: pass (≈1e-4). Price-family levels/flows: pass (≤0.25% median, integrals ±1.2%). Carve-outs: (a) 1203/1204/1205 fail under bucketing — needs an eng/product decision (consumption-only fix + accept −26%, bucket-aware acq price, or exclude family); (b) genesis-classifier family (223/786/787/1203/1204) additionally gated by the genesis-valuation product call — orthogonal to bucketing.

**Escalation bundle additions (prod defects, exp correct side in each):** windowed dollar-age family garbage (impossible values, all history); `daily_marketcap_usd` zero-days 2013–14 → MVRV/nvt contamination pre-2015; the stale-composite pattern (composites not recomputed when inputs are re-backfilled) is systemic — detectable as `composite.computed_at < input.computed_at`.

**Next:** (1) team decision on tx-volume-P/L under bucketing (present §17.3 numbers); (2) fold verdicts into the ADR + product note; (3) escalation write-up; (4) Flink savepoint/RocksDB gauge readout (§15, still pending); (5) next-chain rollout unblocked for everything except the carve-outs.

## 18. Evaluation — proposed 5-min/5-min batch redesign (fix for §17 carve-out 1) (2026-07-15)

Proposal (operator): drop hourly odt cohorts; use **5-min odt cohorts AND 5-min-aggregated dt** (Flink becomes a 5-min micro-batch job; only 5-min-aligned (dt, odt) pairs leave the stacks).

### 18.1 Carve-out 1: FIXED — exactly, not approximately

Analytical: acquisition price is sampled at `toStartOfFiveMinute(odt)`, which is invariant under 5-min odt bucketing → consumption-leg classification bit-equal to unbatched; addition legs get odt-bucket == dt-bucket → acq ≡ current price → never classify (invariant restored). Empirical (2019 full-year sim on prod seam, HEAD formula): **sim-5min/5min profit 71.646B / loss 93.293B = EXACTLY the nb baseline**, all-legs ≡ cons-only again; hourly control sim reproduces the damaged exp numbers (loss 19.217B = exp replica to 3 decimals — also validates sim methodology). `stack_price_consumed` 0.1–0.25% and `supply_in_profit` 0.3% deviations collapse to ≈exact too (same acq-price-sampling root). All age errors go ≤1h → ≤5min. Flink-internal LIFO merge not captured by sim, but Layer-1 proved seam-equivalence for hourly merging; 5-min is strictly finer.

### 18.2 Space evaluation (measured on prod v8 / exp, XRP)

**ClickHouse storage — proposed design BEATS hourly** (dt is the high-cardinality axis; ~75 XRP ledgers per 5-min):

| rows, 2024-06 sample | stacks | seam (162) |
|---|---|---|
| unbatched v8 | 63.9M | 19.8M |
| hourly design (sim / actual exp) | 42.9M / 45.9M | 9.71M / 9.70M |
| 5min/5min (sign kept / sign-netted) | **15.0M / 10.0M (−77/−84% vs v8)** | **2.6M (−87% vs v8)** |

**Flink RocksDB state — segment multiplier ~2×, not 12×** (odt-driven only; dt batching doesn't touch state): distinct (address, odt-bucket): raw 1.005B / 5-min 352M / hourly 182M all-time → 5-min = **1.94× hourly** (2.13× on 2024 alone), still **2.85× below unbatched** (hourly was 5.53×). nonce CF unchanged; 5-min output buffer negligible. Calibrate against §15 RocksDB gauges when read.

**Latency:** micro-batch adds ≤5min output delay — matches the intraday 5-min cadence; frontier consumers see one-bucket lag.

### 18.3 ⚠ NEW COST — same-bucket netting hits the circulation family

dt-aggregation nets coins acquired AND spent within one 5-min bucket to zero in the (B,B) seam cell. Measured on prod seam: **5.0% of consumed volume 2024-06, 15.2% 2019-06**. Unaffected: P/L family (those legs classify as neither profit nor loss in unbatched too — contributes 0), age/dollar-weighted metrics (weight ≤5min ≈ 0). Affected: **entire `stack_circulation_*` family (uniform absolute loss across all windows — any consumption with age<window counts), `spent_coins_age_band_0d_to_1d`** — a material definition change of its own (would fail the same standard as carve-out 1). Note: gross-per-sign seam rows can't fix it in the current schema (no sign in seam ORDER BY key → Replacing collision).

**Options:** (a) product blesses it as "sub-5-min self-churn removal" (path-payments/AMM hops); (b) seam schema change to carry gross flows; (c) **variant D: 5-min odt, dt UNBATCHED** — fixes carve-out 1 identically, zero flow-metric impact, keeps the full 2.85× Flink-state win, but forfeits the CH storage win (stacks/seam rows ≈ unbatched). Decision needed on which axis (state vs storage vs definitions) dominates.

### 18.4 Variant D evaluated (5-min odt, dt untouched) — measured worth-it verdict (2026-07-15)

**It's a config flip, not a project:** ADR knob `Config.stacksOdtBucketMs` = 300000 (vs current 3600000); `dt` is never bucketed by design; merge invariant is bucket-size-agnostic; per-deploy (per-chain) choice.

**Metric integrity — effectively exact:** `stack_age_consumed` 2019 sim: p50 relerr **2.7e-6** (hourly: 4.1e-5), p99 5e-5, integral 0.999999. P/L family bit-exact (§18.1), no flow netting (dt untouched), ADR's ~2% 1d-window bias → ~0.17%. All §17 carve-out-1 damage gone; only genesis (orthogonal) remains.

**Space (measured):**

| vs unbatched v8 | hourly (current branch) | variant D (5-min odt) | D retains |
|---|---|---|---|
| Flink LIVE segments (1% addr sample, dt<2025-06) | −48% (raw/1.91) | **−27%** (raw/1.36) | 55% of win |
| stacks rows (2024-06) | −33% | **−31%** | 93% |
| seam cells (2024-06) | −51% | **−35%** | 69% |

Live-segment absolutes (×100 fleet est.): raw ~130M, 5-min ~95M, hourly ~68M; live addresses ~6.5M; live segments/address: 20.0 raw → 14.7 (5-min) → 10.5 (hourly). NOTE: live ratios are much smaller than the all-time segment-universe ratios (§18.2: 2.85×/5.53×) — live state is dominated by dormant addresses whose acquisitions are temporally spread (bucketing can't merge them); the all-time universe overweights churny consumed segments. The §15 RocksDB live-data-size gauges remain the byte-level calibration. Futures not re-measured (day-granularity preserved → expected ≈ hourly's).

**Verdict:** vs unbatched, variant D still wins meaningfully on every axis at ~zero metric cost; vs hourly it trades ~half the live-state win and a third of the seam win for the complete elimination of carve-out 1 (and every other bucketing deviation). Since the knob is per-chain, hourly can remain the setting for state-desperate chains (bot/MEV L2s per ADR) where holding the P/L family is acceptable.
