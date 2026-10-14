# Analysis of a comparison (recipe for the agent)

You are given the output of `compare.py` for one config, `results/<config>/`. Your job: find why old and new differ,
decide which side is right, and write it up so that a reader who has not seen the investigation can decide whether
new may replace old.

Invoke as: *"Analyse `results/<config>/` following `scripts/analysis/RECIPE.md`."* Read, before you start:
- this file, `catalog.md` (known causes), `template.md` (the outputs), and the playbook of the table kind in
  `playbooks/` (`stacks`, `balances`, `transfers`, `metrics`);
- `../compare.md` (what `compare.py` computes, the month file format, how to list the keys of a cell) and the
  config in `../configs.py`.

## Ground rules

1. **Prod is read-only.** `SELECT` only, never DDL/DML. At most **2 long-running queries** (> ~1 min) at a time, and
   none while a `compare.py` run is going. Small probes alongside are fine. Use the `santiment-clickhouse-query`
   skill for access; the other `santiment-*` skills (metrics navigation, Kafka source, Airflow) as the playbook says.
2. **Back every conclusion with evidence**: rows, code, a test or the source, whichever makes the case. A cause
   found in code must also be visible in the data. Which side is right must be argued, not assumed; both can be
   wrong. Counts come from the month files or a query you ran (keep it in the log); estimates are marked as such.
3. **Account for every failing key, by date.** Each discrepancy goes to a cause from the catalog, a new cause
   (proposed as a candidate), a consequence of an earlier event (link shown), or `unexplained`.
4. **One agent, one context.** Causes interact; keep the whole picture yourself. Subagents only for mechanical
   probes (run these N queries, fetch these rows), with their result checked by you.
5. **Don't touch the inputs or the catalog.** Never edit the month files, `configs.py`, or `catalog.md`; propose
   catalog candidates in the log. Write only `results/<config>/analysis.md` and `investigation-log.md` (and, when continuing, the
   renamed earlier `analysis.md`).
6. **Keep it local.** No artifacts, docs, PRs, or messages unless the user asks.
7. **Ask when blocked on a fact only a person has** (did the backfill finish, is the end date intended, was a
   contract switch deliberate). If you can't ask, write the question into the summary and continue on the stated
   assumption.

## Environment

**Queries:** run them with `analysis/chq` (from `scripts/`), not a hand-built `clickhouse-client` line. SQL comes
from stdin or a file, so no shell quoting; values go in as ClickHouse parameters, not by shell substitution:
```sh
analysis/chq -p d=2025-06-17 <<'EOF'
SELECT count() FROM default.xrp_stacks FINAL WHERE dt >= {d:Date} AND dt < {d:Date} + 1
EOF
```
`-f PrettyCompact` for reading, the default `TSVWithNames` for files. Keep reusable SQL in the scratchpad and
loop over days with `-p`.

**Tables:** the config names the local `*_shard_vN` tables. For probes, the Distributed tables over them read all
shards at once: `SELECT database, name, engine_full FROM system.tables WHERE engine = 'Distributed' AND engine_full
LIKE '%<local table>%'` (e.g. `xrp_stacks_shard_v9` is behind `test.xrp_stacks_test`).

**Repos:** `~/santiment/src/clickhouse-tables` (metric jobs, `table_qa`), `~/santiment/src/etherbi-flink` (the Flink
jobs that write transfers, balances and stacks), `~/santiment/src/san-chain-exporter` (the exporters feeding Kafka).
Earlier investigations are in `~/src/agentic-tasks/*/`; grep them for the table names.

**The server runs the old analyzer** (`enable_analyzer=0`):
- `ORDER BY` after `UNION ALL` belongs to the last branch only: wrap the union in `SELECT * FROM (…) ORDER BY …`.
- An alias that shadows a column changes the expressions using it; `sum(x) AS x` fails with `ILLEGAL_AGGREGATION`.
  Give aliases names that no column has.
- `SELECT *` skips MATERIALIZED columns (e.g. `assetRefId` in balances): list the columns.
- 64-bit integers come quoted in JSON formats; `chq` turns that off.

**Shell:** zsh. A variable holding a command is not split into words (use `chq`); quote globs (`--include='*.scala'`,
`ls -d ~/x/*/y`), since an unmatched glob is an error. Foreground `sleep` is blocked: run long queries with
`run_in_background` and wait for the notification.

## Continuing an analysis

If `results/<config>/analysis.md` exists, this is a continuation: more months were compared, or the earlier analysis
left questions. Read it and `investigation-log.md` first, and compare their scope with the month files present.
- **Earlier findings are the starting point, not to be proven again.** For each high-confidence event, state what it
  predicts for the new months (e.g. "the state error persists on the 779 B/D addresses, fading") and check that with
  a cheap probe: are the failing keys inside the known footprint, does the trend hold.
- **Spend the effort on what they don't predict:** new onsets, failures outside a known footprint, and the earlier
  open questions and `unexplained` items.
- **Earlier conclusions can be wrong.** If the new months contradict an event, re-open it and say so in the log.
- **Outputs:** rename the previous `analysis.md` to `analysis-<YYYY-MM-DD of its writing>.md`, then write a new one
  covering the whole scope, standalone (not a diff). Add a dated section to `investigation-log.md`; don't rewrite the
  earlier ones.

## Procedure

### 0. Orient (≤ 15 min)
- Read the config and `coverage.json` (groups on one side only: not failures, but list them in the summary).
- Check that new is completely written (`../../table-comparison.md`, Procedure step 1). A side still being written
  explains a failing tail and nothing else.
- Note the tables' engines and sorting keys (`system.tables`), and find the job that writes each side (repo, job
  name, version suffix). You will need it for the mechanism.
- Note the scope: months present, `partition.started` of each (one run or several?), the cutoff, and whether the run
  stopped early (the log's last line, `--max-failing-keys`). Months not written are *not checked*, not equal.

### 1. Build the time line
Per month: rows per side and keys per category (`jq '.month, .rows, .keys'`). Then per day (`days`) and, with
`group_by`, per group (`groups`). Derive whatever views you need ad hoc (jq, a scratch Python script in the
scratchpad); don't save derived files next to the results.

An **onset** is any of:
- the first failing day or month;
- a category that appears, disappears, or jumps (≥ 2× or by ≥ 10k keys) from one day to the next;
- a group that starts or stops failing, or changes category (e.g. `missing_in_old` → `differs`);
- a change in the mix: `zero` share, size class (`float`/`small`/`large`), or direction (`new_gt_old` share).

Group onsets that happen on the same day into one candidate event; then check whether they share a cause.

### 2. The earliest failing year first
Much of what follows is accumulated (stacks, balances, lookback metrics carry an error forward). Characterize the
first failing month fully before moving on: find its cause to high confidence, or rule out the testable hypotheses.
If the first year is massively broken (the run stopped early, or most keys fail), stop after it and report: the new
run needs a look before the rest is worth analysing.

### 3. Walk forward, month by month
For each onset after the first: is it explained by an event already found (a consequence: it propagates through
state, a lookback window, a downstream job), or is it new? Late, independent causes are common; don't assume
everything after the first event is its consequence without showing the link (same keys/addresses/groups, a
mechanism that carries it).

Also account for **ends**: when an event stops, say why (lookback window ran out, state healed, contract switch
date).

### 4. Investigate an event
1. **Match the catalog.** Compare the event's shape (categories, zeros, size class, direction, which groups, which
   days) with each entry's signature. Take the best match as the first hypothesis; keep the runner-up in mind.
2. **List keys** of 2–3 failing cells: the first day, one in the middle, the last day of the event, and, with groups,
   cells from different kinds of group (e.g. a transfer metric and a stack metric). `../compare.md` says how
   (`--print-sql`, then `SELECT * … WHERE category = … AND cell = […] LIMIT 10`).
3. **Fetch the rows of both sides** for those keys, all columns (list them: `SELECT *` skips MATERIALIZED ones), with
   `FINAL` and without (unmerged copies, versions).
4. **Decide which side is right** (rule 2): against the source, an invariant, or the code that wrote it.
5. **Find the mechanism**: why the wrong side is wrong. Data first (a gap in blocks, a price joined from the wrong
   minute, a different contract), then the code or job that produces it.
6. **Size it**: keys, rows, days, groups, and the impact on consumers in the value's units where it matters (e.g.
   "~0.0096 XRP on one address", "P/L ~0.4% of volume instead of 5–68%").

### 5. Confidence and stopping
- **high**: catalog signature (or a clear new signature), confirmed on samples across the event's extent, and the
  mechanism shown in the data (rows of both sides and the reference; the source or job when that is the cause).
- **medium**: signature and samples, no mechanism.
- **low**: signature only.
- **unexplained**: the testable hypotheses are ruled out (list them, with the evidence against each). Not a
  failure; it goes to the log for a later iteration.

There is no time budget. Stop when every event is high or unexplained. Medium and low are allowed in the output
only when the next step needs something you can't do (a person's answer, a non-prod system, a re-run): say what.

Before writing, **check your conclusions from another side**: for each high-confidence event, name the one
observation that would refute it, and look for it (e.g. "if old lost blocks, new's extra rows must add up to the
gap": they did, 135,131 exactly). Downgrade if you can't run the check.

### 6. Write the outputs
`template.md` has both. `analysis.md` is 2–3 pages, verdict first; `investigation-log.md` holds everything that
does not belong there: queries, dead ends, rejected hypotheses, open questions, catalog candidates.
