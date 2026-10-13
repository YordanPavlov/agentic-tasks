# xrp_stacks_shard_v8 vs xrp_stacks_shard_v9: PASS with caveats

**Verdict and impact:** New is right everywhere it differs. Old lost the output of 5 ledger ranges in June 2025 and of 36
ledgers on 2025-12-22 (142,736 rows in all). For 12 of the June ledgers, old's stacks job never saw the input, so its
state has been wrong since then for the 779 addresses active in them; by the cutoff this is 2 addresses with drop-sized
errors on ~1,300 keys/day. Replacing old restores the lost rows and fixes those addresses' stacks; nothing gets worse.

**Scope:** 2013-01-01 .. 2026-09-03 (exclusive), all 165 months compared, nothing skipped. `compare.py` runs on
2026-10-13: 2013-01 .. 2025-06 at 11:02–11:37 UTC (stopped early), 2025-07 .. 2026-06 at 12:11–12:35, and
2026-07 .. 2026-09 at 13:05–13:08. Analysis 2026-10-13, continuing `analysis-2026-10-13.md` (which covered through 2025-06).
- Rows (`FINAL`): old 8,405,525,597, new 8,405,668,707 (+143,110).
- Keys: old 8,261,747,700, new 8,261,888,988. 8,252,089,561 equal, 10,106,943 failing, all from 2025-06-17 on.
- No `group_by`, so there's no `coverage.json`. Both tables were last written 2026-10-10 ~21:20, well past the cutoff;
  new has no unmerged (level-0) parts, so new is complete for the range.

**Events** (in order of first appearance):

| # | first day | event | duration | keys | impact | confidence |
|---|---|---|---|---|---|---|
| 1 | 2025-06-17 | old lost the output of 5 ledger ranges (484 ledgers) | intermittent, 3 days between 2025-06-17 and 2025-06-30 | 133,798 `missing_in_old` (135,501 rows) | new better | high |
| 2 | 2025-06-17 | old's state wrong for the 779 addresses active in 12 of those ledgers (ranges B, D) | from 2025-06-17 until the cutoff, fading | 9,966,026 (9,350,623 `differs`, 307,516 / 307,887 paired missing) | new better | high |
| 3 | 2025-12-22 | old lost the output of 36 ledgers (every 3rd, one shard) | 1 day | 7,119 `missing_in_old` (7,235 rows) | new better | high |

## 1. Old lost the output of 5 ledger ranges

- **What:** `missing_in_old` bursts on 2025-06-17, 06-26 and 06-30; new has 5 ledger ranges that old lacks completely,
  old has no ledgers that new lacks. Catalog: *Lost input on one side*.

  | range | ledgers | day, time (UTC) | rows in new | in old balances (`_v8`)? |
  |---|---|---|---|---|
  | A | 96867745–96867916 (172) | 06-17 11:31–11:43 | 50,454 | yes |
  | B | 96868120–96868121 (2) | 06-17 11:56 | 427 | **no** |
  | C | 97066419–97066618 (200) | 06-26 10:53–11:05 | 52,717 | yes |
  | D | 97066622–97066631 (10) | 06-26 11:06 | 2,473 | **no** |
  | E | 97154619–97154718 (100) | 06-30 10:11–10:17 | 29,430 | yes |

- **Mechanism:** old's stacks job (`XRPStacks.scala`) reads the XRP balances job's Kafka output and writes
  `xrp_stacks_v6.clickhouse-*`, which feeds `_v8`. For A, C, E only the stacks job's output was lost; its state saw
  the ledgers (the addresses active only there fail on the gap day and never again, through the cutoff). B and D are
  missing from old's balances table as well, so the stacks job never received them: event 2.
- **Example:** `rKi8uT8GQsyKvNo4ie4MVGsqsLUjG3noEy` has 160 rows in A in new (e.g. ledger 96867745 spends 37.820615
  and re-creates 37.820603, `odt` 2025-03-26 21:55:21); old has none of them.
- **Impact:** ~532M XRP of stacks born and spent missing from old on 3 days; old's daily XRP spent volume is short by
  0.90% (06-17), 0.76% (06-26), 0.21% (06-30). Each affected address's cumulative holdings in old (Σ `sign·amount`)
  are shifted by its net flow in the lost ledgers, permanently.
- **Related:** causes 2.

## 2. Old's state is wrong for the addresses active in ranges B and D

- **What:** `differs` (mostly `small`; many `float`-sized on large stacks) and paired `missing_in_new` /
  `missing_in_old`, every day from 2025-06-17 to the cutoff, on the 779 addresses active in B or D. Every failing
  key outside events 1 and 3, on all 443 days, belongs to one of them. Catalog: *Wrong state after lost input*.
- **Time line:** 2.45M keys in June 2025, 4.44M in July (529 addresses), 1.99M in August, then 0.08–0.17M per month
  (42–200 addresses), down to ~1,300–1,500 keys/day in August 2026 (35 addresses in the month). 626 of the 779 still fail after June,
  36 in August 2026. The steady tail is `rXPMusrUYj1t…` (~1,000 `float` keys/day, missed flow 12 drops) and
  `rsNvoAZ9MquZ…` (~300 `small` keys/day, missed flow 20 drops), which re-spend the affected stack on every transaction.
  Later bursts are single addresses spending or re-creating many stacks at once: 2025-10-24..11-11 and 2026-01-14..18
  (`large`, new > old, `rP4xfx…`, `rBYCfQ…`), 2025-11-04, 2026-06-22/23, 2026-08-01/02 (paired missing, `odt`
  differs; `rP4xfx…`, `rJ4EpE…`).
- **Mechanism:** old's Flink state never saw B and D, so each address's stacks carry its missed flow from then on.
  - `rKi8uT8…` on 2025-06-20: new's re-created stack is 31.783324 = the chain balance (balances `_v10`); old's is
    31.783516, +0.000192, exactly the address's flow in B.
  - When the missed flow was an inflow, old runs short and writes **deficit stacks with negative amounts and
    `odt = 1970-01-01`**. `rJ4EpEPTDR88…` on 2026-08-02: old re-creates an epoch stack of −0.195 XRP on each of 2,311
    transactions, new a stack born 2025-03-04 of +1.162; the gap per row is 1.3562, the address's flow in B/D
    (1.356212). `rP4xfx…` on 2025-11-07: both sides have an epoch deficit stack, old's lower by 0.107454 per row =
    its B/D flow.
  - Net flow per address is unaffected: per address and month, 2025-07 .. 2026-09-02, old's net flow equals new's
    (exact decimal sums) for every address except the event-3 ones. Only how the flow is split into stacks and ages
    differs.
- **Impact:** 9,966,026 keys over 443 days. Most values are off by drops, but deficit-stack keys are wrong by up to
  100%. Old has ~24k more negative epoch-dated rows than new over 2025-06 .. 2026-09 (estimate, without `FINAL`),
  i.e. 55-year-old negative stacks that age-based metrics count. The error persists while an affected stack is held.
- **Related:** consequence of 1 (ranges B and D).

## 3. Old lost the output of 36 ledgers on 2025-12-22

- **What:** 7,119 `missing_in_old` keys (522 zero) on 2025-12-22, no `missing_in_new`; new has 36 ledgers that old
  lacks: every third ledger of 101038121–101038226 (02:19:11–02:26:01 UTC), all with `blockNumber % 3 = 2`, i.e.
  everything old's shard 3 should have received in those 7 minutes. Old has the neighbouring ledgers on shards 1–2.
  Catalog: *Lost input on one side* (output only, as A/C/E).
- **Mechanism:** old's balances table has all 36 ledgers (8,867 rows, as `_v10`), so old's stacks job received them;
  its rows for one of the 3 topics (the sink spreads ledgers over them by `blockNumber`) were lost for a
  window. The state saw them: none of the 965 addresses active only in these ledgers fails on any later day,
  and every address whose December net flow differs between the sides (992) is one of them, by exactly its flow
  in these ledgers.
- **Impact:** 7,235 rows, 32.9M XRP born and spent; old's XRP spent volume on 2025-12-22 is short by 0.125%. The
  affected addresses' cumulative holdings in old are shifted by their flow in these ledgers, permanently.
- **Related:** none (independent of 1 and 2; 164 of the addresses overlap event 2's).

**Not checked:**
- After the cutoff (2026-09-03 .. 2026-10-10), though both tables have data there.
- Stacks invariants (`get_stacks_with_huge_delta`) were not run.
- Whether the lost rows are still in Kafka (June and December 2025, likely past retention).

**Open questions:**
- Both tables were last written 2026-10-10 ~21:20, including the live `_v8`. Is the live pipeline stopped, and will
  `_v9` (or its job) take over live writing?
