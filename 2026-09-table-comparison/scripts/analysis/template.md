# Output templates

## `analysis.md`

```markdown
# <old> vs <new>: <PASS | PASS with caveats | DIFFERS>[, through <last month compared>]

**Verdict and impact** (≤ 3 sentences): which side is right, what a consumer would see if new replaced old.
E.g. "New is right: old lost 5 block ranges in June 2025 and carries a wrong state for ~400 addresses since.
Replacing old fixes their stacks; nothing gets worse."

**Scope:** <start> .. <cutoff> (exclusive); months compared / not written (early stop, skipped); rows and keys per
side; date of the `compare.py` run and of this analysis. Groups on one side only (`coverage.json`): count, and why.

**Events** (table, in order of first appearance):

| # | first day | event | duration | keys | impact | confidence |
|---|---|---|---|---|---|---|
| 1 | 2025-06-17 | old lost 5 block ranges | 3 days (2025-06-17..30), not recurring | 135k rows | new better | high |
| 2 | 2025-06-17 | old's state wrong for ~420 addresses | from 2025-06-17 until the cutoff, fading | … | new better | high |

## 1. <event title>
- **What:** categories, groups, days; the shape that identifies it (catalog entry, if any).
- **Mechanism:** why the wrong side is wrong. One example key with its row on each side and in the reference.
- **Impact:** keys, rows, value in units; consumer-visible effect.
- **Related:** "Consequence of 1", "Causes 3", or none.

## 2. …

**Not checked:** months not written, invariants not run, metrics on one side only.
**Open questions:** for the owner of the re-run (e.g. backfill end date, intended contract switch).
```

Rules for the wording:
- **Verdict:** `PASS` = no failing keys, or only float noise. `PASS with caveats` = every event is explained
  (high or medium) and is benign or new better. `DIFFERS` = any event where new is worse or both are wrong, or an
  unexplained event of material size.
- **Partial scope:** when months up to the cutoff were not written (early stop, skipped), the verdict covers only
  what was compared: add `, through <YYYY-MM>` to the title and say in the verdict that the rest is not checked.
- **Impact,** one of: `new better`, `old better`, `both wrong`, `benign` (no consumer-visible change: zero padding,
  float noise, a split that the key sums away), `regression` (new wrong where old was right; `old better` from the
  consumer's view, named so because it blocks the replacement).
- **Duration,** fixed phrases: `1 day`; `N days (A..B), not recurring`; `intermittent, N days between A and B`;
  `from A until the cutoff`; add `growing` / `fading` when the daily count trends.
- Dates `YYYY-MM-DD`; key counts exact from the month files, or `~` when estimated.
- Events in order of first appearance; a consequence gets its own number only if it has its own days or impact.

## `investigation-log.md`

```markdown
# Investigation log: <config>, <date>

## Time line
The onsets found (date, what changed), and which event each went to.

## Per event
Hypotheses tried, in order; the queries that decided them (SQL in a code block, with the result); samples used;
the refutation check (README, step 5) and its result.

## Unexplained
What it looks like, the hypotheses ruled out and why, what would be needed next.

## Catalog candidates
A new cause or a correction to an entry, in the catalog's format, with the evidence. Promoted by hand.

## Notes
Side findings outside the comparison (e.g. an invariant broken on both sides), cost of the probes.
```
