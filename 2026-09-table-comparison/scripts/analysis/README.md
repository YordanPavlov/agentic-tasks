# Step 2: the analysis agent

An agent reads the month files of `compare.py` (`../compare.md`), finds why old and new differ, decides which side
is right, and writes a summary for a reader deciding whether new may replace old.

Start it explicitly: *"Analyse `results/<config>/` following `scripts/analysis/RECIPE.md`."* If
`results/<config>/analysis.md` exists, the agent continues that analysis (e.g. after more months were compared).

## Files

| file | for | what |
|---|---|---|
| `RECIPE.md` | agent | ground rules, environment, procedure, confidence levels |
| `catalog.md` | agent, edited by us | known causes of differences, with their signatures |
| `playbooks/<kind>.md` | agent | hints per table kind: `stacks`, `balances`, `transfers`, `metrics` |
| `template.md` | agent | format of the two outputs |
| `chq` | agent | query helper: SQL from stdin or a file, values as ClickHouse parameters |


## Outputs

In `results/<config>/`, not committed:
- `analysis.md`: 2–3 pages, verdict and impact first, then the events in order of first appearance.
- `investigation-log.md`: queries, dead ends, rejected hypotheses, open questions, catalog candidates.

## Improving the agent

- **Catalog:** the agent never edits it; it proposes candidates in `investigation-log.md`, which we promote by hand.
- **Recipe and playbooks:** change them when an analysis shows a recurring mistake or a missing hint.
