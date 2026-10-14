# Old-vs-new table comparison

Tools to compare two versions of a ClickHouse table: same structure, different versions of the data (typically a
pipeline re-run into an `*_experimental` table). The goal is to find what differs, attribute it to code changes or
events, and decide whether the new version may replace the old one.

## The two steps

### Step 1. A generic comparison script

`compare.py` compares the two tables key by key over the full history, counts the failing keys per day and group. It is
meant to be universal: the SQL is static, each run plugs in the tables of one config, and it is made efficient by
working one partition at a time, cut into windows. Details: [`compare.md`](compare.md).

### Step 2. An agentic LLM analyst

An agent interprets the differences. We give it a recipe, tools, hints and preferences, but leave it free to reason:
to inspect the tables further, read the code and its git history, etc. The goal is to use all its abilities for as
good an investigation as possible. Details: [`analysis/README.md`](analysis/README.md).

## Layout

| path | what |
|---|---|
| `compare.py`, `common.py`, `sql/` | step 1: the script and its SQL templates |
| `configs.py` | one config per comparison: tables, keys, value, range |
| `compare.md` | step 1: method, output format, usage |
| `analysis/` | step 2: the agent's recipe, cause catalog, playbooks, output templates |
| `results/<config>/` | step 1's month files, and step 2's `analysis.md` and `investigation-log.md` |

## Quick start

1. Add a config to `configs.py` and run `compare.py` ([`compare.md`](compare.md), "Usage").
2. Ask an agent: *"Analyse `results/<config>/` following `scripts/analysis/RECIPE.md`."*
