# agentic-tasks

Storage for documentation of tasks worked on with agentic help. Each task
directory is a self-contained journal (runbooks, session logs, analysis
scripts) — list the task directories to find prior work. Durable residue is
distilled OUT on close-out: decisions → in-repo ADRs, reusable procedures →
agent skills, prod defects → tracker issues.

## Conventions

- **Every task lives in its own directory named `YYYY-MM-<kebab-slug>/`**,
  dated by task start. Never create task docs as loose files at the repo root
  (repo-level files `CLAUDE.md`, `README.md` excepted).
- The directory contains a main doc plus any session logs, runbooks, and
  analysis scripts. Keep the journal append-only during the task: the top of
  the main doc says what the task IS; dated session logs say what happened.
