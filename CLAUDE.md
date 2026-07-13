# agentic-tasks

Storage for documentation of tasks worked on with agentic help. Each task
directory is a self-contained journal (runbooks, session logs, analysis
scripts); `INDEX.md` is the literal index of them — scan it to find prior
work. Durable residue is distilled OUT on close-out: decisions → in-repo
ADRs, reusable procedures → agent skills, prod defects → tracker issues.

## Conventions (enforced by the pre-commit hook in `.githooks/`)

- **Every task lives in its own directory named `YYYY-MM-<kebab-slug>/`**,
  dated by task start. Never create task docs as loose files at the repo root
  — the hook rejects any new file outside a task directory (repo-level files
  `INDEX.md`, `CLAUDE.md`, `README.md`, `.githooks/` excepted).
- The directory contains a main doc plus any session logs, runbooks, and
  analysis scripts. Keep the journal append-only during the task: the top of
  the main doc says what the task IS; dated session logs say what happened.
- **Every task gets a row in `INDEX.md`** (newest first), and its status
  column is updated on every significant milestone — the hook rejects commits
  that touch a task directory without touching `INDEX.md`.
- Keep the outcome cell to one or two sentences — current state and where to
  look. Detail belongs in the task's own journal.
