# san-chain-exporter: repair `npm run lint`

**Started:** 2026-09-16
**Status:** scoped, not started — branch to be forked from `master`
**Repo:** `san-chain-exporter`

## Problem

`npm run lint` has been broken since the Node 24 upgrade (`38f9592`),
which bumped `eslint` to `^9.4.0` but kept the legacy `.eslintrc.js`.
Two independent breakages:

- ESLint 9 only reads flat configs (`eslint.config.*`); `.eslintrc.js`
  needs `ESLINT_USE_FLAT_CONFIG=false` to be honoured at all.
- `.eslintrc.js` references `@typescript-eslint/parser` and
  `@typescript-eslint/eslint-plugin`, neither of which is declared in
  `devDependencies`, so they are not installed.

Lint is not wired into `Jenkinsfile` or `.github/workflows`, so nothing
caught this. Discovered while preparing the XRP rate-limit fix (PR on
branch `xrpRateLimitRetry`), which could not be linted.

## Measured baseline (2026-09-16)

Trial run with ESLint 9.6 + `typescript-eslint` 8 installed in a scratch
dir and a flat config mirroring the current rules (`eslint.config.mjs`
in this directory). 120 files linted, 91 with issues, 1331 violations:

| Rule | Count | Note |
|---|---|---|
| `quotes`, `semi`, `prefer-const` | 1060 | autofixable (`eslint --fix`) |
| `@typescript-eslint/no-explicit-any` | 191 | new strict preset default; relax to warn/off |
| `@typescript-eslint/no-require-imports` | 58 | mostly tests; relax or convert to imports |
| `@typescript-eslint/no-unused-vars` | 16 | manual |
| one-offs (`no-var`, `eqeqeq`, `no-prototype-builtins`, `no-wrapper-object-types`, `no-extra-non-null-assertion`) | 5 | manual, minutes |

Install note: `typescript-eslint@latest` hit a peer conflict with the
pinned `eslint@9.6.0`; `typescript-eslint@8` + `@eslint/js@9.6.0` +
`globals` installed cleanly.

## Plan

1. Add `typescript-eslint@8`, `@eslint/js`, `globals` to `devDependencies`.
2. Replace `.eslintrc.js` with `eslint.config.mjs` (draft here); set
   `no-explicit-any` and `no-require-imports` to `warn` to keep scope
   bounded — typing away 191 `any`s is a separate multi-day task.
3. Commit 1: `eslint --fix` (mechanical, ~1060 changes). Commit 2: the
   ~21 manual fixes. Run `npm test` after each.
4. Add `npm run lint` to CI so it stays green.

Estimate: 2–4 h. Keep separate from the XRP rate-limit PR for review
readability.
