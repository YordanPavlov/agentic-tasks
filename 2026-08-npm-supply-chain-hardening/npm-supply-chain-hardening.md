# npm supply-chain hardening — repo remediation list

**Task:** Following the 2026-08-04 npm supply-chain attack (keyv / flat-cache /
file-entry-cache + 8 more, "Shai-Hulud" worm — see
https://www.aikido.dev/blog/keyv-and-friends-compromised-in-npm-supply-chain-attack),
survey the santiment GitHub org's npm/Node repos and track remediation of the
two structural weaknesses found: missing/disabled package locking, and git
(GitHub-ref) dependencies.

**Survey scope & method:** all 331 non-archived org repos; the 61 with a
`package.json` and activity since 2025-01-01 were analyzed (package.json,
lockfiles, committed .npmrc, GitHub workflows, lockfile scan against the 11
directly-compromised package versions). Raw data: `scan-results.tsv`,
`vuln-report.tsv` in this directory. Dormant pre-2025 repos were not analyzed —
check before reviving any of them.

**Headline result:** no repo had a compromised version locked. The lists below
are preventive hardening, not incident response.

---

## 1. Repos needing package locking

Installs in these repos resolve dependencies fresh from the registry — a
malicious release published upstream is ingested automatically, no human
action required. Fix: commit a lockfile, use `npm ci` (or frozen-lockfile
equivalent) in CI, remove lockfile-disabling config.

| Repo | Deps | Problem | Priority |
|---|---|---|---|
| `san-charts` | 1 + 53 dev | No lockfile **by design**: `.npmrc` sets `package-lock=false`; GitHub workflow runs bare `npm install` on every CI run | **P1 — worst posture in the org** |
| `san-node` | 7 + 12 dev | No lockfile | P2 |
| `support-pages` | 9 | No lockfile | P3 (static pages, low install frequency) |

### 1b. Ambiguous / weakened locking (pick one lockfile, delete the other)

Two lockfiles means the one your tooling *doesn't* use silently drifts and
nobody knows which resolution actually ships.

| Repo | Problem |
|---|---|
| `openarena-frontend` | `package-lock.json` **and** `bun.lock` |
| `san-graphs` | `package-lock.json` **and** `yarn.lock` |
| `sanr-op-bridge` | `package-lock.json` **and** `yarn.lock` |
| `sanr-chain-landing` | `.npmrc` sets `resolution-mode=highest` — every resolution grabs newest matching, maximizing exposure windows |

Note on `openarena-frontend`: it is also the org's closest near-miss — locked
on the same major lines as 5 of the 11 poisoned releases (flat-cache 6.1.20 vs
poisoned 6.1.24, file-entry-cache 11.1.2 vs 11.1.6, cacheable 2.3.3 /
@cacheable/memory 2.0.8 / @cacheable/utils 2.4.0 vs poisoned 2.5.1 / 2.2.1 /
2.5.1). Any lockfile refresh during a compromise window would have pulled
malware. Treat its dependency bumps with extra care.

## 2. Repos with git dependencies

Git deps bypass registry immutability, provenance, and audit; they require
install-time `prepare` script execution (blocking `ignore-scripts=true`
adoption) and are the surface of the npm git-binary/.npmrc hijack that
`allow-git=none` (npm ≥ 11.10) closes. Remediation path: pin to commit SHAs
now; publish internal libs (`san-webkit`, `san-webkit-next`, `san-studio`,
`svelte-preprocess-cssmodules` fork) to a registry (GitHub Packages) and
migrate consumers; then set `ignore-scripts=true` + `allow-git=none` per repo.

| Repo | Git dependencies |
|---|---|
| `sanbase-app` | Sanbase, san-studio, san-webkit, san-webkit-next, svelte-preprocess-cssmodules |
| `san-queries` | san-studio, san-webkit, san-webkit-next, svelte-preprocess-cssmodules |
| `san-studio` | san-webkit, svelte-preprocess-cssmodules |
| `san-webkit` | svelte-preprocess-cssmodules |
| `embed-app` | san-studio, san-webkit |
| `san-preview` | san-studio, san-webkit |
| `insights-app` | san-webkit, svelte-preprocess-cssmodules |
| `api-landing` | san-webkit, svelte-preprocess-cssmodules |
| `sanr-chain-landing` | san-webkit, svelte-preprocess-cssmodules |
| `research-landing` | san-webkit, san-webkit-next |
| `academy` | san-webkit-next |
| `santiment.net` | san-webkit-next |
| `san-mcp-apps` | san-webkit-next |
| `san-agent-chart-formulas-service` | san-webkit |
| `openarena-frontend` | san-webkit |
| `santiops-options-web` | @rainbow-me/rainbowkit (fork) |
| `blockscout-frontend` | gradient-avatar |
| `grafana` (fork) | rst2html, tether-drop (upstream's; low priority) |

Publish-order dependency: `svelte-preprocess-cssmodules` → `san-webkit` /
`san-webkit-next` → `san-studio` → consumers (leaf libs first, since the
shared libs are themselves consumers of each other).

## Related work

- `san-chain-exporter` branch `npm-ignore-scripts` (2026-08-04, pending
  review): repo-level `.npmrc` with `ignore-scripts=true`, Dockerfile +
  README updates, explicit `npm rebuild node-rdkafka --ignore-scripts=false`.
  Template for rolling the same pattern to the other backends (which have no
  git deps and are the easy wins).

## Session log

- **2026-08-05** — Org survey run (script + raw TSVs in this dir). Lists
  above compiled. No compromised versions found locked anywhere. Caveats:
  only the 11 directly-poisoned packages checked (full ~434-package worm list
  not yet public); default branches only.
