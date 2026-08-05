#!/bin/bash
# Scan santiment org repos for npm usage + supply-chain posture
set -u
TMP=/home/agent/.claude/jobs/2724b601/tmp
OUT=$TMP/scan-results.tsv
LOCKDIR=$TMP/lockfiles
mkdir -p "$LOCKDIR"
echo -e "repo\tlang\tpushed\tpkgjson\tlockfiles\tpkgmanager\tbuildtool\thas_workflows\tnotes" > "$OUT"

# candidates: non-archived, pushed >= 2025-01-01
awk -F'\t' '$3 >= "2025-01-01"' "$TMP/repos.tsv" | while IFS=$'\t' read -r repo lang pushed fork; do
  # root package.json?
  pkg=$(gh api "repos/santiment/$repo/contents/package.json" -q .content 2>/dev/null | base64 -d 2>/dev/null)
  loc="root"
  if [ -z "$pkg" ]; then
    # JS-ish repo without root package.json: look for one in the tree (skip huge scan for others)
    case "$lang" in
      JavaScript|TypeScript|Svelte|HTML|Vue)
        path=$(gh api "repos/santiment/$repo/git/trees/HEAD?recursive=1" -q '.tree[].path' 2>/dev/null | grep -E '(^|/)package\.json$' | grep -v node_modules | head -1)
        if [ -n "$path" ]; then
          pkg=$(gh api "repos/santiment/$repo/contents/$path" -q .content 2>/dev/null | base64 -d 2>/dev/null)
          loc=$(dirname "$path")
        fi
        ;;
    esac
  fi
  [ -z "$pkg" ] && continue

  dir=$loc; [ "$dir" = "root" ] && dir="" || dir="$dir/"
  locks=""
  for lf in package-lock.json yarn.lock pnpm-lock.yaml bun.lockb bun.lock npm-shrinkwrap.json; do
    if gh api "repos/santiment/$repo/contents/${dir}${lf}" -q .name >/dev/null 2>&1; then
      locks="$locks$lf,"
      # download npm/yarn lockfiles for vulnerability grep (contents API caps at 1MB -> use raw)
      if [ "$lf" = "package-lock.json" ] || [ "$lf" = "yarn.lock" ] || [ "$lf" = "pnpm-lock.yaml" ]; then
        gh api "repos/santiment/$repo/contents/${dir}${lf}" -H "Accept: application/vnd.github.raw" > "$LOCKDIR/${repo}__${lf}" 2>/dev/null
      fi
    fi
  done

  pm=$(echo "$pkg" | jq -r '.packageManager // empty' 2>/dev/null)
  # crude build tool detection from scripts + devDeps
  build=$(echo "$pkg" | jq -r '[
      (if (.scripts.build // "") != "" then "build:\"" + (.scripts.build|.[0:60]) + "\"" else empty end),
      (if (.devDependencies // {} | has("vite")) or (.dependencies // {} | has("vite")) then "vite" else empty end),
      (if (.devDependencies // {} | has("webpack")) then "webpack" else empty end),
      (if (.devDependencies // {} | has("next")) or (.dependencies // {} | has("next")) then "next" else empty end),
      (if (.devDependencies // {} | has("@sveltejs/kit")) or (.dependencies // {} | has("@sveltejs/kit")) then "sveltekit" else empty end),
      (if (.devDependencies // {} | has("react-scripts")) or (.dependencies // {} | has("react-scripts")) then "CRA" else empty end),
      (if (.devDependencies // {} | has("typescript")) then "ts" else empty end)
    ] | join("|")' 2>/dev/null)
  wf=$(gh api "repos/santiment/$repo/contents/.github/workflows" -q 'length' 2>/dev/null || echo 0)
  gitdeps=$(echo "$pkg" | jq -r '[(.dependencies // {}), (.devDependencies // {})] | add | to_entries[] | select(.value | test("^(git|github:|.*://)")) | .key' 2>/dev/null | paste -sd, -)
  echo -e "$repo\t$lang\t$pushed\t$loc\t${locks%,}\t$pm\t$build\t$wf\tgitdeps:$gitdeps"
done >> "$OUT"
echo "DONE $(grep -c . "$OUT") lines"
