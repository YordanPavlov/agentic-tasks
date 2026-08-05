#!/bin/bash
# Check lockfiles for the 11 packages compromised 2026-08-04 (keyv/Shai-Hulud attack)
# Output: repo, package, locked version(s), COMPROMISED flag
set -u
LOCKDIR=/home/agent/.claude/jobs/2724b601/tmp/lockfiles
declare -A BAD=(
  [keyv]=6.0.0 [flat-cache]=6.1.24 [file-entry-cache]=11.1.6
  [cacheable-request]=13.0.20 [cacheable]=2.5.1 [cache-manager]=7.2.10
  [@cacheable/memory]=2.2.1 [@cacheable/node-cache]=3.1.2
  [@cacheable/utils]=2.5.1 [@cacheable/net]=2.1.1 [ecto]=5.0.1
)
for f in "$LOCKDIR"/*; do
  base=$(basename "$f"); repo=${base%%__*}; kind=${base##*__}
  for pkg in "${!BAD[@]}"; do
    badv=${BAD[$pkg]}
    case "$kind" in
      package-lock.json)
        vers=$(jq -r --arg p "$pkg" '.packages // {} | to_entries[] | select(.key | endswith("node_modules/" + $p)) | .value.version' "$f" 2>/dev/null | sort -u | paste -sd, -)
        # npm lockfile v1 fallback
        [ -z "$vers" ] && vers=$(jq -r --arg p "$pkg" '.. | objects | select(has("dependencies")) | .dependencies[$p]? // empty | .version? // empty' "$f" 2>/dev/null | sort -u | paste -sd, -)
        ;;
      yarn.lock)
        vers=$(awk -v p="$pkg" '$0 ~ "^\"?" p "@" {found=1} found && /^  version/ {gsub(/[" ]/,"",$2); print $2; found=0}' "$f" | sort -u | paste -sd, -)
        ;;
      pnpm-lock.yaml)
        vers=$(grep -oE "^  /?'?${pkg}[@/]([0-9]+\.[0-9]+\.[0-9]+)" "$f" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+$' | sort -u | paste -sd, -)
        [ -z "$vers" ] && vers=$(grep -oE "${pkg}@[0-9]+\.[0-9]+\.[0-9]+" "$f" 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | sort -u | paste -sd, -)
        ;;
      *) continue ;;
    esac
    if [ -n "$vers" ]; then
      flag=OK
      for v in ${vers//,/ }; do [ "$v" = "$badv" ] && flag="!!COMPROMISED!!"; done
      echo -e "$repo\t$pkg\t$vers\t$flag"
    fi
  done
done
