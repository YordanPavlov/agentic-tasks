#!/usr/bin/env bash
# Catch a short-lived Airflow job pod running the clickhouse-tables image and
# inspect /app/daily_metrics/specs.d for AirflowDags docs + the generated
# cronjobs-airflow-dsl.yaml (evidence for the PR #2270 double-load review).
#
# Usage: bash check_pod_airflowdags.sh [kubeconfig]   (default: ~/.kube/config_prod)

set -u
KUBECONFIG_FILE="${1:-$HOME/.kube/config_prod}"
NS=airflow
DEADLINE=$((SECONDS + 300))   # give up after 5 minutes

kc() { kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$NS" "$@"; }

while (( SECONDS < DEADLINE )); do
  # One API call: running job pods sorted oldest->newest, with their image.
  # Take the NEWEST clickhouse-tables pod (just started => most lifetime left).
  pod=$(kc get pods -l airflow_task_pod=true \
          --field-selector=status.phase=Running \
          --sort-by=.metadata.creationTimestamp \
          -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\n"}{end}' 2>/dev/null \
        | awk -F'\t' '$2 ~ /clickhouse-tables/ {p=$1} END {if (p) print p}')

  if [[ -n "${pod:-}" ]]; then
    echo ">>> exec into: $pod"
    if kc exec "$pod" -- sh -c '
      echo "== image tag proof =="
      ls -la /app/daily_metrics/specs.d/cronjobs/cronjobs-airflow-dsl.yaml 2>/dev/null \
        || echo "cronjobs-airflow-dsl.yaml: MISSING"
      echo
      echo "== files containing kind: AirflowDags under specs.d =="
      grep -rl "kind: AirflowDags" /app/daily_metrics/specs.d/ 2>/dev/null \
        | sed "s|/app/daily_metrics/specs.d/||" | sort
      echo
      echo "== total AirflowDags files =="
      grep -rl "kind: AirflowDags" /app/daily_metrics/specs.d/ 2>/dev/null | wc -l
      echo
      echo "== sample job defined twice? (xrp-circulation-intraday-deltas) =="
      grep -rln "name: xrp-circulation-intraday-deltas" /app/daily_metrics/specs.d/ 2>/dev/null
    '; then
      exit 0
    fi
    echo "!!! pod vanished mid-exec, retrying..."
  else
    printf '.'
  fi
  sleep 2
done

echo
echo "Timed out (5 min) without catching a running clickhouse-tables pod."
exit 1
