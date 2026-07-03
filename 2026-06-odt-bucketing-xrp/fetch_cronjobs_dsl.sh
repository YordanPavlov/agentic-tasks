#!/usr/bin/env bash
# Grab /app/daily_metrics/specs.d/cronjobs/cronjobs-airflow-dsl.yaml out of a
# live clickhouse-tables Airflow job pod (they live <1 min, so poll and strike).
#
# Usage: bash fetch_cronjobs_dsl.sh [kubeconfig] [output-file]
#   defaults: ~/.kube/config_prod  ./cronjobs-airflow-dsl.yaml

set -u
KUBECONFIG_FILE="${1:-$HOME/.kube/config_prod}"
OUT="${2:-./cronjobs-airflow-dsl.yaml}"
mkdir -p "$(dirname "$OUT")"
NS=airflow
SRC=/app/daily_metrics/specs.d/cronjobs/cronjobs-airflow-dsl.yaml
DEADLINE=$((SECONDS + 300))

kc() { kubectl --kubeconfig "$KUBECONFIG_FILE" -n "$NS" "$@"; }

while (( SECONDS < DEADLINE )); do
  # newest running clickhouse-tables pod = most lifetime remaining
  pod=$(kc get pods -l airflow_task_pod=true \
          --field-selector=status.phase=Running \
          --sort-by=.metadata.creationTimestamp \
          -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.containers[0].image}{"\n"}{end}' 2>/dev/null \
        | awk -F'\t' '$2 ~ /clickhouse-tables/ {p=$1} END {if (p) print p}')

  if [[ -n "${pod:-}" ]]; then
    echo ">>> copying from pod: $pod"
    # exec+cat instead of `kubectl cp` (no tar dependency, single round-trip)
    if kc exec "$pod" -- cat "$SRC" > "$OUT.tmp" 2>/dev/null && [[ -s "$OUT.tmp" ]]; then
      mv "$OUT.tmp" "$OUT"
      echo ">>> saved: $OUT ($(wc -c < "$OUT") bytes)"
      echo ">>> quick stats:"
      grep -c "kind: DailyMetricsCronJob" "$OUT" | xargs echo "    DailyMetricsCronJob docs:"
      grep -c "^  dag: \|^    dag: " "$OUT" | xargs echo "    specs with dag field:   "
      exit 0
    fi
    rm -f "$OUT.tmp"
    echo "!!! copy failed (pod died or local write error), retrying..."
  else
    printf '.'
  fi
  sleep 2
done

echo
echo "Timed out (5 min) without catching a clickhouse-tables pod."
exit 1
