#!/bin/bash
# Per-month output comparison of prod xrp_balances (old) vs a re-run table (new).
# Dedup = GROUP BY the ReplacingMergeTree ORDER BY key; per key: min/max hash of the
# non-float columns (catches internal version conflicts), avg of the float columns.
# See qa-output-comparison.md for the method and gotchas.
#
# Usage: ./compare_months.sh <workdir>
#   <workdir> must contain monthly_counts.tsv (see the doc for the query that makes it).
#   Appends to <workdir>/results.tsv; progress.log / errors.log alongside.
set -u
WORK="${1:?usage: compare_months.sh <workdir with monthly_counts.tsv>}"
OUT="$WORK/results.tsv"
LOG="$WORK/progress.log"
ERR="$WORK/errors.log"
OLD_TABLE="xrp_balances"
NEW_TABLE="test.xrp_balances_test"

CH() {
  clickhouse-client -h clickhouse.production.san --port 30900 -u readonly \
    --max_memory_usage=25000000000 --format=TSV --query="$1"
}
EH="cityHash64(currency, ifNull(issuer,''), isNull(issuer), issuerCurrency, ifNull(oldDt, toDateTime(0)), isNull(oldDt), ifNull(oldBlockNumber,0), isNull(oldBlockNumber), addressType, transactionHash)"

run_one() {
  local src=$1 tbl=$2 month=$3 K=$4
  local b cond
  for ((b=0; b<K; b++)); do
    cond=""
    if (( K > 1 )); then cond="AND cityHash64(address) % $K = $b"; fi
    CH "
    SELECT $month, '$src', $b,
      count(), sum(kh), sum(cityHash64(kh, min_eh)), sum(cityHash64(kh, max_eh)),
      sum(bal), sum(abs(bal)), sum(obal), sum(abs(obal)), max(spread), sum(dups)
    FROM (
      SELECT cityHash64(dt, assetRefId, address, blockNumber, transactionIndex) AS kh,
             min($EH) AS min_eh, max($EH) AS max_eh,
             avg(balance) AS bal, avg(oldBalance) AS obal,
             max(balance) - min(balance) AS spread,
             count() - 1 AS dups
      FROM $tbl WHERE toYYYYMM(dt) = $month $cond
      GROUP BY dt, assetRefId, address, blockNumber, transactionIndex
    )" >> "$OUT" || echo "FAILED month=$month src=$src bucket=$b/$K" >> "$ERR"
  done
}

tail -n +2 "$WORK/monthly_counts.tsv" | while read -r month old_rows new_rows diff; do
  maxraw=$(( old_rows > new_rows ? old_rows : new_rows ))
  K=$(( (maxraw + 119999999) / 120000000 )); (( K < 1 )) && K=1
  echo "$(date -u +%H:%M:%S) start $month K=$K old_raw=$old_rows new_raw=$new_rows" >> "$LOG"
  run_one old "$OLD_TABLE" "$month" "$K" &
  run_one new "$NEW_TABLE" "$month" "$K" &
  wait
  echo "$(date -u +%H:%M:%S) done  $month" >> "$LOG"
done
echo "$(date -u +%H:%M:%S) ALL DONE" >> "$LOG"
