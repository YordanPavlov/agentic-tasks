#!/usr/bin/env python3
"""Compare production XRP metrics against the experimental (batched-odt) run.

Adaptation of ltc-migration/compare_ltc_experimental.py for the hourly-odt-bucket
validation (runbook: verifying-the-batched-odt-migration.md §4/§10). Differences:

1. `distribution` join is HOUR-TRUNCATED: the experimental seam cells are hourly by
   design (odt bucketing), so prod's 5-min cells must be rolled up to the hour before
   diffing (toStartOfHour + sum(measure)) — the raw (metric_id, dt, value) join of the
   LTC script would report phantom 100% divergence. Mirrors Layer 1 §3.5.
2. Adds p50/p99 of the symmetric relative diff per metric — the §4 deviation bounds
   are stated as median/p99 (the sawtooth skews plain means).

Per metric the §4 expectations, by window suffix (hourly bucket, clean baseline):
  *_1d: median ~2.1%, p99 <~4.2% | *_7d: <1% | *_30d: <~0.14% | >=90d: noise
  daily & prices: exact | MRP/MVRV: ~0.2% flat (price-ASOF term only)
NOTE on *delta* metrics (circulation/realized-cap deltas, age/price-consumed, NPL):
pointwise relative diffs can legitimately be large — bucketing shifts age-out timing
within the hour, reallocating value between adjacent 5-min slots. Judge deltas by
sum_ratio (the integral) and by the cumsum LEVELS once stage 2 runs; judge levels by
p50/p99.

Dedups ReplacingMergeTree rows via argMax(value|measure, computed_at). No writes.

Usage:
  .venv/bin/python compare_xrp_experimental.py --start 2013-01-01 --end 2013-12-31 \
      [--table daily|intraday|distribution|all] [--tol 0.005]
"""
import argparse
import sys

from clickhouse_driver import Client

# label -> (prod_table, experimental_table, numeric_column, group_cols)
TABLE_PAIRS = {
    "daily":        ("daily_metrics_v2", "daily_metrics_v2_experimental", "value", ["metric_id", "dt"]),
    "intraday":     ("intraday_metrics", "intraday_metrics_experimental", "value", ["metric_id", "dt"]),
    "distribution": ("distribution_deltas_5min", "distribution_deltas_5min_experimental", "measure",
                     ["metric_id", "dt", "odtb"]),
}

# SYMMETRIC relative diff |pv-ev| / mean(|pv|,|ev|), bounded [0,2] (see LTC script for why).
COMPARE_SQL = """
SELECT
    metric_id,
    countIf(pv IS NOT NULL AND ev IS NOT NULL)                                       AS both,
    countIf(pv IS NOT NULL AND ev IS NULL)                                           AS prod_only,
    countIf(pv IS NULL  AND ev IS NOT NULL)                                          AS exp_only,
    maxIf(abs(pv - ev),                                            pv IS NOT NULL AND ev IS NOT NULL) AS max_abs,
    maxIf(abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12),     pv IS NOT NULL AND ev IS NOT NULL) AS max_rel,
    avgIf(abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12),     pv IS NOT NULL AND ev IS NOT NULL) AS mean_rel,
    quantileIf(0.5)(abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12), pv IS NOT NULL AND ev IS NOT NULL) AS p50_rel,
    quantileIf(0.99)(abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12), pv IS NOT NULL AND ev IS NOT NULL) AS p99_rel,
    minIf(dt, pv IS NOT NULL AND ev IS NOT NULL
              AND abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12) > %(tol)s)      AS first_div,
    sumIf(pv, pv IS NOT NULL)                                                         AS prod_sum,
    sumIf(ev, ev IS NOT NULL)                                                         AS exp_sum
FROM
(
    {prod_subquery}
) p
FULL OUTER JOIN
(
    {exp_subquery}
) e
USING ({groupcols})
GROUP BY metric_id
HAVING countIf(ev IS NOT NULL) > 0
ORDER BY metric_id
SETTINGS join_use_nulls = 1
"""

# daily/intraday: one row per (metric_id, dt); dedup versions, done.
PLAIN_SUBQUERY = """
    SELECT {groupcols}, argMax({valcol}, computed_at) AS {alias}
    FROM {table} WHERE asset_id = %(aid)s AND dt BETWEEN %(start)s AND %(end)s
    GROUP BY {groupcols}
"""

# distribution: dedup versions per raw 5-min cell (metric_id, dt, value=odt), THEN roll
# odt up to the hour and sum. Exp cells are already hour-aligned; truncating both sides
# keeps the query symmetric.
DIST_SUBQUERY = """
    SELECT metric_id, dt, odtb, sum(m) AS {alias}
    FROM (
        SELECT metric_id, dt, toStartOfHour(value) AS odtb,
               argMax(measure, computed_at) AS m
        FROM {table} WHERE asset_id = %(aid)s AND dt BETWEEN %(start)s AND %(end)s
        GROUP BY metric_id, dt, value, odtb
    )
    GROUP BY metric_id, dt, odtb
"""


def verdict(row, tol):
    # Classify on p50 (median): robust to both sawtooth skew and single outlier days.
    both, prod_only, exp_only, max_abs, max_rel, mean_rel, p50_rel, p99_rel, first_div = row[1:10]
    if both == 0:
        return "EXP-ONLY (no prod counterpart)" if exp_only else "no-overlap"
    if p50_rel is None or p50_rel <= tol:
        return "MATCH"
    if p50_rel <= 0.05:
        return "minor-drift"
    return "DIVERGE"


def fmt(v, nd=6):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:.{nd}g}"
    return str(v)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="clickhouse.production.san")
    ap.add_argument("--port", type=int, default=30900)
    ap.add_argument("--asset", default="xrp", help="asset name (resolved to asset_id)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--tol", type=float, default=0.005, help="relative-diff tolerance for MATCH (default 0.5%%)")
    ap.add_argument("--table", default="all", choices=list(TABLE_PAIRS) + ["all"])
    ap.add_argument("--meta-table", default="metric_metadata",
                    help="table to resolve metric_id->name (prod: XRP adds no new ids)")
    args = ap.parse_args()

    client = Client(host=args.host, port=args.port, user="readonly")

    aid = client.execute(
        "SELECT asset_id FROM asset_metadata WHERE name = %(n)s ORDER BY computed_at DESC LIMIT 1",
        {"n": args.asset},
    )
    if not aid:
        sys.exit(f"asset {args.asset!r} not found in asset_metadata")
    aid = aid[0][0]

    names = dict(client.execute(f"SELECT metric_id, name FROM {args.meta_table} FINAL"))

    pairs = TABLE_PAIRS.items() if args.table == "all" else [(args.table, TABLE_PAIRS[args.table])]
    params = {"aid": aid, "start": args.start, "end": args.end, "tol": args.tol}

    print(f"# XRP odt-bucket discrepancy report  asset={args.asset}({aid})  window={args.start}..{args.end}  tol={args.tol}")
    print("# rel = SYMMETRIC |pv-ev|/mean(|pv|,|ev|) in [0,2]; distribution rolled up to (metric_id, dt, odt_hour)\n")
    for label, (prod, exp, valcol, group_cols) in pairs:
        groupcols = ", ".join(group_cols)
        sub = DIST_SUBQUERY if label == "distribution" else PLAIN_SUBQUERY
        sql = COMPARE_SQL.format(
            groupcols=groupcols,
            prod_subquery=sub.format(table=prod, valcol=valcol, groupcols=groupcols, alias="pv"),
            exp_subquery=sub.format(table=exp, valcol=valcol, groupcols=groupcols, alias="ev"),
        )
        print(f"== {label}:  {prod}  vs  {exp}  (col={valcol}, key={groupcols}) ==")
        try:
            rows = client.execute(sql, params)
        except Exception as ex:
            print(f"  query failed: {ex}\n")
            continue
        if not rows:
            print("  (experimental produced no rows for this asset/window)\n")
            continue
        hdr = ("metric_id", "name", "both", "prod_only", "exp_only",
               "p50_rel", "p99_rel", "max_rel", "mean_rel", "first_div", "prod_sum", "exp_sum", "sum_ratio", "verdict")
        print("  " + "  ".join(f"{h:<22}" if h == "name" else f"{h:<11}" for h in hdr))
        for r in rows:
            mid = r[0]
            prod_sum, exp_sum = r[10], r[11]
            sum_ratio = (exp_sum / prod_sum) if (prod_sum not in (None, 0)) else None
            line = [
                f"{mid:<11}",
                f"{names.get(mid, '?')[:22]:<22}",
                f"{r[1]:<11}", f"{r[2]:<11}", f"{r[3]:<11}",
                f"{fmt(r[7]):<11}", f"{fmt(r[8]):<11}", f"{fmt(r[5]):<11}", f"{fmt(r[6]):<11}",
                f"{str(r[9] or '-'):<11}",
                f"{fmt(prod_sum):<11}", f"{fmt(exp_sum):<11}", f"{fmt(sum_ratio, 4):<11}",
                verdict(r, args.tol),
            ]
            print("  " + "  ".join(line))
        print()


if __name__ == "__main__":
    main()
