#!/usr/bin/env python3
"""Compare production LTC metrics against the experimental (balances-derived) run.

For LTC (asset_id resolved from asset_metadata), for every metric the experimental
run actually produced, compare the prod table vs its `_experimental` variant over a
date window and report, per metric_id:

  both        points present in BOTH prod and experimental (the comparable set)
  prod_only   dates in prod but not yet computed in experimental (coverage gap)
  exp_only    dates in experimental but absent from prod (new metric / extra)
  max_rel     max |exp-prod| / |prod| over `both`        (the headline divergence)
  mean_rel    mean relative diff over `both`
  max_abs     max absolute diff over `both`
  first_div   earliest dt where rel diff exceeds --tol   (where it starts breaking)
  verdict     MATCH / minor-drift / DIVERGE / EXP-ONLY / no-overlap

Dedups ReplacingMergeTree rows via argMax(value, computed_at). Reads prod + experimental
from the SAME host (they differ only by table name). No writes.

Usage:
  ../.venv/bin/python analysis/compare_ltc_experimental.py \
      --start 2011-10-01 --end 2014-10-01 [--table daily|intraday|distribution|all] [--tol 0.005]
"""
import argparse
import sys

from clickhouse_driver import Client

# label -> (prod_table, experimental_table, numeric_column, group_cols)
# distribution_deltas_5min keeps its numeric quantity in `measure` (its `value` is a DateTime/odt),
# and has MANY rows per (metric_id, dt) — one per odt cohort. So it must be joined on
# (metric_id, dt, value=odt), NOT collapsed to one row per (metric_id, dt) (which would compare
# arbitrary odt buckets and report phantom divergence). daily/intraday have one row per
# (metric_id, dt), so their numeric column IS `value` and the key is (metric_id, dt).
TABLE_PAIRS = {
    "daily":        ("daily_metrics_v2",         "daily_metrics_v2_experimental",         "value",   ["metric_id", "dt"]),
    "intraday":     ("intraday_metrics",         "intraday_metrics_experimental",         "value",   ["metric_id", "dt"]),
    "distribution": ("distribution_deltas_5min", "distribution_deltas_5min_experimental", "measure", ["metric_id", "dt", "value"]),
}

# SYMMETRIC relative diff: |pv-ev| / mean(|pv|,|ev|), bounded to [0, 2].
# The old one-sided |pv-ev|/|pv| exploded to 1e22 on (a) *_delta metrics that cross zero and
# (b) *_squared_* metrics (tau^2 ~ 1e18) wherever prod was momentarily near zero — both are
# denominator artifacts, not real divergence. The symmetric form caps a "one side is zero"
# point at 2.0, so genuine drift (small) stays readable and zero-crossings stop dominating.
COMPARE_SQL = """
SELECT
    metric_id,
    countIf(pv IS NOT NULL AND ev IS NOT NULL)                                       AS both,
    countIf(pv IS NOT NULL AND ev IS NULL)                                           AS prod_only,
    countIf(pv IS NULL  AND ev IS NOT NULL)                                          AS exp_only,
    maxIf(abs(pv - ev),                                            pv IS NOT NULL AND ev IS NOT NULL) AS max_abs,
    maxIf(abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12),     pv IS NOT NULL AND ev IS NOT NULL) AS max_rel,
    avgIf(abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12),     pv IS NOT NULL AND ev IS NOT NULL) AS mean_rel,
    minIf(dt, pv IS NOT NULL AND ev IS NOT NULL
              AND abs(pv - ev) / greatest((abs(pv)+abs(ev))/2, 1e-12) > %(tol)s)      AS first_div,
    -- Deduped Σ over the window (the inner argMax already collapses ReplacingMergeTree
    -- row-versions, so these are FINAL-equivalent totals). Surfaced so absolute levels and
    -- cross-metric conservation (e.g. daily stack_age_consumed Σ == its _5min Σ) are visible
    -- without a separate ad-hoc query — a raw non-deduped sum(value) double-counts unmerged
    -- versions and manufactured a phantom "5min = 2x daily" once.
    sumIf(pv, pv IS NOT NULL)                                                         AS prod_sum,
    sumIf(ev, ev IS NOT NULL)                                                         AS exp_sum
FROM
(
    SELECT {groupcols}, argMax({valcol}, computed_at) AS pv
    FROM {prod} WHERE asset_id = %(aid)s AND dt BETWEEN %(start)s AND %(end)s
    GROUP BY {groupcols}
) p
FULL OUTER JOIN
(
    SELECT {groupcols}, argMax({valcol}, computed_at) AS ev
    FROM {exp} WHERE asset_id = %(aid)s AND dt BETWEEN %(start)s AND %(end)s
    GROUP BY {groupcols}
) e
USING ({groupcols})
GROUP BY metric_id
HAVING countIf(ev IS NOT NULL) > 0          -- only metrics the experimental run produced
ORDER BY metric_id
SETTINGS join_use_nulls = 1
"""


def verdict(row, tol):
    # Classify on MEAN rel, not MAX: with the symmetric metric a single near-zero-crossing day
    # (a *_delta point, or a sparse early-history day) still spikes max_rel toward 2 while the
    # series is otherwise a clean match. mean_rel is the series-level signal; max_rel/max_abs are
    # shown alongside for the spot-check. (Was the old behaviour — max_rel — that flagged
    # everything DIVERGE off one outlier day.)
    both, prod_only, exp_only, max_abs, max_rel, mean_rel, first_div = row[1:8]
    if both == 0:
        return "EXP-ONLY (no prod counterpart)" if exp_only else "no-overlap"
    if mean_rel is None:
        return "MATCH"
    if mean_rel <= tol:
        return "MATCH"
    if mean_rel <= 0.05:
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
    ap.add_argument("--asset", default="litecoin", help="asset name (resolved to asset_id)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--tol", type=float, default=0.005, help="relative-diff tolerance for MATCH (default 0.5%%)")
    ap.add_argument("--table", default="all", choices=list(TABLE_PAIRS) + ["all"])
    ap.add_argument("--meta-table", default="metric_metadata_experimental",
                    help="table to resolve metric_id->name (has prod + new ids)")
    args = ap.parse_args()

    client = Client(host=args.host, port=args.port)

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

    print(f"# LTC discrepancy report  asset={args.asset}({aid})  window={args.start}..{args.end}  tol={args.tol}")
    print("# rel = SYMMETRIC |pv-ev|/mean(|pv|,|ev|), bounded [0,2]; distribution joined on (metric_id,dt,odt)\n")
    for label, (prod, exp, valcol, group_cols) in pairs:
        groupcols = ", ".join(group_cols)
        print(f"== {label}:  {prod}  vs  {exp}  (col={valcol}, key={groupcols}) ==")
        try:
            rows = client.execute(COMPARE_SQL.format(prod=prod, exp=exp, valcol=valcol, groupcols=groupcols), params)
        except Exception as ex:
            print(f"  query failed: {ex}\n")
            continue
        if not rows:
            print("  (experimental produced no rows for this asset/window)\n")
            continue
        hdr = ("metric_id", "name", "both", "prod_only", "exp_only",
               "max_rel", "mean_rel", "max_abs", "first_div", "prod_sum", "exp_sum", "sum_ratio", "verdict")
        print("  " + "  ".join(f"{h:<14}" if h == "name" else f"{h:<11}" for h in hdr))
        for r in rows:
            mid = r[0]
            prod_sum, exp_sum = r[8], r[9]
            sum_ratio = (exp_sum / prod_sum) if (prod_sum not in (None, 0)) else None
            line = [
                f"{mid:<11}",
                f"{names.get(mid, '?')[:14]:<14}",
                f"{r[1]:<11}", f"{r[2]:<11}", f"{r[3]:<11}",
                f"{fmt(r[5]):<11}", f"{fmt(r[6]):<11}", f"{fmt(r[4]):<11}",
                f"{str(r[7] or '-'):<11}",
                f"{fmt(prod_sum):<11}", f"{fmt(exp_sum):<11}", f"{fmt(sum_ratio, 4):<11}",
                verdict(r, args.tol),
            ]
            print("  " + "  ".join(line))
        print()


if __name__ == "__main__":
    main()
