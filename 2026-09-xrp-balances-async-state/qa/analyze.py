#!/usr/bin/env python3
"""Aggregate bucketed results.tsv (from compare_months.sh) and compare old vs new per month.

Usage: analyze.py <workdir> [-v]   (-v prints every month, not just failures)
"""
import sys
from collections import defaultdict

WORK = sys.argv[1]
M64 = 1 << 64

# per (month, src): [keys, kh_sum, ehmin_sum, ehmax_sum, bal, bal_abs, obal, obal_abs, spread, dups]
agg = defaultdict(lambda: [0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0])

for line in open(f"{WORK}/results.tsv"):
    f = line.rstrip("\n").split("\t")
    month, src = f[0], f[1]
    a = agg[(month, src)]
    a[0] += int(f[3])
    for i, col in ((1, 4), (2, 5), (3, 6)):
        a[i] = (a[i] + int(f[col])) % M64
    for i, col in ((4, 7), (5, 8), (6, 9), (7, 10)):
        a[i] += float(f[col])
    a[8] = max(a[8], 0.0 if f[11] == "\\N" else float(f[11]))
    a[9] += int(f[12])

months = sorted({m for m, _ in agg})
bad = []
print(f"{'month':>6} {'keys':>12} {'keyset':>6} {'exact':>6} {'balΔrel':>10} {'obalΔrel':>10} {'old_dups':>11} {'old_maxspread':>13}")
for m in months:
    o, n = agg.get((m, "old")), agg.get((m, "new"))
    if o is None or n is None:
        print(f"{m:>6}  MISSING {'old' if o is None else 'new'} side")
        bad.append(m)
        continue
    keys_ok = o[0] == n[0] and o[1] == n[1]
    # exact non-float cols: min==max within each table (internal consistency) and equal across
    exact_ok = o[2] == o[3] == n[2] == n[3]
    def rel(i, j):
        d = abs(o[i] - n[i])
        base = max(o[j], n[j], 1.0)
        return d / base
    bal_rel, obal_rel = rel(4, 5), rel(6, 7)
    floats_ok = bal_rel < 1e-9 and obal_rel < 1e-9
    ok = keys_ok and exact_ok and floats_ok
    if not ok or "-v" in sys.argv:
        print(f"{m:>6} {o[0]:>12} {'OK' if keys_ok else 'DIFF':>6} {'OK' if exact_ok else 'DIFF':>6} "
              f"{bal_rel:>10.2e} {obal_rel:>10.2e} {o[9]:>11} {o[8]:>13.3e} new_dups={n[9]} new_spread={n[8]:.3e}"
              + ("" if o[0] == n[0] else f"  keys old={o[0]} new={n[0]} d={n[0]-o[0]}"))
    if not ok:
        bad.append(m)
print(f"\n{len(months)} months compared, {len(bad)} with differences: {bad if bad else 'NONE'}")
new_dups = sum(a[9] for (m, s), a in agg.items() if s == "new")
new_spread = max((a[8] for (m, s), a in agg.items() if s == "new"), default=0.0)
old_spread = max((a[8] for (m, s), a in agg.items() if s == "old"), default=0.0)
print(f"new-table duplicate rows total: {new_dups}, max float spread within new dup versions: {new_spread:.3e}")
print(f"old-table max float spread within dup versions: {old_spread:.3e}")
