"""L2: for hours whose L1 fingerprints differ, FULL JOIN old/new per key IN the DB and return
per-hour category + per-field diff counts, tolerance checks and sample key hashes. No row export.
At most 2 queries in flight. Resumable: one cached TSV per day."""
import subprocess, os, json, collections, datetime as D
from concurrent.futures import ThreadPoolExecutor, as_completed
import l1_fingerprints as L1

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'l2'); os.makedirs(OUT, exist_ok=True)
RTOL, ATOL = 1e-9, 1e-12

SIDE = f"""
SELECT toStartOfHour(dt) hr, cityHash64({L1.KEY}) kh, min(rh) {{p}}mn, max(rh) {{p}}mx, count() {{p}}c,
       argMin(tuple(balance, oldBalance, ifNull(toUnixTimestamp(oldDt), -1), ifNull(toInt64(oldBlockNumber), -1),
                    cityHash64(currency, ifNull(issuer, ''), issuerCurrency), cityHash64(addressType),
                    cityHash64(transactionHash), issuerCurrency = 'XRP'), rh) {{p}}v
FROM (SELECT {L1.KEY}, {L1.VALS}, assumeNotNull(cityHash64({L1.KEY}, {L1.HVALS})) rh
      FROM {{table}} WHERE dt >= '{{lo}}' AND dt < '{{hi}}' AND toStartOfHour(dt) IN ({{hours}}))
GROUP BY hr, kh
"""

def close(a, b):  # SQL isclose
    return f"(abs({a} - {b}) <= greatest({RTOL} * greatest(abs({a}), abs({b})), {ATOL}))"

SQL = f"""
SELECT hr,
  countIf(o_c = 0) only_new, countIf(n_c = 0) only_old,
  countIf(both AND (n_mn = o_mn OR n_mn = o_mx)) same,
  countIf(both AND o_mn != o_mx AND (n_mn = o_mn OR n_mn = o_mx)) conflict_match,
  countIf(both AND o_mn != o_mx AND (n_mn != o_mn AND n_mn != o_mx)) conflict_nomatch,
  countIf(n_mn != n_mx) new_conflicts,
  countIf(cmp) differ,
  countIf(cmp AND tupleElement(o_v, 8)) differ_xrp,
  countIf(cmp AND tupleElement(o_v, 1) != tupleElement(n_v, 1)) bal_ne,
  countIf(cmp AND NOT {close('tupleElement(o_v, 1)', 'tupleElement(n_v, 1)')}) bal_tol,
  max(if(cmp, abs(tupleElement(o_v, 1) - tupleElement(n_v, 1)), 0)) bal_maxabs,
  max(if(cmp, abs(tupleElement(o_v, 1) - tupleElement(n_v, 1)) / greatest(abs(tupleElement(o_v, 1)), abs(tupleElement(n_v, 1)), 1e-300), 0)) bal_maxrel,
  countIf(cmp AND NOT (tupleElement(o_v, 2) IS NULL AND tupleElement(n_v, 2) IS NULL) AND (isNull(tupleElement(o_v, 2)) != isNull(tupleElement(n_v, 2)) OR tupleElement(o_v, 2) != tupleElement(n_v, 2))) obal_ne,
  countIf(cmp AND (isNull(tupleElement(o_v, 2)) != isNull(tupleElement(n_v, 2)) OR NOT {close('ifNull(tupleElement(o_v, 2),0)', 'ifNull(tupleElement(n_v, 2),0)')})) obal_tol,
  countIf(cmp AND tupleElement(o_v, 3) != tupleElement(n_v, 3)) odt_ne,
  countIf(cmp AND tupleElement(o_v, 4) != tupleElement(n_v, 4)) oblk_ne,
  countIf(cmp AND tupleElement(o_v, 5) != tupleElement(n_v, 5)) cur_ne,
  countIf(cmp AND tupleElement(o_v, 6) != tupleElement(n_v, 6)) atype_ne,
  countIf(cmp AND tupleElement(o_v, 7) != tupleElement(n_v, 7)) tx_ne,
  sumIf(tupleElement(n_v, 1) - tupleElement(o_v, 1), cmp AND tupleElement(o_v, 8)) xrp_bal_sumdiff,
  groupArraySampleIf(5, 1)(kh, cmp AND NOT {close('tupleElement(o_v, 1)', 'tupleElement(n_v, 1)')}) s_bal,
  groupArraySampleIf(5, 1)(kh, cmp AND (isNull(tupleElement(o_v, 2)) != isNull(tupleElement(n_v, 2)) OR NOT {close('ifNull(tupleElement(o_v, 2),0)', 'ifNull(tupleElement(n_v, 2),0)')})) s_obal,
  groupArraySampleIf(5, 1)(kh, cmp AND (tupleElement(o_v, 3) != tupleElement(n_v, 3) OR tupleElement(o_v, 4) != tupleElement(n_v, 4))) s_optr,
  groupArraySampleIf(5, 1)(kh, cmp AND (tupleElement(o_v, 5) != tupleElement(n_v, 5) OR tupleElement(o_v, 6) != tupleElement(n_v, 6) OR tupleElement(o_v, 7) != tupleElement(n_v, 7))) s_str,
  groupArraySampleIf(5, 1)(kh, both AND o_mn != o_mx AND (n_mn != o_mn AND n_mn != o_mx)) s_conf,
  groupArraySampleIf(5, 1)(kh, cmp AND {close('tupleElement(o_v, 1)', 'tupleElement(n_v, 1)')} AND NOT (isNull(tupleElement(o_v, 2)) != isNull(tupleElement(n_v, 2)) OR NOT {close('ifNull(tupleElement(o_v, 2),0)', 'ifNull(tupleElement(n_v, 2),0)')}) AND tupleElement(o_v, 3) = tupleElement(n_v, 3) AND tupleElement(o_v, 4) = tupleElement(n_v, 4) AND tupleElement(o_v, 5) = tupleElement(n_v, 5) AND tupleElement(o_v, 6) = tupleElement(n_v, 6) AND tupleElement(o_v, 7) = tupleElement(n_v, 7)) s_noise
FROM (SELECT *, (o_c > 0 AND n_c > 0) AS both, (both AND o_mn = o_mx AND n_mn != o_mn) AS cmp
      FROM ({SIDE.format(table=L1.TABLES['old'], lo='{lo}', hi='{hi}', hours='{hours}', p='o_')}) o
      FULL OUTER JOIN ({SIDE.format(table=L1.TABLES['new'], lo='{lo}', hi='{hi}', hours='{hours}', p='n_')}) n
      USING (hr, kh))
GROUP BY hr ORDER BY hr
SETTINGS join_use_nulls = 0
FORMAT TSVWithNames
"""


def differing_hours():
    """Hours from the L1 cache where both sides exist and fingerprints differ, grouped by day."""
    import l1_analyze as A
    old, new = A.load('old'), A.load('new')
    days = collections.defaultdict(list)
    for h in sorted(set(old) & set(new)):
        if old[h]['fp'] != new[h]['fp'] or old[h]['key_fp'] != new[h]['key_fp']:
            days[h[:10]].append(h)
    return days


def run(day, hours):
    path = os.path.join(OUT, f'{day}.tsv')
    if os.path.exists(path): return path, 0
    lo = D.date.fromisoformat(day); hi = lo + D.timedelta(days=1)
    sql = SQL.format(lo=lo, hi=hi, hours=', '.join(f"'{h}'" for h in hours))
    t0 = D.datetime.now()
    for _ in range(3):
        r = subprocess.run(['clickhouse-client', '-h', 'clickhouse.production.san', '--port', '30900', '-u', 'readonly',
                            f'--log_comment={L1.LOG}', '--max_execution_time=900', '--query', sql],
                           capture_output=True, text=True)
        if r.returncode == 0: break
    else:
        raise RuntimeError(f'{day}: {r.stderr[:800]}')
    open(path + '.tmp', 'w').write(r.stdout); os.rename(path + '.tmp', path)
    return path, (D.datetime.now() - t0).total_seconds()


if __name__ == '__main__':
    import sys
    days = differing_hours()
    todo = sorted(days.items())
    if len(sys.argv) > 1: todo = [d for d in todo if d[0] in sys.argv[1:]]
    print(f'{len(todo)} days, {sum(len(h) for _, h in todo)} hours', flush=True)
    with ThreadPoolExecutor(max_workers=int(os.environ.get('WORKERS', 2))) as ex:
        futs = {ex.submit(run, d, h): d for d, h in todo}
        for i, f in enumerate(as_completed(futs), 1):
            p, secs = f.result()
            if secs: print(f'{i}/{len(todo)} {futs[f]} {secs:.1f}s', flush=True)
