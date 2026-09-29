"""L1: exhaustive per-hour dedup fingerprints of old vs new balances table.
At most 2 queries in flight. Resumable: one cached TSV per (side, window)."""
import subprocess, os, sys, math, json, datetime as D
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'l1v3'); os.makedirs(OUT, exist_ok=True)
TABLES = {'old': 'default.xrp_balances', 'new': 'test.xrp_balances_test'}
START, CUTOFF = D.date(2013, 1, 1), D.date(2026, 9, 26)
TARGET_ROWS = 30_000_000
KEY = 'dt, assetRefId, address, blockNumber, transactionIndex'
VALS = 'balance, oldDt, oldBlockNumber, oldBalance, currency, issuer, issuerCurrency, addressType, transactionHash'
# cityHash64 returns NULL if ANY arg is NULL -> every nullable gets an explicit isNull flag + non-null default.
HVALS = ('balance, isNull(oldDt), ifNull(oldDt, toDateTime(0)), isNull(oldBlockNumber), ifNull(oldBlockNumber, 0), '
         'isNull(oldBalance), ifNull(oldBalance, 0), currency, isNull(issuer), ifNull(issuer, \'\'), issuerCurrency, '
         'addressType, transactionHash')
LOG = json.dumps({'job': 'xrp-balances-compare', 'owner': 'yordan.p@santiment.net', 'team': 'bigdata',
                  'repo': 'clickhouse-tables', 'dag': 'manual-tests'})

SQL = f"""
SELECT hr, count() keys, sum(c) rows, countIf(c>1) dup_keys, countIf(mn!=mx) conflict_keys, sum(mn) fp, sum(kh) key_fp, sumIf(mn, mn=mx) fp_nc,
       countIf(x) xrp_keys, sumIf(d, x) xrp_delta, sumIf(abs(d), x) xrp_absdelta, uniqExact(blk) blocks
FROM (
  SELECT toStartOfHour(dt) hr, cityHash64({KEY}) kh, min(rh) mn, max(rh) mx, count() c,
         any(issuerCurrency = 'XRP') x, argMin(balance - ifNull(oldBalance, 0), rh) d, any(blockNumber) blk
  FROM (SELECT {KEY}, {VALS}, assumeNotNull(cityHash64({KEY}, {HVALS})) rh FROM {{table}} WHERE dt >= '{{lo}}' AND dt < '{{hi}}')
  GROUP BY hr, kh)
GROUP BY hr ORDER BY hr FORMAT TSV
"""

def month_rows():
    rows = {}
    for line in open(os.path.join(HERE, '..', 'parts.tsv')):
        t, p, n = line.split('\t'); rows[p] = max(rows.get(p, 0), int(n) * 3)
    return rows

def windows():
    mr = month_rows(); d = START
    while d < CUTOFF:
        nm = (d.replace(day=28) + D.timedelta(days=4)).replace(day=1)
        end = min(nm, CUTOFF); days = (end - d).days
        step = max(1, math.floor(days * TARGET_ROWS / max(mr.get(d.strftime('%Y%m'), 1), 1)))
        while d < end:
            e = min(d + D.timedelta(days=step), end); yield d, e; d = e

def run(side, lo, hi):
    path = os.path.join(OUT, f'{side}_{lo}_{hi}.tsv')
    if os.path.exists(path): return path, 0
    sql = SQL.format(table=TABLES[side], lo=lo, hi=hi)
    t0 = D.datetime.now()
    for attempt in range(3):
        r = subprocess.run(['clickhouse-client', '-h', 'clickhouse.production.san', '--port', '30900', '-u', 'readonly',
                            f'--log_comment={LOG}', '--max_execution_time=900', '--query', sql],
                           capture_output=True, text=True)
        if r.returncode == 0: break
    else:
        raise RuntimeError(f'{side} {lo}: {r.stderr[:500]}')
    open(path + '.tmp', 'w').write(r.stdout); os.rename(path + '.tmp', path)
    return path, (D.datetime.now() - t0).total_seconds()

if __name__ == '__main__':
    jobs = [(s, lo, hi) for lo, hi in windows() for s in ('old', 'new')]
    print(f'{len(jobs)} jobs', flush=True)
    done = 0
    with ThreadPoolExecutor(max_workers=2) as ex:
        futs = {ex.submit(run, *j): j for j in jobs}
        for f in as_completed(futs):
            done += 1; p, secs = f.result()
            if secs: print(f'{done}/{len(jobs)} {futs[f]} {secs:.1f}s', flush=True)
