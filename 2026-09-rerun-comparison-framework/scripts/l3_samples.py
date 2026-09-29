"""L3: fetch the actual rows (old + new) for sampled differing keys from L2, one query per hour.
Usage: python3 l3_samples.py <category> [max_hours]   (category = s_bal|s_obal|s_optr|s_str|s_conf|s_noise)
Writes l3/<category>.tsv with side-labelled rows for eyeballing / root-cause classification."""
import csv, glob, os, sys, subprocess, json
import l1_fingerprints as L1

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'l3'); os.makedirs(OUT, exist_ok=True)
csv.field_size_limit(10**9)

ROWS = f"""
SELECT side, {L1.KEY}, {L1.VALS}, cityHash64({L1.KEY}) kh FROM (
  SELECT 'old' side, {L1.KEY}, {L1.VALS} FROM {L1.TABLES['old']} WHERE toStartOfHour(dt) = '{{hr}}' AND cityHash64({L1.KEY}) IN ({{khs}})
  UNION ALL
  SELECT 'new' side, {L1.KEY}, {L1.VALS} FROM {L1.TABLES['new']} WHERE toStartOfHour(dt) = '{{hr}}' AND cityHash64({L1.KEY}) IN ({{khs}}))
ORDER BY kh, side DESC
FORMAT TSVWithNames
"""


def samples(cat, max_hours):
    out = []
    for f in sorted(glob.glob(os.path.join(HERE, 'l2', '*.tsv'))):
        for r in csv.DictReader(open(f), delimiter='\t'):
            khs = json.loads(r[cat])
            if khs: out.append((r['hr'], khs))
    step = max(1, len(out) // max_hours)  # spread samples over the whole history
    return out[::step][:max_hours]


if __name__ == '__main__':
    cat, max_hours = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 40
    path = os.path.join(OUT, f'{cat}.tsv'); header = True
    with open(path, 'w') as fh:
        for hr, khs in samples(cat, max_hours):
            r = subprocess.run(['clickhouse-client', '-h', 'clickhouse.production.san', '--port', '30900', '-u', 'readonly',
                                f'--log_comment={L1.LOG}', '--query', ROWS.format(hr=hr, khs=', '.join(map(str, khs)))],
                               capture_output=True, text=True, check=True)
            lines = r.stdout.splitlines()
            fh.write('\n'.join(lines if header else lines[1:]) + '\n'); header = False
    print(path)
