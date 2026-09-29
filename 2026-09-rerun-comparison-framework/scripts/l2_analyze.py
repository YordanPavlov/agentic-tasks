"""Roll up L2 per-hour field-diff results into per-month and grand totals."""
import csv, glob, os, collections, json
HERE = os.path.dirname(os.path.abspath(__file__))
csv.field_size_limit(10**9)

COUNT_COLS = ['only_new', 'only_old', 'same', 'conflict_match', 'conflict_nomatch', 'new_conflicts', 'differ',
              'differ_xrp', 'bal_ne', 'bal_tol', 'obal_ne', 'obal_tol', 'odt_ne', 'oblk_ne', 'cur_ne', 'atype_ne', 'tx_ne']
MAX_COLS = ['bal_maxabs', 'bal_maxrel']
SAMPLE_COLS = ['s_bal', 's_obal', 's_optr', 's_str', 's_conf', 's_noise']


def load():
    for f in sorted(glob.glob(os.path.join(HERE, 'l2', '*.tsv'))):
        yield from csv.DictReader(open(f), delimiter='\t')


if __name__ == '__main__':
    mon = collections.defaultdict(collections.Counter); mx = collections.defaultdict(lambda: collections.defaultdict(float))
    tot = collections.Counter(); tmx = collections.defaultdict(float); xrp_sum = collections.defaultdict(float)
    hours_with = collections.Counter()
    for r in load():
        m = r['hr'][:7]
        for c in COUNT_COLS:
            v = int(r[c]); mon[m][c] += v; tot[c] += v
            if v: hours_with[c] += 1
        for c in MAX_COLS:
            v = float(r[c]); mx[m][c] = max(mx[m][c], v); tmx[c] = max(tmx[c], v)
        xrp_sum[m] += float(r['xrp_bal_sumdiff'])
    print('TOTAL', dict(tot)); print('MAX', dict(tmx)); print('HOURS_WITH', dict(hours_with))
    cols = ['same', 'conflict_match', 'conflict_nomatch', 'differ', 'bal_tol', 'obal_tol', 'odt_ne', 'oblk_ne', 'cur_ne', 'atype_ne', 'tx_ne', 'only_new', 'only_old']
    print('month   ' + ' '.join(f'{c:>14}' for c in cols) + '   bal_maxrel  xrp_sumdiff')
    for m in sorted(mon):
        print(m, ' '.join(f'{mon[m][c]:>14}' for c in cols), f"{mx[m]['bal_maxrel']:.2e}", f'{xrp_sum[m]:.6g}')
