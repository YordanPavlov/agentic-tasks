"""Join old/new hourly L1 fingerprints; classify each hour; print per-month rollup."""
import glob, os, sys, collections
HERE = os.path.dirname(os.path.abspath(__file__)); L1 = os.path.join(HERE, 'l1v3')
COLS = ['hr','keys','rows','dup_keys','conflict_keys','fp','key_fp','fp_nc','xrp_keys','xrp_delta','xrp_absdelta','blocks']

def load(side):
    d = {}
    for f in sorted(glob.glob(f'{L1}/{side}_*.tsv')):
        for line in open(f):
            r = dict(zip(COLS, line.rstrip('\n').split('\t')))
            for c in COLS[1:]:
                r[c] = 0 if r[c] == '\\N' else (float(r[c]) if c in ('xrp_delta','xrp_absdelta') else int(r[c]))
            d[r['hr']] = r
    return d

def classify(o, n):
    if o is None: return 'missing_in_old'
    if n is None: return 'missing_in_new'
    if o['key_fp'] != n['key_fp'] or o['keys'] != n['keys']: return 'keyset_diff'
    if o['fp'] == n['fp']: return 'identical'
    if o['conflict_keys'] and o['fp_nc'] == n['fp_nc'] - 0 or False: pass
    return 'content_diff'

if __name__ == '__main__':
    old, new = load('old'), load('new')
    hours = sorted(set(old) & set(new) | set(old) | set(new))
    both_done = {h for h in hours if h in old and h in new}
    out = open(os.path.join(HERE, 'l1_hours.tsv'), 'w')
    mon = collections.defaultdict(lambda: collections.Counter())
    for h in hours:
        o, n = old.get(h), new.get(h); c = classify(o, n)
        out.write('\t'.join(map(str, [h, c, o and o['keys'], n and n['keys'], o and o['rows'], n and n['rows'],
              o and o['dup_keys'], o and o['conflict_keys'], n and n['dup_keys'], n and n['conflict_keys'],
              o and o['blocks'], n and n['blocks'], o and o['xrp_delta'], n and n['xrp_delta']])) + '\n')
        m = mon[h[:7]]; m[c] += 1
        if o: m['old_rows'] += o['rows']; m['old_keys'] += o['keys']; m['old_dup'] += o['dup_keys']; m['old_conf'] += o['conflict_keys']
        if n: m['new_rows'] += n['rows']; m['new_keys'] += n['keys']; m['new_dup'] += n['dup_keys']; m['new_conf'] += n['conflict_keys']
    print('month   hours: ident content keyset miss_old miss_new | old rows/keys/dup/conf | new rows/keys/dup/conf')
    for k in sorted(mon):
        m = mon[k]
        print(f"{k} {m['identical']:5} {m['content_diff']:5} {m['keyset_diff']:5} {m['missing_in_old']:4} {m['missing_in_new']:4} | "
              f"{m['old_rows']:>11} {m['old_keys']:>11} {m['old_dup']:>10} {m['old_conf']:>7} | {m['new_rows']:>11} {m['new_keys']:>11} {m['new_dup']:>6} {m['new_conf']:>5}")
