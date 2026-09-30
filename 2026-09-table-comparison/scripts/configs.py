"""One entry per comparison. Required: old, new (tables, Distributed or local), dt, start, cutoff (dates, cutoff exclusive).
Optional (see table-comparison.md, "Config"):
  block, tx     column names; None skips the blocks/txs checks
  key           'new' (default) | 'old' | explicit list of key expressions
  exclude       value columns left out of the comparison (e.g. computed_at)
  split_by      extra Tier 1 grouping columns besides the day (e.g. ['metric_id'])
  where, where_old, where_new   extra filters (both sides / one side)
  agg_checks    {name: dict(group=[cols], value=expr)} for tier2_agg.py
"""
CONFIGS = {
    'xrp_balances': dict(old='default.xrp_balances', new='test.xrp_balances_test',
                         dt='dt', block='blockNumber', tx='transactionHash',
                         start='2013-01-01', cutoff='2026-09-26',
                         agg_checks={'net_change': dict(group=['assetRefId'], value='balance - oldBalance')}),
    'xrp_stacks': dict(old='default.xrp_stacks', new='test.xrp_stacks_test',
                       dt='dt', block='blockNumber', tx='txID',
                       start='2013-01-01', cutoff='2026-09-03',
                       agg_checks={'net_change': dict(group=['contractAddress', 'address', 'blockNumber'], value='sign * amount'),
                                   'age_dist': dict(group=['contractAddress', 'address', 'blockNumber', 'toDate(odt)'],
                                                    value='sign * amount'),
                                   # exact odt: also catches a changed consumption order within a day
                                   'age_exact': dict(group=['contractAddress', 'address', 'blockNumber', 'odt'],
                                                     value='sign * amount')}),
}
