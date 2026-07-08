#!/usr/bin/env python3
"""
Build a colored dependency map of Santiment's on-chain metrics.

Color = which raw coin-data SOURCE the metric (transitively) needs:
    R red   = *_transfers
    G green = *_balances
    B blue  = *_stacks  (the legacy per-cohort coin model we want to deprecate)
A metric carries 1-3 of these colors (striped node = needs >1 source).
Grey = price feed (external; NOT a coin-data source, shown for honesty on *_usd metrics).

Edges point  dependency --> dependent.

Structure/edges were extracted & verified from daily_metrics/specs.d/metrics/*.yaml
(spec.dependsOn) plus the leaf-producing jobs' source tables (e.g. age_distribution
reads distribution_deltas_5min, which is built from *_stacks; age_balances after migration).

Output: metrics_map.dot  ->  render with:  dot -Tsvg metrics_map.dot -o metrics_map.svg
"""

# ---- palette ----
COL = {'R': '#e06666', 'G': '#93c47d', 'B': '#6fa8dc'}   # transfers / balances / stacks
GREY = '#d9d9d9'

# ---- nodes: id -> (label, colorset)   colorset is subset of "RGB" (order-insensitive) ----
N = {}
def node(nid, label, colors): N[nid] = (label, colors)

# sources (drawn as cylinders)
SOURCES = {
    'S_TX':     ('*_transfers',                       'R'),
    'S_BAL':    ('*_balances\\n(balance + avg-birth age)', 'G'),
    'S_STACKS': ('*_stacks\\n(legacy per-cohort coin model)', 'B'),
}

# the coin-age primitive (the migration pivot: stacks today -> age_balances tomorrow)
node('age_distribution_5min_delta', 'age_distribution_5min_delta\\n(per-cohort coin-age deltas)', 'B')

# --- circulation family (pure stacks) ---
node('stack_circulation_delta_T', 'stack_circulation_delta_T', 'B')
node('stack_circulation_T',       'stack_circulation_T', 'B')
node('nvt',                       'nvt', 'B')
node('stock_to_flow_ratio',       'stock_to_flow_ratio', 'B')
node('token_velocity',            'token_velocity', 'BR')
node('nvt_transaction_volume',    'nvt_transaction_volume', 'BR')

# --- realized-value family (stacks; price folded in) ---
node('stack_realized_cap_usd_delta', 'stack_realized_cap_usd_delta', 'B')
node('stack_realized_cap_usd_T',     'stack_realized_cap_usd_T', 'B')
node('mean_realized_price_usd_T',    'mean_realized_price_usd_T', 'B')
node('realized_cap_hodl_waves_T',    'realized_cap_hodl_waves_T', 'B')
node('mvrv_usd',                     'mvrv_usd', 'B')
node('mvrv_z',                       'mvrv_z', 'BG')

# --- age / dormancy / profit family (pure stacks) ---
node('stack_age_consumed',           'stack_age_consumed', 'B')
node('stack_liveliness',             'stack_liveliness', 'B')
node('stack_mean_age_dollar_days',   'stack_mean_age_dollar_days', 'B')
node('dormant_circulation',          'dormant_circulation', 'B')
node('dormant_circulation_usd',      'dormant_circulation_usd', 'B')
node('spent_coins_age_band_T',       'spent_coins_age_band_T', 'B')
node('network_profit_loss',          'network_profit_loss', 'B')
node('total_supply_in_profit',       'total_supply_in_profit', 'B')
node('percent_of_total_supply_in_profit', 'percent_of_total_supply_in_profit', 'B')

# --- supply / marketcap PIVOT (blue only because total_supply = stack_circulation_20y) ---
node('custom_total_supply_delta',    'custom_total_supply_delta', 'G')
node('total_supply',                 'total_supply', 'BG')
node('daily_marketcap_usd',          'daily_marketcap_usd', 'BG')
node('fully_diluted_valuation_usd',  'fully_diluted_valuation_usd', 'BG')
node('bitcoin_dominance',            'bitcoin_dominance', 'BG')
node('annual_inflation_rate',        'annual_inflation_rate', 'BG')
node('non_exchange_token_supply',    'non_exchange_token_supply', 'BG')
node('percent_of_total_supply_on_exchanges', 'percent_of_total_supply_on_exchanges', 'BG')
PIVOT = {'total_supply','daily_marketcap_usd','fully_diluted_valuation_usd','bitcoin_dominance',
         'annual_inflation_rate','non_exchange_token_supply','percent_of_total_supply_on_exchanges'}

# --- balances family (green) ---
node('exchange_token_supply',        'exchange_token_supply', 'G')
node('holders_distribution_T',       'holders_distribution_T', 'G')
node('active_holders_distribution_T','active_holders_distribution_T', 'G')

# --- transfers family (red) ---
node('transaction_volume',           'transaction_volume', 'R')
node('transaction_count',            'transaction_count', 'R')
node('payment_count',                'payment_count', 'R')
node('daily_active_addresses',       'daily_active_addresses', 'R')
node('network_growth',               'network_growth', 'R')
node('whale_transaction_count_usd',  'whale_transaction_count_>usd', 'R')
node('whale_transaction_volume_usd', 'whale_transaction_volume_>usd', 'R')
node('daa_divergence',               'daa_divergence', 'R')

# ---- edges: (dependency, dependent) ----
E = [
    ('S_STACKS','age_distribution_5min_delta'),
    # circulation
    ('age_distribution_5min_delta','stack_circulation_delta_T'),
    ('stack_circulation_delta_T','stack_circulation_T'),
    ('stack_circulation_T','nvt'),
    ('stack_circulation_T','stock_to_flow_ratio'),
    ('stack_circulation_delta_T','stock_to_flow_ratio'),
    ('stack_circulation_T','token_velocity'),
    ('transaction_volume','token_velocity'),
    ('stack_circulation_T','nvt_transaction_volume'),
    ('transaction_volume','nvt_transaction_volume'),
    # realized
    ('age_distribution_5min_delta','stack_realized_cap_usd_delta'),
    ('stack_realized_cap_usd_delta','stack_realized_cap_usd_T'),
    ('stack_circulation_T','mean_realized_price_usd_T'),
    ('stack_realized_cap_usd_T','mean_realized_price_usd_T'),
    ('stack_realized_cap_usd_T','realized_cap_hodl_waves_T'),
    ('mean_realized_price_usd_T','mvrv_usd'),
    ('stack_realized_cap_usd_T','mvrv_z'),
    ('daily_marketcap_usd','mvrv_z'),
    # age / dormancy / profit
    ('S_STACKS','stack_age_consumed'),
    ('stack_age_consumed','stack_liveliness'),
    ('age_distribution_5min_delta','stack_mean_age_dollar_days'),
    ('age_distribution_5min_delta','dormant_circulation'),
    ('dormant_circulation','dormant_circulation_usd'),
    ('age_distribution_5min_delta','spent_coins_age_band_T'),
    ('age_distribution_5min_delta','network_profit_loss'),
    ('age_distribution_5min_delta','total_supply_in_profit'),
    ('total_supply_in_profit','percent_of_total_supply_in_profit'),
    ('stack_circulation_T','percent_of_total_supply_in_profit'),
    # supply / marketcap pivot
    ('S_BAL','custom_total_supply_delta'),
    ('custom_total_supply_delta','total_supply'),
    ('stack_circulation_T','total_supply'),
    ('total_supply','daily_marketcap_usd'),
    ('total_supply','fully_diluted_valuation_usd'),
    ('daily_marketcap_usd','bitcoin_dominance'),
    ('total_supply','annual_inflation_rate'),
    ('stack_circulation_T','non_exchange_token_supply'),
    ('exchange_token_supply','non_exchange_token_supply'),
    ('stack_circulation_T','percent_of_total_supply_on_exchanges'),
    ('exchange_token_supply','percent_of_total_supply_on_exchanges'),
    # balances
    ('S_BAL','exchange_token_supply'),
    ('S_BAL','holders_distribution_T'),
    ('S_BAL','active_holders_distribution_T'),
    # transfers
    ('S_TX','transaction_volume'),
    ('S_TX','transaction_count'),
    ('S_TX','payment_count'),
    ('S_TX','daily_active_addresses'),
    ('S_TX','network_growth'),
    ('S_TX','whale_transaction_count_usd'),
    ('S_TX','whale_transaction_volume_usd'),
    ('daily_active_addresses','daa_divergence'),
]

# grey price-feed dependents (drawn as dashed grey edges from PRICE)
PRICE_TO = ['stack_realized_cap_usd_delta','mvrv_usd','mvrv_z','dormant_circulation_usd',
            'network_profit_loss','total_supply_in_profit','daily_marketcap_usd',
            'fully_diluted_valuation_usd','daa_divergence']

# ---- emit DOT ----
def fill(colors):
    cs = [COL[c] for c in 'RGB' if c in colors]   # stable R,G,B order
    if len(cs) == 1:
        return f'style="filled,rounded" fillcolor="{cs[0]}"'
    return f'style="striped,filled" fillcolor="{":".join(cs)}"'

out = []
out.append('digraph metrics {')
out.append('  rankdir=LR; bgcolor="white";')
out.append('  node [shape=box style="filled,rounded" fontname="Helvetica" fontsize=10 color="#444444" penwidth=1.2];')
out.append('  edge [color="#888888" arrowsize=0.7];')
out.append('  PRICE [shape=diamond style="filled" fillcolor="%s" label="price_usd\\n(price feed - external)"];' % GREY)
# sources
out.append('  { rank=source;')
for sid,(lbl,col) in SOURCES.items():
    out.append(f'    {sid} [shape=cylinder {fill(col)} penwidth=2 label="{lbl}"];')
out.append('  }')
# pivot cluster
out.append('  subgraph cluster_pivot {')
out.append('    label="blue ONLY because total_supply = stack_circulation_20y\\n(re-root total_supply on *_balances -> these drop stacks)";')
out.append('    style="dashed"; color="#999999"; fontsize=10; fontcolor="#666666";')
for nid in PIVOT:
    lbl,col = N[nid]
    out.append(f'    {nid} [{fill(col)} label="{lbl}"];')
out.append('  }')
# remaining nodes
for nid,(lbl,col) in N.items():
    if nid in PIVOT: continue
    out.append(f'  {nid} [{fill(col)} label="{lbl}"];')
# edges
for a,b in E:
    out.append(f'  {a} -> {b};')
for b in PRICE_TO:
    out.append(f'  PRICE -> {b} [style=dashed color="{GREY}" arrowsize=0.6];')
# legend
out.append('  subgraph cluster_legend {')
out.append('    label="legend"; fontsize=10; color="#cccccc";')
out.append(f'    L_tx  [label="needs *_transfers" {fill("R")}];')
out.append(f'    L_bal [label="needs *_balances" {fill("G")}];')
out.append(f'    L_st  [label="needs *_stacks"   {fill("B")}];')
out.append(f'    L_mix [label="needs 2-3 sources (striped)" {fill("BR")}];')
out.append(f'    L_pr  [label="+ price feed (external)" shape=diamond style=filled fillcolor="{GREY}"];')
out.append('    L_tx -> L_bal -> L_st -> L_mix -> L_pr [style=invis];')
out.append('  }')
out.append('}')

open('metrics_map.dot','w').write('\n'.join(out)+'\n')
print(f"wrote metrics_map.dot : {len(N)+1} metric nodes + {len(SOURCES)} sources, {len(E)+len(PRICE_TO)} edges")
