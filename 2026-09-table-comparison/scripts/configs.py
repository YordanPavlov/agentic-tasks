"""One entry per comparison. The user names the key and value columns explicitly; nothing is inferred.

Fields of Config:
  old, new     'db.table' of the LOCAL tables (the *_shard_vN tables for sharded ones), not the Distributed ones.
  cluster      Cluster whose shards hold the tables, for sharded tables. Each shard compares its own local
               tables, which relies on old and new placing a key on the same shard (see README.md).
               None for a table fully replicated to every broker: it is compared on the connected broker.
  dt           DateTime/Date column; [start, cutoff) is a range of it, and the output is per day of it.
  keys         Columns a key is made of. Usually the ReplacingMergeTree sorting key; a coarser key compares the
               table the way its consumers aggregate it.
  value        The one compared column, numeric: summed over a key's rows and compared as Float64 with a relative
               tolerance.
  buckets      Key columns the key space is cut on into windows, a prefix of both sorting keys. A bucket is never
               split, so it should hold far fewer rows than a window.
  monthly      True if the tables are partitioned by month: each month is cut and compared on its own. False
               cuts the whole range at once.
  group_by     Key columns the output is broken down by, before the day, e.g. asset and metric.
  common_groups_only  Compare only the group_by groups present on both sides; the others go to coverage.json.
  start, cutoff  'YYYY-MM-DD'; cutoff is exclusive. Leave the live tail out: it is still being written.
  where, where_old, where_new  Optional SQL filters on both sides / one side.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    old: str
    new: str
    cluster: str | None
    dt: str
    keys: tuple[str, ...]
    value: str
    buckets: tuple[str, ...]
    start: str
    cutoff: str
    monthly: bool = True
    group_by: tuple[str, ...] = ()
    common_groups_only: bool = False
    where: str = '1'
    where_old: str = '1'
    where_new: str = '1'


CONFIGS: dict[str, Config] = {
    'xrp_balances': Config(
        old='default.xrp_balances_shard_v8', new='default.xrp_balances_shard_v10', cluster='default_cluster',
        dt='dt',
        keys=('dt', 'assetRefId', 'address', 'blockNumber', 'transactionIndex'),
        value='balance', buckets=('dt',),
        start='2013-01-01', cutoff='2026-09-26'),
    'xrp_stacks': Config(
        # The key the metric jobs aggregate by (daily_metrics/job_functions/xrp_stacks.py); contractAddress is 1:1
        # with assetRefId and is there for the buckets. How a key's amount is split into stacks is not compared.
        old='default.xrp_stacks_shard_v8', new='default.xrp_stacks_shard_v9', cluster='default_cluster',
        dt='dt',
        keys=('contractAddress', 'address', 'sign', 'assetRefId', 'dt', 'odt'),
        value='amount', buckets=('contractAddress', 'address', 'sign'),
        start='2013-01-01', cutoff='2026-09-03'),
    'daily_metrics': Config(
        old='default.daily_metrics_v2', new='default.daily_metrics_v2_experimental', cluster=None,
        dt='dt',
        keys=('asset_id', 'metric_id', 'dt'),
        value='value', buckets=('asset_id', 'metric_id'), monthly=False,
        group_by=('asset_id', 'metric_id'), common_groups_only=True,
        where='asset_id IN (SELECT DISTINCT asset_id FROM default.daily_metrics_v2_experimental)',
        start='2009-01-01', cutoff='2026-10-04'),
    'intraday_metrics': Config(
        old='default.intraday_metrics', new='default.intraday_metrics_experimental', cluster=None,
        dt='dt',
        keys=('asset_id', 'metric_id', 'dt'),
        value='value', buckets=('asset_id', 'metric_id'),
        group_by=('asset_id', 'metric_id'), common_groups_only=True,
        where='asset_id IN (SELECT DISTINCT asset_id FROM default.intraday_metrics_experimental)',
        start='2017-07-01', cutoff='2026-10-04'),
}
