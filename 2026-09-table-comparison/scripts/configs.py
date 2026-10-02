"""One entry per comparison. The user names the key and value columns explicitly; nothing is inferred.

Fields of Config:
  old, new     'db.table' of the LOCAL tables (the *_shard_vN tables), not the Distributed ones.
  cluster      Cluster whose shards hold the tables, for sharded tables. Each shard compares its own local
               tables, which relies on old and new using the same sharding (see README.md).
               None for a table fully replicated to every broker: it is compared on the connected broker.
  dt           DateTime/Date column the tables are partitioned by. Work is done one month at a time.
  keys         Columns identifying a row: the ReplacingMergeTree sorting key, or the key you want to compare by.
  values       Columns compared for each key. Float columns are compared with a relative tolerance,
               everything else exactly.
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
    values: tuple[str, ...]
    start: str
    cutoff: str
    where: str = '1'
    where_old: str = '1'
    where_new: str = '1'


CONFIGS: dict[str, Config] = {
    'xrp_balances': Config(
        old='default.xrp_balances_shard_v8', new='default.xrp_balances_shard_v10', cluster='default_cluster',
        dt='dt',
        keys=('dt', 'assetRefId', 'address', 'blockNumber', 'transactionIndex'),
        values=('balance', 'oldDt', 'oldBlockNumber', 'oldBalance', 'currency', 'issuer', 'issuerCurrency',
                'addressType', 'transactionHash'),
        start='2013-01-01', cutoff='2026-09-26'),
    'xrp_stacks': Config(
        # Old is sorted by (…, dt, nonce), new by (…, blockNumber, nonce); dt follows from blockNumber,
        # so new's key is used and dt is compared as a value.
        old='default.xrp_stacks_shard_v8', new='default.xrp_stacks_shard_v9', cluster='default_cluster',
        dt='dt',
        keys=('contractAddress', 'address', 'sign', 'blockNumber', 'nonce'),
        values=('dt', 'odt', 'amount', 'assetRefId', 'txID'),
        start='2013-01-01', cutoff='2026-09-03'),
}
