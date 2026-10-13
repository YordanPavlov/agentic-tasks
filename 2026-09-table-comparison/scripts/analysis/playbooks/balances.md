# Playbook: balances

`*_balances_shard_vN`: one row per balance change, `(dt, assetRefId, address, blockNumber, transactionIndex)` with
`balance`, and `oldBlockNumber` / `oldBalance` pointing to the previous row of the address. Compared on the sorting
key, value `balance`.

**Code:** the Flink balances job of the chain; it keeps per-address state, so lost input corrupts later balances of
the addresses involved.

**Third references:**
- transfers of the chain (`*_transfers`): the balance change of a row is the net of the block's transfers;
- the chain itself (node RPC) for a handful of addresses, when the sides and transfers disagree;
- the Kafka source topic for missing blocks.

**Typical shapes:**
- `missing_in_<side>` on a few days with the other side +N rows → lost blocks.
- `differs` on the same addresses for a long time after → wrong state; the difference per address is constant
  (the lost amount) until the address is emptied.
- `float`-class `differs` → Float64 accumulation order; check the absolute difference against the asset's precision.

**Invariants:** chain continuity, on both sides: every `(address, assetRefId, oldBlockNumber, oldBalance)` must
point to an existing `(address, assetRefId, blockNumber, balance)`. Dangling pointers locate lost blocks directly
(XRP balances old: ~2,862 from 12 lost ledgers). Run it first when there are missing keys: it is cheaper than
listing blocks.
