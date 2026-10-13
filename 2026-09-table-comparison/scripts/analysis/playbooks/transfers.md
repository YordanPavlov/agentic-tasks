# Playbook: transfers

Not yet tried on a real comparison; refine after the first one.

`*_transfers`: one row per transfer, keyed by block, tx and log/trace position. Compared on the sorting key, value
`value` (amount). Stateless: an error stays on its own day and does not propagate within the table, but it does
into balances, stacks and metrics.

**Third references:**
- the chain: tx count per block and block coverage (no gaps in `blockNumber` per day);
- the Kafka source topic (`santiment-kafka-source-search` skill) for a missing block or tx.

**Typical shapes:**
- `missing_in_<side>` on a few blocks → lost or added blocks; list the block numbers per side.
- paired missing keys, equal counts → a key column differs (log index renumbered, trace position): find the column.
- `differs` → decimals, or a change in how internal transfers / fees are emitted; check the token's decimals and the
  exporter version.

**Invariants:** tx count and block coverage against the chain; the yearly `table_qa/test/` checks in
`clickhouse-tables` (`santiment-add-table-qa-tests` skill).
