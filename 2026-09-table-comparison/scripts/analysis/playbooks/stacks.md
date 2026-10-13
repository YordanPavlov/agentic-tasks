# Playbook: stacks

`*_stacks_shard_vN`: one row per stack (a lot of coins with its acquisition date `odt`), `sign = 1` born, `-1`
spent. Compared on the consumers' key `(contractAddress, address, sign, assetRefId, dt, odt)`, `amount` summed (see
`configs.py`); how an amount is split into stacks, `nonce`, block and tx are not compared.

**Code:** the Flink stacks job (one per chain) writes them; consumers are the metric jobs in `clickhouse-tables`
(`daily_metrics/job_functions/<chain>_stacks.py`). The live table is written from a Kafka topic; the version suffix
says which job generation wrote it.

**Third references:**
- the balances table of the same chain (`*_balances_shard_vN`): per address and day, Σ `sign * amount` must match
  the balance change; which side's stacks agree with balances;
- the blocks present: `SELECT DISTINCT blockNumber` per side for the failing day, against balances or transfers;
- the Kafka source topic (`santiment-kafka-source-search` skill) when a block is missing from every table.

**Typical shapes:**
- `missing_in_<side>` burst on one day, other side +N rows → lost input (find the block ranges first).
- months of `differs` (`small`) and paired missing keys on the same addresses after such a burst → wrong state.
- a late single-day burst of millions of keys → a sweep spending many stacks of an address with wrong state; check
  the address's history around the first event.
- paired missing keys with equal values → `odt` differs (inherited through a transfer from a wrong-state address).

**Invariants:** Σ `sign * amount` per block ≈ 0 except issuance (`table_qa/*_stacks.py`,
`get_stacks_with_huge_delta`).

**Cautions:** a full month read with `FINAL` is ~10 GiB per shard; filter probes by `contractAddress`, `address` and
`dt` (the sorting key prefix). A key's rows must sit on one shard (checked for XRP 2025-07-15).
