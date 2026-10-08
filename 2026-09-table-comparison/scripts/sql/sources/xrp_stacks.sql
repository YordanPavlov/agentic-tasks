-- XRP stacks: nonce numbers the entries of a block, and old and new start that numbering at different offsets.
-- The key uses nonce_rank, the entry's position within (contractAddress, address, sign, blockNumber), so an
-- offset is not a difference, while a missing, extra or reordered entry still shifts the ranks after it.
-- Inputs and contract: see plain.sql.
SELECT
    *,
    row_number() OVER (PARTITION BY contractAddress, address, sign, blockNumber ORDER BY nonce) AS nonce_rank
FROM $table FINAL
WHERE $dt >= '$start' AND $dt < '$end' AND ($key_range) AND ($where)
