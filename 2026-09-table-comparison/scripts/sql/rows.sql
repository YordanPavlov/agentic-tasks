-- The actual rows of both sources for a few key hashes (from summary.py), as compare.sql saw them.
-- Runs on every shard, so a key that sits on different shards in old and new shows up with two hosts.
--
-- Inputs: the same as compare.sql, plus
--   $key_hashes                  the hex key hashes to show, quoted and comma-separated
--   $columns                     the key, value and soft-value columns, as they are
SELECT 'old' AS side, hostName() AS host, hex(sipHash128($key_columns)) AS key_hash, $columns
FROM (
    $old_source
)
WHERE has([$key_hashes], hex(sipHash128($key_columns)))
UNION ALL
SELECT 'new' AS side, hostName() AS host, hex(sipHash128($key_columns)) AS key_hash, $columns
FROM (
    $new_source
)
WHERE has([$key_hashes], hex(sipHash128($key_columns)))
