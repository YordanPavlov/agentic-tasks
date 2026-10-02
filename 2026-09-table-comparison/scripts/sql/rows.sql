-- The actual rows of both tables for a few key hashes (from summary.py) in one window.
-- Runs on every shard, so a key that sits on different shards in old and new shows up with two hosts.
--
-- Inputs: the same as compare.sql, plus
--   {key_hashes:Array(String)}  the hex key hashes to show
--   $columns                    the key and value columns, as they are
SELECT 'old' AS side, hostName() AS host, hex(sipHash128($key_columns)) AS key_hash, $columns
FROM {old_database:Identifier}.{old_table:Identifier}
WHERE {dt:Identifier} >= {start:Date} AND {dt:Identifier} < {end:Date} AND ($where) AND ($where_old)
    AND has({key_hashes:Array(String)}, hex(sipHash128($key_columns)))
UNION ALL
SELECT 'new' AS side, hostName() AS host, hex(sipHash128($key_columns)) AS key_hash, $columns
FROM {new_database:Identifier}.{new_table:Identifier}
WHERE {dt:Identifier} >= {start:Date} AND {dt:Identifier} < {end:Date} AND ($where) AND ($where_new)
    AND has({key_hashes:Array(String)}, hex(sipHash128($key_columns)))
