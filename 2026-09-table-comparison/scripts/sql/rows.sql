-- The actual rows of both tables for a few key hashes (from summary.py) in one window.
-- Runs on every shard, so a key that sits on different shards in old and new shows up with two hosts.
SELECT 'old' AS side, hostName() AS host, hex($key_hash) AS key_hash, $columns
FROM $old
WHERE $window AND ($where) AND ($where_old) AND hex($key_hash) IN ($key_hashes)
UNION ALL
SELECT 'new' AS side, hostName() AS host, hex($key_hash) AS key_hash, $columns
FROM $new
WHERE $window AND ($where) AND ($where_new) AND hex($key_hash) IN ($key_hashes)
