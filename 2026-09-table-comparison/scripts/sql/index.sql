-- The primary index entries of both tables, one per granule, from the active parts that overlap [$start, $end).
-- Runs on one shard against its local tables, like compare.sql. Read by bounds.sql.
--
-- Inputs, filled in by common.py:
--   $old_database, $old_name, $new_database, $new_name   the local tables
--   $bound_columns               the key columns the windows are cut on, a prefix of both sorting keys
--   $bound_literals              those columns as SQL literals, e.g. unhex('7239') for a String
--   A part's time range is in min_time/max_time for a DateTime partition column, in min_date/max_date for a Date.
SELECT hostName() AS host, $bound_columns, $bound_literals AS literals, rows_in_granule
FROM mergeTreeIndex('$old_database', '$old_name')
WHERE part_name IN (
    SELECT name FROM system.parts
    WHERE database = '$old_database' AND table = '$old_name' AND active
        AND greatest(toDateTime(max_date), max_time) >= toDateTime('$start')
        AND greatest(toDateTime(min_date), min_time) < toDateTime('$end'))
UNION ALL
SELECT hostName() AS host, $bound_columns, $bound_literals AS literals, rows_in_granule
FROM mergeTreeIndex('$new_database', '$new_name')
WHERE part_name IN (
    SELECT name FROM system.parts
    WHERE database = '$new_database' AND table = '$new_name' AND active
        AND greatest(toDateTime(max_date), max_time) >= toDateTime('$start')
        AND greatest(toDateTime(min_date), min_time) < toDateTime('$end'))
