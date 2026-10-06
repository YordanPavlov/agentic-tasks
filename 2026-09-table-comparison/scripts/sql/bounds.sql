-- Splits the comparison into key ranges ("windows") of about WINDOW_ROWS rows per shard, both sides together.
-- Each index entry is the first key of a granule (8192 rows), so ranking the entries by key and summing their
-- rows gives row-count quantiles of the key, however skewed it is. Nothing but the indexes is read.
-- The result is the first key of every window but the first, sorted. Any sorted list of keys splits the key space
-- correctly; the index only makes the windows even.
--
-- Inputs: the index entries (index.sql, run on every shard), the bound columns and WINDOW_ROWS.
WITH
    -- enough windows for the shard with the most rows
    (
        SELECT greatest(1, toUInt64(ceil(max(rows) / $window_rows)))
        FROM (SELECT host, sum(rows_in_granule) AS rows FROM ($index_entries) GROUP BY host)
    ) AS windows
SELECT literals, label
FROM
(
    SELECT
        $bound_columns, literals, toString(tuple($bound_columns)) AS label, rows_in_granule,
        sum(rows_in_granule) OVER (ORDER BY $bound_columns ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            AS rows_through,
        sum(rows_in_granule) OVER () AS total_rows
    FROM ($index_entries)
)
-- the granule that holds the first row of window k, for k = 1 .. windows - 1
WHERE rows_through > rows_in_granule
    AND intDiv((rows_through - rows_in_granule - 1) * windows, total_rows)
        != intDiv((rows_through - 1) * windows, total_rows)
ORDER BY $bound_columns
