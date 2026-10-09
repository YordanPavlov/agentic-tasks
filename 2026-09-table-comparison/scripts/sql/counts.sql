-- Rows per bucket and side in one time range, as FINAL shows them. Runs on one shard against its local tables,
-- like compare.sql. Read by cuts.sql.
--
-- Placeholders, filled in by common.py from the config:
--   old_table, new_table         the local tables
--   dt, start, end               the time column and the range [start, end)
--   old_where, new_where         the filters of each side
--   buckets                      the bucket columns
--   bucket_literals              those columns as SQL literals, e.g. unhex('7239') for a String
SELECT $buckets, $bucket_literals AS literals, count() AS old_rows, 0 AS new_rows
FROM $old_table FINAL
WHERE $dt >= '$start' AND $dt < '$end' AND ($old_where)
GROUP BY $buckets
UNION ALL
SELECT $buckets, $bucket_literals AS literals, 0 AS old_rows, count() AS new_rows
FROM $new_table FINAL
WHERE $dt >= '$start' AND $dt < '$end' AND ($new_where)
GROUP BY $buckets
