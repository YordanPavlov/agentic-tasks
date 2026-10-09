-- Cuts one time range into windows of about $$window_rows rows (both sides, all shards together), between buckets.
-- A bucket is never split, so all rows of a key fall into one window.
-- Returns the rows of each side, which the windows' results must add up to, and the first bucket of every window
-- but the first, in key order.
--
-- Inputs: the row counts per bucket ($$counts: counts.sql, run on every shard), the bucket columns, $$window_rows.
SELECT
    sum(bucket_old_rows) AS old_rows,
    sum(bucket_new_rows) AS new_rows,
    arrayMap(cut -> cut.2, arraySort(groupArrayIf((rows_before, literals), starts_window))) AS cuts
FROM
(
    -- a bucket starts a window if it reaches the next multiple of $$window_rows
    SELECT
        literals, bucket_old_rows, bucket_new_rows, rows_before,
        rows_before > 0
            AND intDiv(rows_before, $window_rows) < intDiv(rows_before + bucket_old_rows + bucket_new_rows,
                                                           $window_rows) AS starts_window
    FROM
    (
        SELECT
            literals, bucket_old_rows, bucket_new_rows,
            sum(bucket_old_rows + bucket_new_rows)
                OVER (ORDER BY $buckets ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS rows_before
        FROM
        (
            -- the shards' counts added up
            SELECT $buckets, any(literals) AS literals, sum(old_rows) AS bucket_old_rows,
                sum(new_rows) AS bucket_new_rows
            FROM ($counts)
            GROUP BY $buckets
        )
    )
)
