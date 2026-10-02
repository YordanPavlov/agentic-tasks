-- Compares one window (a month) of old and new key by key and counts keys per day and category.
-- Runs on one shard against its local tables, or on the connected broker for a fully replicated table.
-- Read it from the innermost layer outwards:
--   1. every row of either table becomes (side, key hash, day, value hashes, float values)
--   2. GROUP BY the key hash puts the old and new versions of a key into one row
--   3. each key gets a category
--   4. keys are counted per day and category
SELECT
    hostName() AS host,
    day,
    category,
    count() AS keys,
    sum(old_row_count) AS old_rows,
    sum(new_row_count) AS new_rows,
    -- key hashes for rows.py and for the cross-shard check in summary.py; not kept for matching keys
    groupArrayIf($max_key_hashes)(hex(key_hash), category NOT IN ('equal', 'float_noise')) AS key_hashes
FROM
(
    -- 3. Category per key, checked in this order.
    --    *_row_hash: all compared values (floats exactly), so min != max means several versions on that side.
    --    *_values_hash: the non-float values only. Floats are compared separately, with the tolerance.
    SELECT
        key_hash, day, old_row_count, new_row_count,
        multiIf(
            new_row_count = 0,                          'only_old',
            old_row_count = 0,                          'only_new',
            new_row_hash_min != new_row_hash_max,       'new_multi',
            old_row_hash_min != old_row_hash_max
                AND (new_row_hash_min = old_row_hash_min OR new_row_hash_min = old_row_hash_max),
                                                        'old_multi_new_matches',
            old_row_hash_min != old_row_hash_max,       'old_multi',
            old_row_hash_min = new_row_hash_min,        'equal',
            old_values_hash != new_values_hash,         'value_diff',
            $floats_within_tolerance,
                                                        'float_noise',
                                                        'float_diff') AS category
    FROM
    (
        -- 2. One row per key. Duplicate rows of a key (unmerged ReplacingMergeTree parts) only raise the row counts.
        SELECT
            cmp_key_hash AS key_hash,
            min(cmp_day) AS day,
            countIf(cmp_is_new = 0) AS old_row_count,
            countIf(cmp_is_new = 1) AS new_row_count,
            minIf(cmp_row_hash, cmp_is_new = 0) AS old_row_hash_min,
            maxIf(cmp_row_hash, cmp_is_new = 0) AS old_row_hash_max,
            minIf(cmp_row_hash, cmp_is_new = 1) AS new_row_hash_min,
            maxIf(cmp_row_hash, cmp_is_new = 1) AS new_row_hash_max,
            minIf(cmp_values_hash, cmp_is_new = 0) AS old_values_hash,
            minIf(cmp_values_hash, cmp_is_new = 1) AS new_values_hash$float_columns
        FROM
        (
            -- 1. Both tables reduced to the same shape. cmp_ prefixes keep aliases from shadowing table columns.
            SELECT 0 AS cmp_is_new, $row_columns
            FROM $old
            WHERE $window AND ($where) AND ($where_old)
            UNION ALL
            SELECT 1 AS cmp_is_new, $row_columns
            FROM $new
            WHERE $window AND ($where) AND ($where_new)
        )
        GROUP BY cmp_key_hash
    )
)
GROUP BY day, category
