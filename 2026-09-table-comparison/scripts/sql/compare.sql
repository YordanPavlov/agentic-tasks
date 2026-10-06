-- Compares one window (a key range, from bounds.sql) of old and new key by key and counts keys per day and
-- category. Runs on one shard against its local tables, or on the connected broker for a fully replicated table.
--
-- Inputs, filled in by common.py from the config:
--   $old_table, $new_table       'db.table' of the local tables
--   $dt                          the partition column; the comparison covers [$start, $end)
--   $key_range                   the window: a range of a sorting-key prefix, written so the primary index applies
--   $tolerance                   relative tolerance for floats
--   $max_key_hashes              key hashes kept per day and category
--   $key_columns                 the key columns
--   $exact_value_columns         the non-float value columns, plus isNull() of nullable float columns
--   $float_value_columns         the float value columns, as Float64
--   $where, $where_old, $where_new   filters from the config
--   Nullable columns arrive as isNull(c), ifNull(c, default), since a NULL argument makes a hash NULL.
--
-- Read it from the innermost layer outwards:
--   1. every row of either table, as FINAL shows it, becomes (side, key hash, day, value hashes, float values)
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
    --    *_row_hash: all compared values (floats exactly), so min != max means several different rows on that side.
    --    *_values_hash: the non-float values only. Floats are compared separately, with the tolerance.
    --    Floats are indexed rather than zipped, so a missing side (an empty array) cannot raise an error.
    SELECT
        key_hash, day, old_row_count, new_row_count,
        multiIf(
            new_row_count = 0,                          'only_old',
            old_row_count = 0,                          'only_new',
            new_row_hash_min != new_row_hash_max,       'new_multi',
            old_row_hash_min != old_row_hash_max,       'old_multi',
            old_row_hash_min = new_row_hash_min,        'equal',
            old_values_hash != new_values_hash,         'value_diff',
            arrayAll(i -> old_floats[i] = new_floats[i]
                          OR (isNaN(old_floats[i]) AND isNaN(new_floats[i]))
                          OR abs(old_floats[i] - new_floats[i])
                             <= $tolerance * greatest(abs(old_floats[i]), abs(new_floats[i])),
                     arrayEnumerate(old_floats)),
                                                        'float_noise',
                                                        'float_diff') AS category
    FROM
    (
        -- 2. One row per key. FINAL leaves one row per sorting key of each table, so a side has several rows for a
        --    key only if the configured key is coarser than that table's sorting key; min != max detects it.
        --    The float and values-hash minimums are only read once both sides have a single distinct row.
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
            minIf(cmp_values_hash, cmp_is_new = 1) AS new_values_hash,
            minIf(cmp_floats, cmp_is_new = 0) AS old_floats,
            minIf(cmp_floats, cmp_is_new = 1) AS new_floats
        FROM
        (
            -- 1. Both tables unified, read with FINAL: what consumers see, i.e. the latest inserted row per sorting
            --    key (or the highest version, for a table with a version column). FINAL applies per shard.
            --    cmp_key_hash has 128 bits, so collisions are negligible even for billions of keys.
            SELECT
                0 AS cmp_is_new,
                sipHash128($key_columns) AS cmp_key_hash,
                toDate($dt) AS cmp_day,
                cityHash64($exact_value_columns) AS cmp_values_hash,
                CAST([$float_value_columns], 'Array(Float64)') AS cmp_floats,
                cityHash64(cmp_values_hash, arrayMap(f -> reinterpretAsUInt64(f), cmp_floats)) AS cmp_row_hash
            FROM $old_table FINAL
            WHERE $dt >= '$start' AND $dt < '$end' AND ($key_range) AND ($where) AND ($where_old)
            UNION ALL
            SELECT
                1 AS cmp_is_new,
                sipHash128($key_columns) AS cmp_key_hash,
                toDate($dt) AS cmp_day,
                cityHash64($exact_value_columns) AS cmp_values_hash,
                CAST([$float_value_columns], 'Array(Float64)') AS cmp_floats,
                cityHash64(cmp_values_hash, arrayMap(f -> reinterpretAsUInt64(f), cmp_floats)) AS cmp_row_hash
            FROM $new_table FINAL
            WHERE $dt >= '$start' AND $dt < '$end' AND ($key_range) AND ($where) AND ($where_new)
        )
        GROUP BY cmp_key_hash
    )
)
GROUP BY day, category
