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
    sum(o_rows) AS old_rows,
    sum(n_rows) AS new_rows,
    -- key hashes for rows.py and for the cross-shard check in summary.py; not kept for matching keys
    groupArrayIf($max_keys)(hex(kh), category NOT IN ('equal', 'float_noise')) AS sample
FROM
(
    -- 3. Category per key, checked in this order.
    --    *_ah: hash of all compared values (floats exactly), so min != max means several versions on that side.
    --    *_vh: hash of the non-float values only. Floats are compared separately with the tolerance.
    SELECT
        kh, day, o_rows, n_rows,
        multiIf(
            n_rows = 0,                    'only_old',
            o_rows = 0,                    'only_new',
            n_ah_min != n_ah_max,          'new_multi',
            o_ah_min != o_ah_max AND (n_ah_min = o_ah_min OR n_ah_min = o_ah_max),
                                           'old_multi_new_matches',
            o_ah_min != o_ah_max,          'old_multi',
            o_ah_min = n_ah_min,           'equal',
            o_vh != n_vh,                  'value_diff',
            $floats_within_tol,
                                           'float_noise',
                                           'float_diff') AS category
    FROM
    (
        -- 2. One row per key. Duplicate rows of a key (unmerged ReplacingMergeTree parts) only raise *_rows.
        SELECT
            cmp_kh AS kh,
            min(cmp_day) AS day,
            countIf(cmp_side = 0) AS o_rows,
            countIf(cmp_side = 1) AS n_rows,
            minIf(cmp_ah, cmp_side = 0) AS o_ah_min,
            maxIf(cmp_ah, cmp_side = 0) AS o_ah_max,
            minIf(cmp_ah, cmp_side = 1) AS n_ah_min,
            maxIf(cmp_ah, cmp_side = 1) AS n_ah_max,
            minIf(cmp_vh, cmp_side = 0) AS o_vh,
            minIf(cmp_vh, cmp_side = 1) AS n_vh$float_aggs
        FROM
        (
            -- 1. Both tables reduced to the same shape. cmp_ prefixes keep aliases from shadowing table columns.
            SELECT 0 AS cmp_side, $row_exprs
            FROM $old
            WHERE $window AND ($where) AND ($where_old)
            UNION ALL
            SELECT 1 AS cmp_side, $row_exprs
            FROM $new
            WHERE $window AND ($where) AND ($where_new)
        )
        GROUP BY cmp_kh
    )
)
GROUP BY day, category
