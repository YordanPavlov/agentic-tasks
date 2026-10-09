-- Compares one window (a time range and a key range) of old and new key by key, and counts the keys per category
-- and cell. A cell is a group (config.group_by) and day; equal keys are only counted per month.
-- Runs on one shard against its local tables, or on the connected broker for a fully replicated table.
--
-- Placeholders, filled in by common.py from the config:
--   old_source, new_source       the rows of each side in the window (sql/sources/); they apply the time range,
--                                the key range and the filters
--   keys                         the key columns
--   value                        the value column
--   dt                           the time column
--   tolerance                    relative tolerance: a smaller difference is equal
--   group_cells                  the group_by columns as strings, each followed by a comma; empty without group_by
--
-- Read it from the innermost layer outwards:
--   1. the rows of both sides, each with its side
--   2. GROUP BY the key puts the old and new rows of a key into one row, which gets a category
--   3. keys are counted per category and cell
SELECT
    category,
    cell,
    count() AS keys,
    sum(old_rows) AS old_row_count,
    sum(new_rows) AS new_row_count,
    max(diff) AS max_diff
FROM
(
    -- 2. One row per key. FINAL leaves one row per sorting key, so a side has several rows for a key only if the
    --    configured key is coarser than that table's sorting key. diff is the relative difference of the values.
    SELECT
        countIf(cmp_is_new = 0) AS old_rows,
        countIf(cmp_is_new = 1) AS new_rows,
        anyIf(cmp_value, cmp_is_new = 0) AS old_value,
        anyIf(cmp_value, cmp_is_new = 1) AS new_value,
        if(old_rows = 0 OR new_rows = 0 OR old_value = new_value OR (isNaN(old_value) AND isNaN(new_value)),
           0,
           abs(old_value - new_value) / greatest(abs(old_value), abs(new_value))) AS diff,
        multiIf(
            new_rows = 0,           'missing_in_new',
            old_rows = 0,           'missing_in_old',
            new_rows > 1,           'new_multi',
            old_rows > 1,           'old_multi',
            diff <= $tolerance,          'equal',
                                    'differs') AS category,
        if(category = 'equal',
           [toString(toStartOfMonth(min(cmp_day)))],
           [${group_cells}toString(min(cmp_day))]) AS cell
    FROM
    (
        -- 1. Both sources unified. The default source reads the table with FINAL: what consumers see, i.e. the
        --    latest inserted row per sorting key (or the highest version). FINAL applies per shard.
        SELECT 0 AS cmp_is_new, $keys, toDate($dt) AS cmp_day, toFloat64($value) AS cmp_value
        FROM (
            $old_source
        )
        UNION ALL
        SELECT 1 AS cmp_is_new, $keys, toDate($dt) AS cmp_day, toFloat64($value) AS cmp_value
        FROM (
            $new_source
        )
    )
    GROUP BY $keys
)
GROUP BY category, cell
