-- Compares one window (a time range and a key range) of old and new key by key, and counts the keys per category
-- and cell. A cell is a group (config.group_by) and day; equal keys are only counted per month.
-- Runs on one shard against its local tables, or on the connected broker for a fully replicated table.
--
-- Placeholders, filled in by common.py from the config:
--   old_table, new_table         the local tables
--   dt, start, end               the time column and the range [start, end)
--   key_range                    the window's range of buckets
--   old_where, new_where         the filters of each side
--   keys                         the key columns
--   value                        the value column
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
    -- 2. One row per key. A key's value is the sum over its rows, as consumers aggregate it; diff is the relative
    --    difference of the sums.
    SELECT
        countIf(cmp_is_new = 0) AS old_rows,
        countIf(cmp_is_new = 1) AS new_rows,
        sumIf(cmp_value, cmp_is_new = 0) AS old_value,
        sumIf(cmp_value, cmp_is_new = 1) AS new_value,
        if(old_rows = 0 OR new_rows = 0 OR old_value = new_value OR (isNaN(old_value) AND isNaN(new_value)),
           0,
           abs(old_value - new_value) / greatest(abs(old_value), abs(new_value))) AS diff,
        multiIf(
            new_rows = 0,           'missing_in_new',
            old_rows = 0,           'missing_in_old',
            diff <= $tolerance,     'equal',
                                    'differs') AS category,
        if(category = 'equal',
           [toString(toStartOfMonth(min(cmp_day)))],
           [${group_cells}toString(min(cmp_day))]) AS cell
    FROM
    (
        -- 1. Both sides with FINAL: what consumers see, i.e. the latest inserted row per sorting key (or the
        --    highest version). FINAL applies per shard.
        SELECT 0 AS cmp_is_new, $keys, toDate($dt) AS cmp_day, toFloat64($value) AS cmp_value
        FROM $old_table FINAL
        WHERE $dt >= '$start' AND $dt < '$end' AND ($key_range) AND ($old_where)
        UNION ALL
        SELECT 1 AS cmp_is_new, $keys, toDate($dt) AS cmp_day, toFloat64($value) AS cmp_value
        FROM $new_table FINAL
        WHERE $dt >= '$start' AND $dt < '$end' AND ($key_range) AND ($new_where)
    )
    GROUP BY $keys
)
GROUP BY category, cell
