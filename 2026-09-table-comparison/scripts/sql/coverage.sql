-- Rows per group (config.group_by) and side in [$$start, $$end), to find the groups present on one side only.
-- Presence does not depend on FINAL, so the tables are read without it. Runs like compare.sql; common.py adds up
-- the shards.
--
-- Inputs, filled in by common.py from the config:
--   $$old_table, $$new_table, $$dt, $$start, $$end, $$old_where, $$new_where  as in counts.sql
--   $$group_by                   the group_by columns
SELECT $group_by, count() AS old_rows, 0 AS new_rows
FROM $old_table
WHERE $dt >= '$start' AND $dt < '$end' AND ($old_where)
GROUP BY $group_by
UNION ALL
SELECT $group_by, 0 AS old_rows, count() AS new_rows
FROM $new_table
WHERE $dt >= '$start' AND $dt < '$end' AND ($new_where)
GROUP BY $group_by
