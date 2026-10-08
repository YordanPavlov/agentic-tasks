-- The default source: the table as consumers see it.
--
-- A source is the innermost query of compare.sql and rows.sql: it returns the rows of one side, with every
-- configured key, value and soft-value column. It must apply $key_range to the window columns as they are in the
-- table, and keep all rows of a key in one window.
--
-- Inputs, filled in by common.py:
--   $table                       'db.table' of the local table of this side
--   $dt, $start, $end            the partition column and the range [$start, $end)
--   $key_range                   the window
--   $where                       the config's filters for this side
SELECT *
FROM $table FINAL
WHERE $dt >= '$start' AND $dt < '$end' AND ($key_range) AND ($where)
