-- The default source: the table as consumers see it.
--
-- A source is the innermost query of compare.sql: it returns the rows of one side, with every configured key and
-- the value. It must apply $$key_range to the bucket columns as they are in the table, and return one row per
-- row of the table FINAL: the windows' rows are checked against counts.sql.
--
-- Inputs, filled in by common.py:
--   $$table                      'db.table' of the local table of this side
--   $$dt, $$start, $$end         the time column and the range [$$start, $$end)
--   $$key_range                  the window's range of buckets
--   $$where                      the config's filters for this side
SELECT *
FROM $table FINAL
WHERE $dt >= '$start' AND $dt < '$end' AND ($key_range) AND ($where)
