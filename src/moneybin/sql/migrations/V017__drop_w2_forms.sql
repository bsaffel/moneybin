-- Drop raw.w2_forms table.
-- The W-2 PDF extraction pipeline has been removed entirely.
-- Tax data ingestion will be re-designed in a future brainstorm.
DROP TABLE IF EXISTS raw.w2_forms;
