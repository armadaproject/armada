ALTER TABLE job ADD COLUMN annotations jsonb;

-- GIN indexes that use the jsonb_path_ops operator class do not support the
-- key-exists operators of the default operator class, but the operators they do
-- support are more efficient:
--
--     https://www.postgresql.org/docs/current/datatype-json.html#JSON-INDEXING
CREATE INDEX IF NOT EXISTS idx_job_annotations_path ON job USING GIN (annotations jsonb_path_ops);
