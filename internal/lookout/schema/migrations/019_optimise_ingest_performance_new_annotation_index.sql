CREATE INDEX CONCURRENTLY idx_job_annotations_path
  ON job
  USING gin (annotations jsonb_ops)
  WITH (
    fastupdate = true,
    gin_pending_list_limit = 33554432
  );
  