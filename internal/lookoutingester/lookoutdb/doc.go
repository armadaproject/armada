// Package lookoutdb provides the database-insertion layer for the Lookout
// ingester. It translates batches of ingester instructions (job creations,
// job updates, job-run creations/updates, and error records) into SQL writes
// against the Lookout database, with batched and scalar fallback paths, update
// conflation, and terminal-state filtering.
//
// [LookoutDb.Store] attempts each write once and returns immediately on
// error; retry-then-dead-letter policy is owned by the shared
// [ingest.IngestionPipeline.Run] ack-path. Errors classified as
// non-retryable by [armadaerrors.IsRetryablePostgresError] are wrapped with
// [util.NewNonRetryableError] so the ack-path dead-letters immediately
// instead of exhausting its retry budget first.
package lookoutdb
