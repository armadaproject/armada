// Package store provides the [ingest.Sink] implementation for the event
// ingester. [RedisEventStore] writes batches of job-set events to one or
// more Redis instances, sharded by job set, with event-retention expiry.
// [RedisEventStore.Store] attempts each write once and returns immediately
// on error; retry-then-dead-letter policy is owned by the shared
// [ingest.IngestionPipeline.Run] ack-path. Errors classified as
// non-retryable by isRetryableRedisError are wrapped with
// [util.ErrNonRetryable] so the ack-path dead-letters immediately
// instead of exhausting its retry budget first.
package store
