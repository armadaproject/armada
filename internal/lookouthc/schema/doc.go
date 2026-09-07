// Package schema contains the idempotent partitioner for the experimental
// hot-cold phase of Lookout. The partitioner converts the unpartitioned
// job table (produced by the lookout migration chain) into a LIST-partitioned
// shape with job_active and job_terminated partitions, in a single
// PostgreSQL transaction. On an already-partitioned database it is a no-op.
// On an unexpected shape it refuses.
//
// Callers should apply the lookout migration chain first, then call
// ApplyPartitioner.
//
// The partitioned shape carries one index that the unpartitioned chain does
// not need: idx_job_queue_job_id, on (queue, job_id). The unpartitioned job
// table gets an ordered job_id scan for free from its PRIMARY KEY, but
// partitioning forces the primary key to (job_id, state), which cannot
// provide a single ordered scan across partitions. Lookout's default job
// view filters by queue and sorts by job_id, so this index keeps that query
// an index scan instead of a full scan of the table.
//
// The package is scaffolding for the experimental phase
// and is deleted at graduation (when the partitioner SQL is lifted into a
// real lookout-chain migration).
package schema
