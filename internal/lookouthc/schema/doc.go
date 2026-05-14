// Package schema contains the idempotent partitioner for the experimental
// hot-cold phase of Lookout. The partitioner converts the unpartitioned
// job table (produced by the lookout migration chain) into a LIST-partitioned
// shape with job_active and job_terminated partitions, in a single
// PostgreSQL transaction. On an already-partitioned database it is a no-op.
// On an unexpected shape it refuses.
//
// Callers should apply the lookout migration chain first, then call
// ApplyPartitioner. The package is scaffolding for the experimental phase
// and is deleted at graduation (when the partitioner SQL is lifted into a
// real lookout-chain migration).
package schema
