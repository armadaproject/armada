// Package schema owns the Lookout database schema. It embeds the numbered
// lookout migration chain (migrations/*.sql), exposed via LookoutMigrations,
// and the partitioner that converts the job table into its partitioned
// shape.
//
// The migration chain creates an unpartitioned job table. The partitioner
// converts it, in a single PostgreSQL transaction holding an ACCESS
// EXCLUSIVE lock on job, into a LIST-partitioned table (target_schema.sql)
// with a job_active partition for states that are still expected to change
// and a job_terminated partition for terminal states. Postgres moves a row
// between the partitions when its state is updated.
//
// DetectJobTableShape reports the live shape of the job table: missing,
// unpartitioned, partitioned with the expected shape, or partitioned with an
// unexpected shape. Callers use it, rather than any configuration, to decide
// what to do.
//
// PartitionJobTable converts an unpartitioned job table whatever its size.
// PartitionJobTableIfEmpty converts it only if it has no rows, so that
// fresh installs get the partitioned shape while existing installs are never
// converted without an explicit operator action. On an already partitioned
// job table both backfill any indexes added to the partitioned shape since
// that database was first partitioned, then return. On an unexpected shape
// both refuse. Callers must apply the migration chain first.
//
// The partitioned shape carries one index that the unpartitioned chain does
// not need: idx_job_queue_job_id, on (queue, job_id). The unpartitioned job
// table gets an ordered job_id scan for free from its PRIMARY KEY, but
// partitioning forces the primary key to (job_id, state), which cannot
// provide a single ordered scan across partitions. Lookout's default job
// view filters by queue and sorts by job_id, so this index keeps that query
// an index scan instead of a full scan of the table.
//
// # Writing migrations
//
// UpdateDatabase applies the migration chain to either shape of the job
// table. Before each migration it checks the live shape, and on a
// partitioned job table it applies the migration's PartitionedVariant if it
// has one: "NNN_name.partitioned.sql", or a "NNN_name.partitioned/"
// directory whose files run as separate statements outside a transaction.
// Otherwise it applies the default "NNN_name.sql". Every migration that
// touches job must therefore:
//
//   - Work on both shapes. Most DDL, such as ALTER TABLE job ADD COLUMN,
//     propagates from a partitioned parent to its partitions, so needs no
//     variant.
//   - Not block writes while it builds an index. CREATE INDEX CONCURRENTLY
//     is not allowed on a partitioned table, so an index on job needs a
//     partitioned variant directory following partitionedIndexMigration in
//     migrations_test.go.
//   - Be safe to re-run, because recording a migration is not atomic with
//     applying it: use IF NOT EXISTS, and drop any INVALID index left by a
//     failed concurrent build before rebuilding it.
//   - Be matched by the conversion, which rebuilds job from
//     target_schema.sql: add any new column or index there, to the rename
//     list in convertUnpartitionedToPartitioned, and to findShapeMismatch.
//     TestChainThenConvertMatchesConvertThenRestOfChain fails if a database
//     partitioned before the migration ends up different from one
//     partitioned after it.
package schema
