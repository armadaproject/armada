// Package main provides the lookout binary. By default it serves the Lookout
// API. The --migrateDatabase flag instead applies the lookout migration chain
// and, if the job table is still empty, converts it to the partitioned
// shape; a job table that already contains data is left as it is. The
// --pruneDatabase flag instead prunes old jobs from the database.
//
// The --partitionJobTable flag instead runs the operator-initiated migration
// of an existing job table to the partitioned shape. By default, or with
// --preflight, it reports on the job table and the database without
// changing them, and exits non-zero if anything would stop the migration.
// With --printSql it prints the SQL that --execute runs to stdout, sending
// logs to stderr. With --execute it partitions the job table, after a prompt
// on a terminal or with --yes. It exits 0 if the job table is already
// partitioned.
package main
