// Package main provides the lookout binary. By default it serves the Lookout
// API. The --migrateDatabase flag instead applies the lookout migration chain
// and, if the job table is still empty, converts it to the partitioned
// shape; a job table that already contains data is left as it is. The
// --pruneDatabase flag instead prunes old jobs from the database.
package main
