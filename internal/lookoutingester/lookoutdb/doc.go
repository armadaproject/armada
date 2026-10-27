// Package lookoutdb provides the database-insertion layer for the Lookout
// ingester. It translates batches of ingester instructions (job creations,
// job updates, job-run creations/updates, and error records) into SQL writes
// against the Lookout database, with batched and scalar fallback paths, update
// conflation, and terminal-state filtering.
package lookoutdb
