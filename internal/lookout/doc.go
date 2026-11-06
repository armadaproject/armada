// Package lookout contains the Lookout API server, which serves the
// auto-generated REST API defined in swagger.yaml. It wires the swagger
// handlers to the underlying Postgres-backed repositories that power the
// Lookout UI. At startup it logs the shape of the job table, refuses to start
// if the job table still uses the unpartitioned schema and contains data,
// and registers the lookout_job_table_partitioned gauge.
package lookout
