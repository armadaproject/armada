// Package main provides the Broadside load tester for Armada Lookout.
//
// Broadside simulates production-like load on the Lookout ingestion and query
// pipeline, allowing benchmarking of different database backends under controlled
// scenarios. It supports PostgreSQL, ClickHouse, and in-memory databases.
//
// Each run writes two files to the results directory (default:
// cmd/broadside/results): a JSON metrics file and a plain-text log file, both
// named with the same timestamp (e.g. broadside-result-20260302-143000.json and
// broadside-log-20260302-143000.log).
//
// The --teardown-only flag can be used to clean up a database left dirty by a
// crashed load test run, without re-running the full test.
package main
