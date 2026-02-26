// Package main provides the Broadside load tester for Armada Lookout.
//
// Broadside simulates production-like load on the Lookout ingestion and query
// pipeline, allowing benchmarking of different database backends under controlled
// scenarios. It supports PostgreSQL, ClickHouse, and in-memory databases.
//
// The --teardown-only flag can be used to clean up a database left dirty by a
// crashed load test run, without re-running the full test.
package main
