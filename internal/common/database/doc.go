// Package database contains shared utilities for components that interact
// with Armada's PostgreSQL databases.
//
// The package provides:
//   - A simple, custom migration engine (UpdateDatabase, ReadMigrations)
//     used by the scheduler and lookout migrators. Migrations are numbered
//     SQL files; the applied version is tracked in a database_version
//     sequence.
//   - PrepareSchema and MigrationConfig, which let migrators optionally
//     create the target schema and set search_path before running
//     migrations. A separate "schema-creator" connection can be configured
//     so a least-privileged migrator role can run inside a schema created
//     by an admin role.
//   - Connection helpers (OpenPgxConn, OpenPgxPool, CreateConnectionString)
//     that build pgx connections from a PostgresConfig.
//   - Test helpers (WithTestDb, WithTestDbCustom) that spin up a temporary
//     database, run migrations, and clean up afterwards.
//   - A Querier interface that abstracts pgx connections and pools so
//     callers can pass either to the same code.
package database
