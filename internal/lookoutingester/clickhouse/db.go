package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/pkg/errors"
	"time"
)

func OpenClickHouse(ctx context.Context, addr, database, username, password string) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})

	if err != nil {
		return nil, errors.WithMessagef(err, "could not connect to clickhouse on %s", addr)
	}

	if err = conn.Ping(ctx); err != nil {
		return nil, errors.WithMessagef(err, "failed to ping clickhouse at %s", addr)
	}
	return conn, nil
}

func withTestDb(ctx context.Context, f func(db clickhouse.Conn)) error {
	dbName := fmt.Sprintf("test_%s", util.NewULID())

	// Connect to default system database to create/drop test DB
	adminDb, err := OpenClickHouse(ctx, "localhost:9000", "default", "bench", "benchpw")
	if err != nil {
		return err
	}
	defer adminDb.Close()

	// Create the test database
	err = adminDb.Exec(ctx, fmt.Sprintf(`CREATE DATABASE %s`, dbName))
	if err != nil {
		return fmt.Errorf("failed to create test database: %w", err)
	}

	// Connect to the new test database
	testDb, err := OpenClickHouse(ctx, "localhost:9000", dbName, "bench", "benchpw")
	if err != nil {
		return fmt.Errorf("failed to connect to test database: %w", err)
	}
	defer testDb.Close()

	// Create required tables
	if err := CreateTables(ctx, testDb); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	// Run the test function
	f(testDb)

	// Drop the test database to clean up
	err = adminDb.Exec(ctx, fmt.Sprintf(`DROP DATABASE %s`, dbName))
	if err != nil {
		return fmt.Errorf("failed to drop test database: %w", err)
	}

	return nil
}

func CreateTables(ctx context.Context, db clickhouse.Conn) error {
	jobsDdl := `
	CREATE TABLE jobs (
		job_id FixedString(26),
		last_transition_time Nullable(DateTime),
		queue Nullable(String),
		namespace Nullable(String),
		job_set Nullable(String),
		cpu Nullable(Int64),
		memory Nullable(Int64),
		ephemeral_storage Nullable(Int64),
		gpu Nullable(Int64),
		priority Nullable(Int64),
		submitted Nullable(DateTime),
		priority_class Nullable(String),
		annotations Map(String, String),
		job_state Nullable(Int32),
		cancelled Nullable(DateTime),
		cancel_reason Nullable(String),
		cancel_user Nullable(String),
		latest_run_id Nullable(String),
		run_cluster Nullable(String),
		run_exit_code Nullable(Int32),
		run_finished Nullable(DateTime),
		run_state Nullable(Int32),
		run_node Nullable(String),
		run_leased Nullable(DateTime),
		run_pending Nullable(DateTime),
		run_started Nullable(DateTime)
	) ENGINE = CoalescingMergeTree()
		ORDER BY (job_id);
	`

	stmts := []string{
		jobsDdl,
	}

	for _, ddl := range stmts {
		if err := db.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create table/view: %w\nSQL: %s", err, ddl)
		}
	}
	return nil
}
