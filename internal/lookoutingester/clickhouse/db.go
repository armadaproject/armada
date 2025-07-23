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
	jobEventsDdl := `
	CREATE TABLE job_events (
		job_id FixedString(26),
		event_time DateTime,
		queue LowCardinality(Nullable(String)),
		namespace LowCardinality(Nullable(String)),
		job_set Nullable(String),
		cpu Nullable(Int64),
		memory Nullable(Int64),
		ephemeral_storage Nullable(Int64),
		gpu Nullable(Int64),
		priority Nullable(Int64),
		submitted Nullable(DateTime),
		priority_class LowCardinality(String),
		annotations Nullable(String),
		job_state Nullable(Int32),
		cancelled Nullable(DateTime),
		cancel_reason Nullable(String),
		cancel_user Nullable(String),
		last_transition_time Nullable(DateTime),
		latest_run_id Nullable(String),
		run_cluster Nullable(String),
		run_exit_code Nullable(Int32),
		run_finished Nullable(DateTime),
		run_state Nullable(Int32),
		run_node Nullable(String),
		run_leased Nullable(DateTime),
		run_pending Nullable(DateTime),
		run_started Nullable(DateTime)
	) ENGINE = MergeTree()
	ORDER BY (job_id);
	`

	jobsDdl := `
	CREATE TABLE jobs (
		job_id FixedString(26),
		queue LowCardinality(Nullable(String)),
		namespace LowCardinality(Nullable(String)),
		job_set Nullable(String),
		cpu Nullable(Int64),
		memory Nullable(Int64),
		ephemeral_storage Nullable(Int64),
		gpu Nullable(Int64),
		priority Nullable(Int64),
		submitted Nullable(DateTime),
		priority_class LowCardinality(String),
		annotations Nullable(String),
		job_state Nullable(Int32),
		cancelled Nullable(DateTime),
		cancel_reason Nullable(String),
		cancel_user Nullable(String),
		last_transition_time Nullable(DateTime),
		latest_run_id Nullable(String),
		run_cluster Nullable(String),
		run_exit_code Nullable(Int32),
		run_finished Nullable(DateTime),
		run_state Nullable(Int32),
		run_node Nullable(String),
		run_leased Nullable(DateTime),
		run_pending Nullable(DateTime),
		run_started Nullable(DateTime)
	) ENGINE = ReplacingMergeTree()
	ORDER BY job_id;
	`

	materializedView := `
	CREATE MATERIALIZED VIEW mv_jobs
TO jobs AS
SELECT
	job_id,
	argMaxIf(queue, event_time, queue IS NOT NULL) AS queue,
	argMaxIf(namespace, event_time, namespace IS NOT NULL) AS namespace,
	argMaxIf(job_set, event_time, job_set IS NOT NULL) AS job_set,
	argMaxIf(cpu, event_time, cpu IS NOT NULL) AS cpu,
	argMaxIf(memory, event_time, memory IS NOT NULL) AS memory,
	argMaxIf(ephemeral_storage, event_time, ephemeral_storage IS NOT NULL) AS ephemeral_storage,
	argMaxIf(gpu, event_time, gpu IS NOT NULL) AS gpu,
	argMaxIf(priority, event_time, priority IS NOT NULL) AS priority,
	argMaxIf(submitted, event_time, submitted IS NOT NULL) AS submitted,
	argMaxIf(priority_class, event_time, priority_class IS NOT NULL) AS priority_class,
	argMaxIf(annotations, event_time, annotations IS NOT NULL) AS annotations,
	argMaxIf(job_state, event_time, job_state IS NOT NULL) AS job_state,
	argMaxIf(cancelled, event_time, cancelled IS NOT NULL) AS cancelled,
	argMaxIf(cancel_reason, event_time, cancel_reason IS NOT NULL) AS cancel_reason,
	argMaxIf(cancel_user, event_time, cancel_user IS NOT NULL) AS cancel_user,
	argMaxIf(last_transition_time, event_time, last_transition_time IS NOT NULL) AS last_transition_time,
	argMaxIf(latest_run_id, event_time, latest_run_id IS NOT NULL) AS latest_run_id,
	argMaxIf(run_cluster, event_time, run_cluster IS NOT NULL) AS run_cluster,
	argMaxIf(run_exit_code, event_time, run_exit_code IS NOT NULL) AS run_exit_code,
	argMaxIf(run_finished, event_time, run_finished IS NOT NULL) AS run_finished,
	argMaxIf(run_state, event_time, run_state IS NOT NULL) AS run_state,
	argMaxIf(run_node, event_time, run_node IS NOT NULL) AS run_node,
	argMaxIf(run_leased, event_time, run_leased IS NOT NULL) AS run_leased,
	argMaxIf(run_pending, event_time, run_pending IS NOT NULL) AS run_pending,
	argMaxIf(run_started, event_time, run_started IS NOT NULL) AS run_started
	FROM job_events
	GROUP BY job_id;
	`

	stmts := []string{
		jobEventsDdl,
		jobsDdl,
		materializedView,
	}

	for _, ddl := range stmts {
		if err := db.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create table/view: %w\nSQL: %s", err, ddl)
		}
	}
	return nil
}
