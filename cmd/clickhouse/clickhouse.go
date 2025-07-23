package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/common/util"
	"time"

	log "github.com/armadaproject/armada/internal/common/logging"
)

func main() {
	ctx := context.Background()
	db := openClickHouse(ctx)

	if err := createTables(ctx, db); err != nil {
		log.Fatalf("table creation failed: %v", err)
	}
	log.Info("All tables created")
	const numBatches = 100
	const batchSize = 10000
	start := time.Now()
	for i := 0; i < numBatches; i++ {
		batchStart := time.Now()
		batch, err := db.PrepareBatch(ctx, `
	INSERT INTO job_events (
		job_id, event_time, queue, namespace, job_set, cpu, memory, ephemeral_storage, gpu, priority,
		submitted, priority_class, annotations, state, cancelled, cancel_reason, cancel_user,
		last_transition_time, latest_run_id
	) VALUES (
		?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
	)`)
		if err != nil {
			log.Fatalf("prepare batch failed: %v", err)
		}
		for j := 0; j < batchSize; j++ {
			jobID := util.NewULID()

			now := time.Now()
			events := [][]any{
				{
					jobID, now,
					"queueA", "ns1", "jobsetA", int64(2000), int64(4096), int64(1000000000), int64(0), int64(10),
					now, "prod", `{"team": "infra"}`,
					int32(1), nil, nil, nil,
					now, nil,
				},
				{
					jobID, now.Add(1 * time.Second),
					nil, nil, nil, nil, nil, nil, nil, nil,
					nil, nil, nil,
					int32(2), nil, nil, nil,
					now.Add(1 * time.Second), "01runxyzabc00000000000001",
				},
				{
					jobID, now.Add(2 * time.Second),
					nil, nil, nil, nil, nil, nil, nil, nil,
					nil, nil, nil,
					int32(3), now.Add(2 * time.Second), "timeout", "bob",
					now.Add(2 * time.Second), "01runxyzabc00000000000002",
				},
			}

			for _, e := range events {
				if err := batch.Append(e...); err != nil {
					log.Fatalf("batch append failed: %v", err)
				}
			}
		}
		if err := batch.Send(); err != nil {
			log.Fatalf("batch send failed: %v", err)
		}
		log.Infof("inserted %d jobs in %s", batchSize, time.Since(batchStart))
	}
	timeTaken := time.Since(start)
	log.Infof("inserted %d jobs in %s", batchSize*numBatches, timeTaken)

	var totalJobs uint64
	if err := db.QueryRow(ctx, "SELECT count() FROM job_latest_state").Scan(&totalJobs); err != nil {
		log.Fatalf("failed to count jobs: %v", err)
	}
	log.Infof("total jobs in job_latest_state: %d", totalJobs)

}

func openClickHouse(ctx context.Context) clickhouse.Conn {

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "bench",
			Password: "benchpw",
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})

	if err != nil {
		log.Fatalf("clickhouse open failed: %v", err)
	}

	if err = conn.Ping(ctx); err != nil {
		log.Fatalf("clickhouse ping failed: %v", err)
	}
	return conn
}

func createTables(ctx context.Context, conn clickhouse.Conn) error {
	// Drop in reverse dependency order
	dropStatements := []string{
		"DROP TABLE IF EXISTS job_events_mv",
		"DROP TABLE IF EXISTS job_latest_state",
		"DROP TABLE IF EXISTS job_events",
	}

	for _, stmt := range dropStatements {
		if err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to drop table: %w\nSQL: %s", err, stmt)
		}
	}

	tables := []string{
		// Event-sourced input: job_events
		// Event-sourced input: job_events
		`CREATE TABLE IF NOT EXISTS job_events (
	job_id FixedString(26),
	event_time DateTime DEFAULT now(),
	queue LowCardinality(Nullable(String)),
	namespace LowCardinality(Nullable(String)),
	job_set Nullable(String),
	cpu Nullable(Int64),
	memory Nullable(Int64),
	ephemeral_storage Nullable(Int64),
	gpu Nullable(Int64),
	priority Nullable(Int64),
	submitted Nullable(DateTime),
	priority_class LowCardinality(Nullable(String)),
	annotations Nullable(String),
	state Nullable(Int32),
	cancelled Nullable(DateTime),
	cancel_reason Nullable(String),
	cancel_user Nullable(String),
	last_transition_time Nullable(DateTime),
	latest_run_id Nullable(FixedString(26))
) ENGINE = MergeTree
ORDER BY (job_id, event_time)
SETTINGS index_granularity = 64`,

		// Latest state table: job_latest_state
		`CREATE TABLE IF NOT EXISTS job_latest_state (
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
	priority_class LowCardinality(Nullable(String)),
	annotations Nullable(String),
	state Nullable(Int32),
	cancelled Nullable(DateTime),
	cancel_reason Nullable(String),
	cancel_user Nullable(String),
	last_transition_time Nullable(DateTime),
	latest_run_id Nullable(FixedString(26)),
	event_time DateTime
) ENGINE = AggregatingMergeTree(event_time)
ORDER BY job_id
SETTINGS index_granularity = 512, deduplicate_merge_projection_mode = 'drop'`,

		`
ALTER TABLE job_latest_state ADD PROJECTION proj_by_last_transition_time (
	SELECT *
	ORDER BY last_transition_time
)`,

		`ALTER TABLE job_latest_state ADD PROJECTION proj_by_queue_last_transition (
	SELECT *
	ORDER BY queue, last_transition_time
)`,

		`ALTER TABLE job_latest_state ADD PROJECTION proj_by_state_last_transition (
	SELECT *
	ORDER BY state, last_transition_time
)`,

		`ALTER TABLE job_latest_state ADD PROJECTION proj_by_queue_jobset_last_transition (
	SELECT *
	ORDER BY queue, job_set, last_transition_time
)`,

		// Materialized view: job_events_mv
		`CREATE MATERIALIZED VIEW IF NOT EXISTS job_events_mv
		TO job_latest_state
		AS
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
			argMaxIf(state, event_time, state IS NOT NULL) AS state,
			argMaxIf(cancelled, event_time, cancelled IS NOT NULL) AS cancelled,
			argMaxIf(cancel_reason, event_time, cancel_reason IS NOT NULL) AS cancel_reason,
			argMaxIf(cancel_user, event_time, cancel_user IS NOT NULL) AS cancel_user,
			argMaxIf(last_transition_time, event_time, last_transition_time IS NOT NULL) AS last_transition_time,
			argMaxIf(latest_run_id, event_time, latest_run_id IS NOT NULL) AS latest_run_id
		FROM job_events
		GROUP BY job_id`,
	}

	for _, ddl := range tables {
		if err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create table: %w\nSQL: %s", err, ddl)
		}
	}
	return nil
}
