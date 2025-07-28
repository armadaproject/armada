package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/pkg/errors"
	"sync"
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
	CREATE TABLE  IF NOT EXISTS jobs(
		job_id FixedString(26),
		last_transition_time SimpleAggregateFunction(anyLast, DateTime),
		queue SimpleAggregateFunction(any, String),
		namespace SimpleAggregateFunction(any, String),
		job_set SimpleAggregateFunction(any, String),
		cpu SimpleAggregateFunction(any, Int64),
		memory SimpleAggregateFunction(any, Int64),
		ephemeral_storage SimpleAggregateFunction(any, Int64),
		gpu SimpleAggregateFunction(any, Int64),
		priority  SimpleAggregateFunction(anyLast, Int64),
		submitted SimpleAggregateFunction(any, DateTime),
		priority_class SimpleAggregateFunction(any, String),
		annotations SimpleAggregateFunction(any, Map(String, String)),
		job_state SimpleAggregateFunction(anyLast,Int32),
		cancelled SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		cancel_reason SimpleAggregateFunction(anyLast, Nullable(String)),
		cancel_user SimpleAggregateFunction(anyLast, Nullable(String)),
		latest_run_id SimpleAggregateFunction(anyLast, Nullable(String)),
		run_cluster SimpleAggregateFunction(anyLast, Nullable(String)),
		run_exit_code SimpleAggregateFunction(anyLast, Nullable(Int32)),
		run_finished SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		run_state SimpleAggregateFunction(anyLast, Nullable(Int32)),
		run_node SimpleAggregateFunction(anyLast, Nullable(String)),
		run_leased SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		run_pending SimpleAggregateFunction(anyLast, Nullable(DateTime)),
		run_started SimpleAggregateFunction(anyLast, Nullable(DateTime))
	)
		ENGINE = AggregatingMergeTree()
		ORDER BY (job_id)
        SETTINGS deduplicate_merge_projection_mode = 'drop';
	`

	stmts := []string{
		jobsDdl,
		//		`ALTER TABLE jobs MODIFY SETTING min_age_to_force_merge_seconds = 20`,
		`ALTER TABLE jobs
		ADD PROJECTION queue_lookup (
		   SELECT *
		   ORDER BY (queue, last_transition_time)
		)`,
	}

	for _, ddl := range stmts {
		if err := db.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create table/view: %w\nSQL: %s", err, ddl)
		}
	}
	return nil
}

func StartMergeDelayMonitor(ctx context.Context, db clickhouse.Conn) (stop func(), getMaxDelay func() time.Duration) {
	var maxDelay time.Duration
	var mu sync.Mutex
	ticker := time.NewTicker(100 * time.Millisecond)
	logTicker := time.NewTicker(1 * time.Second)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		defer logTicker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				delay := getCurrentMergeDelay(ctx, db)
				mu.Lock()
				if delay > maxDelay {
					maxDelay = delay
				}
				mu.Unlock()
			case <-logTicker.C:
				mu.Lock()
				current := getCurrentMergeDelay(ctx, db)
				log.Infof("Current merge delay: %v, max observed delay: %v", current, maxDelay)
				mu.Unlock()
			}
		}
	}()

	stop = func() { close(done) }
	getMaxDelay = func() time.Duration {
		mu.Lock()
		defer mu.Unlock()
		return maxDelay
	}
	return stop, getMaxDelay
}

func getCurrentMergeDelay(ctx context.Context, db clickhouse.Conn) time.Duration {
	rows, err := db.Query(ctx, `
		SELECT elapsed
		FROM system.merges
		WHERE database = currentDatabase()
		  AND table = 'jobs'
	`)
	if err != nil {
		fmt.Printf("error querying system.merges: %v\n", err)
		return 0
	}
	defer rows.Close()

	var maxElapsed float64
	for rows.Next() {
		var elapsed float64
		if err := rows.Scan(&elapsed); err != nil {
			fmt.Printf("scan error: %v\n", err)
			continue
		}
		if elapsed > maxElapsed {
			maxElapsed = elapsed
		}
	}

	return time.Duration(maxElapsed * float64(time.Second))
}
