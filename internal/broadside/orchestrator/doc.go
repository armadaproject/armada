/*
Package orchestrator coordinates Broadside load test execution.

Broadside is a load testing tool for Armada Lookout that simulates production-like
workloads to benchmark different database backends. This package provides the Runner
type which orchestrates the complete lifecycle of a load test, from database
initialisation through concurrent job ingestion and querying to metrics collection
and reporting.

# Architecture

The Runner coordinates three main components:

  - Database: Pluggable backend (PostgreSQL, ClickHouse, or in-memory) that stores
    job records and handles queries
  - Ingester: Simulates job submissions and state transitions (queued → leased →
    pending → running → succeeded/failed/cancelled)
  - Querier: Simulates user queries against the Lookout UI (job lookups, aggregations,
    filtering by queue/job set)

These components run concurrently to simulate realistic production load where jobs
are continuously ingested whilst users query the system.

# Test Lifecycle

The Runner executes the following steps, with informative log messages at each stage:

 1. Create a database instance based on configuration (PostgreSQL, ClickHouse,
    or in-memory)
 2. Initialise the database schema
 3. Set up the ingester by populating historical job data
 4. Start the ingester and querier in separate goroutines
 5. Start periodic progress logging (every 30 seconds)
 6. If configured, wait for a warmup period and reset both ingester and querier metrics
 7. Wait for the test duration to complete
 8. Tear down the database
 9. Generate and write metrics reports and complete test configuration to a timestamped JSON file

Progress is logged at each stage to provide visibility into the test execution, including
configuration details such as submission rates, worker counts, and query rates. During the
test, progress updates are logged every 30 seconds showing key metrics including batches
executed, query counts, backlog statistics, and error counts.

# Warmup Period

If a warmup duration is configured, the runner waits for that duration after starting
the ingester and querier, then resets all metrics. This allows the system to reach
steady state before measurements begin, excluding initial ramp-up behaviour (such as
cache warming, connection pool establishment, and initial index building) from the
final report.

# Context Handling

The test duration is managed using a timer rather than a context deadline. When the
test duration expires, the runner cancels the context to signal the ingester and
querier to stop. This approach ensures clean shutdown without "context deadline exceeded"
errors. Individual database operations use detached contexts with their own timeouts,
allowing in-flight batches to complete gracefully during shutdown.

The warmup and test duration timers respect external context cancellation, allowing
tests to be interrupted gracefully at any point. When the context is cancelled,
the runner waits for all goroutines to stop, tears down the database, and then
returns the context error.

# Metrics Collection and Output

After the test completes, the runner writes a comprehensive JSON file containing:

  - Test metadata: timestamp, schema version, actual test duration
  - Complete test configuration: database settings, queue configuration, ingestion
    and query parameters
  - Ingester metrics: records processed, batches written, backlog statistics,
    execution latencies
  - Querier metrics: query types executed, latency distributions (p50/p95/p99),
    errors with timestamps

The JSON output file is named with a timestamp (broadside-result-YYYYMMDD-HHMMSS.json)
to facilitate collection and comparison of multiple test runs. The results directory
is configurable via the --results-dir CLI flag, defaulting to cmd/broadside/results.
A JSON schema (internal/broadside/metrics/schema.json) is provided for validation. This structured
output enables building tools for visualising trends and comparing database backend
performance across multiple test runs.

# Usage

	config := configuration.TestConfig{
	    TestDuration:   10 * time.Minute,
	    WarmupDuration: 30 * time.Second,
	    DatabaseConfig: configuration.DatabaseConfig{
	        InMemory: true,
	    },
	    IngestionConfig: configuration.IngestionConfig{
	        SubmissionsPerHour: 60000,
	        NumWorkers:         8,
	        BatchSize:          1000,
	    },
	    QueryConfig: configuration.QueryConfig{
	        GetJobsQueriesPerHour: 1000,
	    },
	}

	runner := orchestrator.NewRunner(config, "/path/to/results")
	if err := runner.Run(ctx); err != nil {
	    log.Fatal(err)
	}

NewDatabase is also exported for use by callers that need a database instance
without running a full test (for example, the --teardown-only CLI flag):

	database, err := orchestrator.NewDatabase(config)
	if err != nil {
	    log.Fatal(err)
	}
	defer database.Close()
*/
package orchestrator
