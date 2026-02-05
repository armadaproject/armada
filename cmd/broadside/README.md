# Broadside Load Tester

Broadside is a load testing tool for Armada Lookout, designed to benchmark
different database backends under controlled, production-like workloads.

## Overview

Broadside is primarily designed for **performance testing against dedicated
database instances** (VMs or cloud-hosted databases) that mirror production
configurations. The tool measures query latency, throughput, and database
behaviour under realistic Lookout workloads.

The in-memory database adapter is useful for **smoke-testing and framework
development**, but should not be used for performance benchmarking.

## Quick Start

### 1. Set Up Your Database

For performance testing, you should use a **dedicated, optimised database
instance**:

**Production-like setup (recommended for benchmarking):**
- Provision a PostgreSQL instance on a VM or cloud provider
- Configure it with production-like resources (CPU, memory, disk)
- Apply appropriate tuning (shared_buffers, work_mem, etc.)
- Ensure network latency is representative of your production environment

**Local development setup (smoke-testing only):**
- See `configs/examples/docker-compose-postgres.yaml` for a basic local
  PostgreSQL setup
- **Note:** Docker on localhost is not suitable for performance benchmarking

### 2. Configure Your Test

Create a YAML configuration in `configs/` or use the provided examples:

```yaml
testDuration: 5m
warmupDuration: 1m

databaseConfig:
  postgres:
    host: your-postgres-host.example.com
    port: "5432"
    database: broadside_test
    user: broadside
    password: <secure-password>
    sslmode: require

ingestionConfig:
  submissionsPerHour: 50000
  batchSize: 100
  numWorkers: 10
  jobStateTransitionConfig:
    queueingDuration: 5s
    leasedDuration: 10s
    pendingDuration: 15s
    proportionSucceed: 0.85
    runningToSuccessDuration: 2m
    proportionFail: 0.15
    runningToFailureDuration: 1m

queryConfig:
  getJobsQueriesPerHour: 7200
  getJobGroupsQueriesPerHour: 3600
  getJobsPageSize: 100
  getJobGroupsPageSize: 50
  getJobSpecQueriesPerHour: 7200
  getJobRunErrorQueriesPerHour: 7200
  getJobRunDebugMessageQueriesPerHour: 7200

actionsConfig:
  jobSetReprioritisations:
    - performAfterTestStartTime: 2m
      queue: queue-1
      jobSet: jobset-1
  jobSetCancellations:
    - performAfterTestStartTime: 3m
      queue: queue-1
      jobSet: jobset-2

queueConfig:
  - name: queue-1
    proportion: 0.6
    jobSetConfig:
      - name: jobset-1
        proportion: 1.0
        historicalJobsConfig:
          numberOfJobs: 100000
          proportionSucceeded: 0.8
          proportionErrored: 0.1
          proportionCancelled: 0.05
          proportionPreempted: 0.05

actionsConfig:
  jobSetReprioritisations: []
  jobSetCancellations: []
```

See `configs/examples/` for complete example configurations.

## Actions

Broadside can simulate bulk operations that users perform via the Lookout UI, such as reprioritising or cancelling all jobs in a specific queue and job set. These actions are scheduled to execute at specific times during the test to measure the database performance impact of bulk updates.

### Configuring Actions

Actions are configured in the `actionsConfig` section:

```yaml
actionsConfig:
  jobSetReprioritisations:
    - performAfterTestStartTime: 2m
      queue: queue-1
      jobSet: jobset-1
    - performAfterTestStartTime: 4m
      queue: queue-2
      jobSet: jobset-3
  jobSetCancellations:
    - performAfterTestStartTime: 3m
      queue: queue-1
      jobSet: jobset-2
```

### Action Types

**Job Set Reprioritisation**: Updates the priority of all active jobs in a specified queue and job set
- Simulates users bulk-reprioritising jobs to expedite or delay execution
- Sets job priority to a fixed value (2000) to ensure the update is applied
- Only affects active jobs (queued, leased, pending, running)
- Terminal jobs (succeeded, failed, cancelled) are not affected

**Job Set Cancellation**: Cancels all active jobs in a specified queue and job set
- Simulates users bulk-cancelling jobs when they're no longer needed
- Sets cancellation reason to "Bulk cancellation via Broadside test"
- Sets cancel user to "broadside-test"
- Only affects active jobs (queued, leased, pending, running)
- Terminal jobs are not affected

### Action Scheduling

Actions are scheduled relative to the test start time using `performAfterTestStartTime`:
- Actions execute once at the specified time
- If an action is scheduled during the warmup period, metrics are still collected
- If an action is scheduled after the test duration, it will not execute
- Actions scheduled for the same time execute sequentially

**Example timeline for a 5-minute test with 1-minute warmup:**
- `performAfterTestStartTime: 30s` - Executes during warmup
- `performAfterTestStartTime: 2m` - Executes during measurement period
- `performAfterTestStartTime: 6m` - Will not execute (after test ends)

### Action Behaviour

When an action executes:
1. Queries the database for all active jobs matching the queue and job set
2. If no matching jobs are found, logs a warning and skips the action
3. Generates bulk update queries for all matching jobs
4. Executes the updates using `ExecuteIngestionQueryBatch`
5. Records metrics (duration, job count, errors)

### Action Metrics

Action metrics are included in the test results:

```json
{
  "results": {
    "actor": {
      "totalReprioritisations": 2,
      "totalCancellations": 1,
      "totalJobsReprioritised": 15000,
      "totalJobsCancelled": 8000,
      "reprioritisationP50Latency": "245ms",
      "reprioritisationP95Latency": "512ms",
      "reprioritisationP99Latency": "891ms",
      "cancellationP50Latency": "198ms",
      "cancellationP95Latency": "445ms",
      "cancellationP99Latency": "623ms",
      "errors": []
    }
  }
}
```

These metrics help measure:
- Database performance under bulk update load
- Impact on concurrent query latency
- Scalability of bulk operations
- Database lock contention

### Use Cases

**Performance Testing**: Measure how bulk operations affect query latency
```yaml
# Execute reprioritisation during peak query load
actionsConfig:
  jobSetReprioritisations:
    - performAfterTestStartTime: 2m30s
      queue: high-priority-queue
      jobSet: critical-jobs
```

**Stress Testing**: Test database behaviour under multiple concurrent bulk operations
```yaml
# Schedule multiple actions close together
actionsConfig:
  jobSetReprioritisations:
    - performAfterTestStartTime: 2m
      queue: queue-1
      jobSet: jobset-1
    - performAfterTestStartTime: 2m05s
      queue: queue-2
      jobSet: jobset-2
  jobSetCancellations:
    - performAfterTestStartTime: 2m10s
      queue: queue-3
      jobSet: jobset-3
```

**Baseline Testing**: Run tests with and without actions to measure overhead
```yaml
# Test 1: No actions (baseline)
actionsConfig:
  jobSetReprioritisations: []
  jobSetCancellations: []

# Test 2: With actions (measure impact)
actionsConfig:
  jobSetReprioritisations:
    - performAfterTestStartTime: 2m
      queue: queue-1
      jobSet: jobset-1
```

### 3. Run Broadside

```bash
# From project root
go run ./cmd/broadside --config ./cmd/broadside/configs/examples/test-postgres.yaml
go run ./cmd/broadside --config ./cmd/broadside/configs/examples/test-inmemory.yaml

# Or from cmd/broadside directory
cd cmd/broadside
go run main.go --config configs/examples/test-postgres.yaml
go run main.go --config configs/my-test.yaml
```

The `--config` flag is **required** and specifies the path to your test
configuration file.

Results are written to `results/broadside-result-YYYYMMDD-HHMMSS.json`

### 4. Analyse Results

Open the JSON file in `results/` to examine:
- Query latency percentiles (p50, p95, p99)
- Throughput metrics
- Error rates
- Ingestion performance

## Architecture

### Database Adapters

Broadside supports multiple database backends through a common interface:

- **In-Memory** (`internal/broadside/db/memory.go`): Framework development and
  smoke-testing only
- **PostgreSQL** (`internal/broadside/db/postgres.go`): Production Lookout
  backend
- **ClickHouse** (`internal/broadside/db/clickhouse.go`): To be implemented

### PostgreSQL Implementation

The Postgres adapter reuses Lookout's production infrastructure to ensure
benchmarks reflect reality:

**Schema**: Applies actual Lookout migrations via `internal/lookout/schema`
- Same tables: `job`, `job_run`, `job_spec`, `job_error`
- Same indexes: GIN JSON indexes, multi-column indexes, pattern matching indexes
- Ensures performance characteristics match production

**Queries**: Delegates to Lookout repositories
- `GetJobs`: Uses `repository.SqlGetJobsRepository`
- `GetJobGroups`: Uses `repository.SqlGroupJobsRepository`
- `GetJobSpec`: Uses `repository.SqlGetJobSpecRepository`
- `GetJobRunError`: Uses `repository.SqlGetJobRunErrorRepository`
- `GetJobRunDebugMessage`: Uses `repository.SqlGetJobRunDebugMessageRepository`

**Ingestion**: Maximises performance and production fidelity
- Inserts: Uses PostgreSQL COPY protocol (10,000+ rows/sec) for bulk loading
- Updates: Delegates to Lookout ingester's batch update methods
- Parallel execution: Independent operations (job specs, runs, errors) execute
  concurrently
- Dependency ordering: Respects foreign key constraints (jobs →
  specs/runs/errors)

**Lifecycle**:
1. `InitialiseSchema()`: Opens connection pool, applies migrations, initialises
   repositories
2. Test runs: Ingestion + queries execute concurrently
3. `TearDown()`: Truncates all tables (fast cleanup, preserves schema for
   multiple runs)
4. `Close()`: Closes connection pool

## Directory Structure

```
cmd/broadside/
├── main.go              # Entry point with YAML configuration support
├── configs/             # Your test configurations (gitignored)
│   ├── README.md
│   ├── examples/        # Example configs and setup scripts
│   │   ├── README.md
│   │   ├── docker-compose-postgres.yaml  # Local dev PostgreSQL (not for benchmarking)
│   │   ├── test-inmemory.yaml           # In-memory smoke test configuration
│   │   └── test-postgres.yaml           # PostgreSQL example configuration
│   └── *.yaml          # Your configurations
└── results/             # Test output (gitignored)
    ├── README.md
    └── *.json          # Timestamped result files
```

## Database Configuration

### Connection Parameters

Broadside uses libpq connection string format (`map[string]string`):

```yaml
databaseConfig:
  postgres:
    host: <hostname>           # Database host
    port: <port>               # Database port (default: 5432)
    database: <database>       # Database name (required for validation)
    user: <username>           # Database user
    password: <password>       # Database password
    sslmode: <mode>            # SSL mode (disable, require, verify-full)
    # Any other libpq parameters (dbname, etc.)...
```

### Database Requirements

**Schema**: Broadside automatically applies Lookout migrations on startup
- Requires CREATE/DROP TABLE permissions
- Requires CREATE INDEX permissions
- Approximately 100MB for schema + minimal data

**Cleanup**: Broadside truncates tables between runs
- Each test run starts with an empty database
- Schema persists (fast repeat testing)
- Use `TearDown()` to clean up after testing

**Resources**: Depends on your test scale
- 100K jobs ≈ 370MB database size
- Scale resources based on expected query load
- Monitor CPU, memory, disk I/O during tests

## Features

Broadside provides comprehensive load testing capabilities:

**Database Support**
- PostgreSQL adapter with production Lookout schema and queries
- In-memory adapter for framework smoke-testing
- ClickHouse adapter interface (awaiting implementation)

**Configuration**
- YAML-based configuration with Viper
- Environment variable support (ARMADA_ prefix)
- Multiple config file layering
- Comprehensive validation with detailed error messages
- Example configurations for common scenarios

**Job Ingestion Simulation**
- Configurable submission rates (jobs per hour)
- Parallel batch execution with multiple workers
- PostgreSQL COPY protocol for high-throughput inserts
- Realistic job state transitions (queued → leased → pending → running → terminal)
- Historical job population with configurable state distributions
- Backlog management with configurable drop/block/error strategies

**Query Simulation**
- GetJobs queries with seven filter combinations
- GetJobGroups queries with aggregation support
- Follow-up queries (GetJobSpec, GetJobRunError, GetJobRunDebugMessage)
- Deterministic query parameter selection for reproducibility
- Concurrent query execution
- Latency tracking by query type and filter combination

**Bulk Actions**
- Job set reprioritisations (scheduled bulk priority updates)
- Job set cancellations (scheduled bulk cancellations)
- Time-based scheduling relative to test start
- Metrics collection for action latency and throughput

**Metrics and Reporting**
- Histogram-based metrics with exponential buckets (bounded memory usage)
- Query latency percentiles (P50, P95, P99)
- Backlog size and wait time tracking
- Batch execution performance
- JSON output with full configuration snapshot
- Timestamped result files for long-term tracking

**Test Lifecycle**
- Warmup period with metrics reset
- Progress logging every 30 seconds
- Graceful shutdown with in-flight batch completion
- Database teardown and cleanup

## Future Enhancements

The following features would be valuable additions:

- **ClickHouse Adapter**: Implement ClickHouse backend for comparison testing
- **Results Comparison Tool**: Build tooling to compare results across runs and visualise trends
- **Results Interpretation Guide**: Add documentation for analysing performance metrics and identifying bottlenecks
- **CI/CD Integration**: Automate benchmarking in continuous integration pipelines for regression detection

## Why Reuse Lookout Infrastructure?

1. **Realistic Schema**: Tests reflect actual production database structure
2. **Realistic Queries**: Tests use actual production query patterns
3. **Tested Code**: Lookout repositories are battle-tested
4. **No Drift**: Schema and queries stay in sync with production changes
5. **Performance Accuracy**: Benchmarks reflect real performance characteristics

Any optimisations or schema changes in Lookout are automatically reflected in
Broadside tests.

## Example Result File

Results are written as JSON to `results/`:

```json
{
  "metadata": {
    "timestamp": "2026-01-15T14:36:46Z",
    "version": "1.0.0",
    "testDuration": "5m"
  },
  "configuration": {
    "testDuration": "5m",
    "warmupDuration": "1m",
    "database": { "type": "postgres" }
    // ... full test configuration
  },
  "results": {
    "ingester": {
      "totalBatches": 3000,
      "totalQueries": 250000,
      "averageBatchLatencyMs": 45.2,
      "errorsEncountered": 0
    },
    "querier": {
      "queryMetrics": {
        "GetJobs": {
          "count": 36000,
          "averageLatencyMs": 12.5,
          "p50LatencyMs": 8.3,
          "p95LatencyMs": 45.2,
          "p99LatencyMs": 123.5,
          "errors": 0
        },
        "GetJobGroups": {
          "count": 18000,
          "averageLatencyMs": 23.1,
          // ... percentiles
        }
        // ... other query types
      }
    }
  }
}
```

## Benchmarking Best Practices

### Database Setup

1. **Dedicated Instance**: Use a separate VM/instance, not shared infrastructure
2. **Production Configuration**: Mirror production database settings
3. **Baseline Performance**: Test on a fresh database before each benchmark
4. **Warm-up Period**: Use sufficient warmup duration (e.g., 1-2 minutes) for
   caches to populate

### Test Design

1. **Realistic Workloads**: Configure job submission and query rates to match
   production
2. **Appropriate Scale**: Start with smaller datasets, scale up gradually
3. **Multiple Runs**: Run each test 3-5 times, report median results
4. **Vary Parameters**: Test different query patterns, data volumes, concurrent
   users

### Comparing Backends

When comparing PostgreSQL vs ClickHouse:

1. **Same Hardware**: Use identical VM specifications
2. **Same Dataset**: Use identical historical job counts and distributions
3. **Same Queries**: Use identical query rates and patterns
4. **Same Duration**: Use identical test and warmup durations
5. **Document Configuration**: Save database tuning parameters for
   reproducibility

## Troubleshooting

**"Connection refused"**: Ensure your database is running and accessible
- Check firewall rules
- Verify connection parameters
- Test connection with `psql` first

**"Permission denied"**: Ensure database user has appropriate permissions
- CREATE TABLE, DROP TABLE for schema management
- INSERT, SELECT, UPDATE, DELETE for data operations

**Poor performance**: Check database configuration
- Insufficient shared_buffers or work_mem
- Missing indexes (Broadside applies them automatically)
- Network latency (test from same region/datacenter)

**Out of memory**: Reduce test scale
- Decrease historical job counts
- Reduce concurrent query rates
- Use smaller batch sizes
