# Broadside Configuration Files

This directory is for your custom YAML configuration files for Broadside load tests. Create configurations here to define your test scenarios, database connections, and workload parameters.

**Note:** Files in this directory are gitignored and will not be committed to the repository.

## Getting Started

1. **Copy an example configuration** from `examples/` as a starting point:
   ```bash
   cp configs/examples/test-postgres.yaml configs/my-test.yaml
   ```

2. **Modify the configuration** to match your test requirements:
   - Update database connection parameters
   - Adjust submission rates and query rates
   - Configure queue and job set distributions
   - Set test duration and warmup period

3. **Run your test**:
   ```bash
   go run ./cmd/broadside --config ./cmd/broadside/configs/my-test.yaml
   ```

## Configuration Structure

A Broadside configuration file contains the following sections:

### Test Parameters
```yaml
testDuration: 1h        # Total test duration
warmupDuration: 5m      # Warmup period before metrics collection starts
```

### Database Configuration
```yaml
databaseConfig:
  postgres:              # PostgreSQL configuration
    host: localhost
    port: "5432"
    database: broadside_test
    user: broadside
    password: <password>
    sslmode: require
```

Or use in-memory for smoke testing:
```yaml
databaseConfig:
  inMemory: true
```

### Queue Configuration
```yaml
queueConfig:
  - name: queue-1
    proportion: 0.6      # This queue gets 60% of submitted jobs
    jobSetConfig:
      - name: jobset-1
        proportion: 0.7  # 70% of queue-1's jobs go to this job set
        historicalJobsConfig:
          numberOfJobs: 10000
          proportionSucceeded: 0.8
          proportionErrored: 0.1
          proportionCancelled: 0.05
          proportionPreempted: 0.05
```

### Ingestion Configuration
```yaml
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
```

### Query Configuration
```yaml
queryConfig:
  getJobsQueriesPerHour: 7200
  getJobGroupsQueriesPerHour: 3600
  getJobsPageSize: 100
  getJobGroupsPageSize: 50
  getJobSpecQueriesPerHour: 7200
  getJobRunErrorQueriesPerHour: 7200
  getJobRunDebugMessageQueriesPerHour: 7200
```

### Actions Configuration
```yaml
actionsConfig:
  jobSetReprioritisations:
    - performAfterTestStartTime: 2m
      queue: queue-1
      jobSet: jobset-1
  jobSetCancellations:
    - performAfterTestStartTime: 3m
      queue: queue-1
      jobSet: jobset-2
```

## Configuration Tips

**Performance Testing**
- Use production-like submission and query rates
- Configure sufficient historical jobs to simulate realistic data volumes
- Set warmup duration to 5-10% of test duration
- Use dedicated database instances (not Docker on localhost)

**Stress Testing**
- Increase submission rates beyond production levels
- Add multiple scheduled actions close together
- Monitor database resource usage (CPU, memory, I/O)

**Baseline Establishment**
- Run multiple iterations (3-5) with identical configuration
- Calculate median latencies across runs
- Use as baseline for comparison with optimisations

**Development Testing**
- Use `inMemory: true` for quick smoke tests
- Set short test durations (30s-1m)
- Reduce historical job counts (1000-10000)

## Environment Variables

Configuration values can be overridden with environment variables using the `ARMADA_` prefix:
```bash
export ARMADA_DATABASECONFIG_POSTGRES_HOST=my-db-host
export ARMADA_DATABASECONFIG_POSTGRES_PASSWORD=secret
go run ./cmd/broadside --config ./cmd/broadside/configs/my-test.yaml
```

Variable names use `_` as the delimiter for nested configuration. The `::` delimiter in YAML becomes `_` in environment variables.

## Validation

Broadside performs comprehensive validation on startup. Common validation errors:

- **Proportions don't sum to 1.0**: Queue or job set proportions must sum to approximately 1.0 (within 0.01 tolerance)
- **Multiple databases configured**: Only one database backend (postgres, clickhouse, or inMemory) can be active
- **Actions scheduled beyond test duration**: Actions must occur before the test ends
- **Negative durations**: All duration values must be non-negative
- **Missing required fields**: Database name, queue names, job set names must not be empty

## Example Configurations

See the `examples/` directory for complete, working configurations:
- `test-inmemory.yaml` - Quick smoke test with in-memory database
- `test-postgres.yaml` - PostgreSQL load test with Docker setup
- `docker-compose-postgres.yaml` - Docker Compose file for local PostgreSQL instance
