# Broadside Test Results

This directory contains JSON output files from Broadside load test runs. Each test execution generates a timestamped result file containing comprehensive performance metrics and the complete test configuration.

**Note:** Files in this directory are gitignored and will not be committed to the repository.

## File Naming

Result files follow the naming convention:
```
broadside-result-YYYYMMDD-HHMMSS.json
```

For example: `broadside-result-20260204-143052.json`

## File Structure

Each result file contains:

### Metadata
- Timestamp of test execution
- Schema version (for compatibility tracking)
- Actual test duration (may differ from configured duration if interrupted)

### Configuration Snapshot
- Complete test configuration including:
  - Database configuration
  - Queue and job set configuration
  - Ingestion parameters (submission rates, batch sizes, state transitions)
  - Query parameters (query rates, page sizes)
  - Actions configuration (reprioritisations, cancellations)

### Results

**Ingester Metrics**
- Total batches executed and failed
- Total queries processed
- Batch execution latency percentiles (P50, P95, P99)
- Average batch execution latency
- Peak backlog size
- Average backlog size
- Backlog wait time percentiles

**Querier Metrics**
- Query counts by type:
  - GetJobs (with filter combination breakdown)
  - GetJobGroups (with filter combination breakdown)
  - GetJobSpec
  - GetJobRunError
  - GetJobRunDebugMessage
- Query latency percentiles (P50, P95, P99) per query type and filter combination
- Total queries executed and failed
- Error details with timestamps (up to configured maximum)

**Actor Metrics** (if actions configured)
- Total reprioritisations and cancellations executed
- Total jobs affected by each action type
- Action latency percentiles (P50, P95, P99)
- Action errors with details

## Analysing Results

### Quick Analysis

1. **Check for Errors**: Look for non-zero `totalQueriesFailed` or errors in the `errors` arrays
2. **Query Performance**: Review P95 and P99 latencies - these indicate tail latency under load
3. **Ingestion Performance**: Check `p95ExecutionLatency` - high values indicate database write bottleneck
4. **Backlog Health**: `peakBacklogSize` should be well below `maxBacklogSize` (if configured)

### Comparing Results

To compare different test runs:
1. Ensure configurations are comparable (same query rates, same historical job counts)
2. Compare P95/P99 latencies rather than averages (tail latency matters for user experience)
3. Look for error rate changes
4. Consider warmup period - first test run may include cold cache effects

### Identifying Bottlenecks

**Database Write Bottleneck**
- High `p95ExecutionLatency` values (>500ms)
- Growing backlog size
- Ingester warnings in logs

**Database Query Bottleneck**
- High query P95/P99 latencies
- Increasing latencies for specific filter combinations
- Query timeout errors

**Test Runner Bottleneck**
- `peakBacklogSize` approaching `maxBacklogSize`
- Backlog warnings in logs
- Consider increasing `numWorkers` or reducing submission rate

## JSON Schema

The result file format is defined by the JSON schema at:
```
internal/broadside/metrics/schema.json
```

This schema is automatically generated from Go type definitions. To regenerate:
```bash
go generate ./internal/broadside/metrics
```
