/*
Package metrics provides histogram-based metrics collection for Broadside components.

This package implements efficient metrics collection that uses bounded memory
regardless of test duration, in contrast to list-based approaches that would
either fill up memory or drop samples after reaching a limit.

# Exponential Histograms

The core abstraction is the Histogram type, which collects observations into
exponential buckets that double in size. Given a base value, buckets cover
ranges [0, base), [base, 2*base), [2*base, 4*base), and so on. This design has
two key advantages:

  - Buckets are created dynamically as observations arrive, so there is no
    need to predefine bucket boundaries or guess how long the tail might be.
  - Memory usage grows logarithmically with the range of observed values.

Percentiles (P50, P95, P99) are calculated using linear interpolation within
bucket ranges. Specialised wrappers are provided for common use cases:

  - DurationHistogram: for time.Duration values (e.g., latencies)
  - IntHistogram: for integer values (e.g., backlog sizes)

# Ingester Metrics

The IngesterMetrics type tracks performance during ingestion simulation:

  - Backlog size distribution (exponential buckets with base 1)
  - Batch execution latency distribution (exponential buckets with base 62.5ms)
  - Total counts for batches executed, failed, and queries processed
  - Backlog warning alerts when backlog exceeds configured threshold

# Querier Metrics

The QuerierMetrics type tracks performance during query simulation:

  - Query latency distribution by query type and filter combination
    (exponential buckets with base 10ms)
  - Query error counts and error details with timestamps
  - Aggregate statistics including total queries executed and failed
  - Circular buffer for error collection (default max 2000 errors) to prevent
    unbounded memory growth during long tests

Filter combinations tracked include: no filters, annotation filters, running
jobs in a queue, queued/leased/pending jobs in a job set, priority filters,
errored jobs within a time range, and cluster/node filters.

# JSON Output

The package provides JSON serialisation for test results, combining the test
configuration with all collected metrics into a structured output file. This
enables long-term tracking of load test performance across different database
backends and configurations.

The TestResult structure includes:

  - Metadata: timestamp, schema version, and actual test duration
  - Configuration snapshot: complete test configuration including database
    settings, queue configuration, ingestion and query parameters
  - Results: ingester and querier metrics in JSON-serialisable format

JSON output files are written with timestamps in their filenames
(broadside-result-YYYYMMDD-HHMMSS.json) to facilitate collection and comparison
of multiple test runs.

A JSON schema (schema.json) is automatically generated from the Go type
definitions using `go generate`. To regenerate the schema after modifying the
output types, run:

	go generate ./internal/broadside/metrics

The schema generation uses github.com/invopop/jsonschema to reflect the TestResult
type structure, ensuring the schema always matches the Go types. This makes it
easy to evolve the output format whilst maintaining validation and documentation.

# Future Extensions

This package is designed to be extended with metrics for other Broadside
components:

  - Action Executor: action latency for reprioritisation and cancellation
*/
package metrics
