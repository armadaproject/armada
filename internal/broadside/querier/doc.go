/*
Package querier simulates user query load for the Broadside load tester.

The querier executes GetJobs and GetJobGroups queries against the database at
configurable rates to simulate real-world user query patterns. Each query is
executed in a separate goroutine and metrics are collected for latency and
error tracking.

# Query Configuration

Query rates are controlled by GetJobsQueriesPerHour and
GetJobGroupsQueriesPerHour in the QueryConfig. If both are zero or negative,
the querier does not run. Page sizes are controlled by GetJobsPageSize and
GetJobGroupsPageSize, defaulting to 100 if not specified.

Additionally, the querier supports follow-up queries that are triggered based on
the results of GetJobs queries:
  - GetJobRunDebugMessageQueriesPerHour: rate for querying debug messages for job runs
  - GetJobRunErrorQueriesPerHour: rate for querying errors for job runs
  - GetJobSpecQueriesPerHour: rate for querying job specifications

These follow-up queries are executed when specific filter combinations return results,
and the rates are distributed across those specific GetJobs queries.

# GetJobs Queries

GetJobs queries cycle through all combinations of order field and direction
using modular arithmetic on the query index. Supported fields include: jobId,
jobSet, submitted, lastTransitionTime, queue, and state. Directions are ASC
and DESC.

# GetJobGroups Queries

GetJobGroups queries cycle through combinations of:
  - Grouped field: queue, namespace, jobSet, state, cluster, node, or any
    configured annotation key
  - Aggregates: all combinations (power set) of submitted, lastTransitionTime,
    and state
  - Order: valid fields depend on the selected aggregates and grouped field
  - LastTransitionTimeAggregate: latest, earliest, or average (when applicable)

The deterministic parameter selection ensures reproducible load patterns whilst
providing comprehensive coverage of the query parameter space.

# Filter Combinations

Both query types implement seven filter combinations, each with equal probability
of being selected (determined by query index modulo 7):

 1. No filters - empty filter list
 2. Annotation filters - 1 to N annotation key-value matches where N is the
    number of configured annotation types
 3. Running jobs in a queue - filters by queue and running state
 4. Queued/leased/pending jobs in a job set - filters by queue, job set, and
    non-terminal active states
 5. Priority filters - filters by queue and priority with various match types
    (exact, greater than, less than, etc.)
 6. Errored jobs in a time range - filters by failed state and submitted time
    within a range that is 5% of the test duration. When this filter returns jobs,
    follow-up queries for GetJobRunDebugMessage and GetJobRunError are executed
    at rates proportional to their configured per-hour rates.
 7. Cluster and node filters - filters by specific cluster and node names

# Follow-up Queries

When GetJobs queries with specific filter combinations return jobs, the querier
executes follow-up queries to simulate detailed job inspection:

For filter combination 6 (errored jobs in time range):
  - GetJobRunDebugMessage and GetJobRunError are called for job runs
  - The number of queries executed per GetJobs call is calculated based on the
    configured hourly rate divided by the frequency of this filter combination
  - Queries cycle through returned jobs as needed

For filter combination 1 (no filters):
  - GetJobSpec is called for returned jobs
  - The number of queries is similarly calculated based on the configured rate

This ensures the total number of follow-up queries per hour matches the
configured rates whilst distributing them across the relevant GetJobs queries.

# Metrics Collection

All queries are tracked in QuerierMetrics, which records:

  - Query latency distributions by query type and filter combination
  - Total queries executed and failed
  - Detailed error information with timestamps

The metrics can be retrieved via the Metrics() method and summarised into a
report via GenerateReport().
*/
package querier
