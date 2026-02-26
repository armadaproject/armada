/*
Package db provides database abstraction for the Broadside load tester.

This package defines the Database interface which encapsulates all operations
required for load testing Lookout's database backends. It supports multiple
database implementations (PostgreSQL, ClickHouse, and in-memory) behind a common
interface, enabling apples-to-apples performance comparisons.

# Interface

The Database interface provides methods for:

  - Schema initialisation (InitialiseSchema)
  - Batch ingestion query execution (ExecuteIngestionQueryBatch)
  - Historical job population (PopulateHistoricalJobs)
  - Individual record retrieval (GetJobRunDebugMessage, GetJobRunError, GetJobSpec)
  - Multiple record retrieval (GetJobs, GetJobGroups)
  - Resource cleanup (TearDown, Close)

# Historical Job Population

PopulateHistoricalJobs inserts a batch of pre-completed historical jobs for a
single (queue, job set) pair. It accepts a HistoricalJobsParams struct that
specifies the queue and job set identifiers, the number of jobs to create, and
threshold values used to divide jobs into terminal states (succeeded, errored,
cancelled, preempted) via modular arithmetic on the job number. Implementations
may perform the insertion using server-side SQL generation (e.g. PostgreSQL
INSERT...SELECT FROM generate_series) to avoid round-trip overhead, or using
in-memory Go loops for the MemoryDatabase.

# Ingestion Queries

Batch operations are performed via ExecuteIngestionQueryBatch, which accepts a
slice of IngestionQuery values. IngestionQuery is an interface implemented by
all ingestion query types.

The interface uses a private method to ensure type safety at compile time. Only
the defined query types can be used, preventing runtime errors from incorrect
types. Implementations use a type switch to handle each query type:

	switch q := query.(type) {
	case db.InsertJob:
	    // handle insert job
	case db.SetJobCancelled:
	    // handle job cancellation
	// ... etc
	}

The JobIDFromQuery function extracts the job ID from any IngestionQuery type,
which is useful for routing queries to workers based on job ID (e.g. for
parallel batch execution whilst maintaining per-job ordering).

Supported ingestion query types:

  - InsertJob: Insert a new job record
  - InsertJobSpec: Insert job specification
  - UpdateJobPriority: Update job priority
  - SetJobCancelled: Mark job as cancelled
  - SetJobSucceeded: Mark job as succeeded
  - InsertJobError: Insert job error
  - SetJobPreempted: Mark job as preempted
  - SetJobRejected: Mark job as rejected
  - SetJobErrored: Mark job as errored
  - SetJobRunning: Mark job as running
  - SetJobRunStarted: Mark job run as started
  - SetJobPending: Mark job as pending
  - SetJobRunPending: Mark job run as pending
  - SetJobRunCancelled: Mark job run as cancelled
  - SetJobRunFailed: Mark job run as failed
  - SetJobRunSucceeded: Mark job run as succeeded
  - SetJobRunPreempted: Mark job run as preempted
  - SetJobLeased: Mark job as leased
  - InsertJobRun: Insert a new job run record

# Implementations

Three implementations are provided:

  - PostgresDatabase: PostgreSQL adapter (placeholder implementation)
  - ClickHouseDatabase: ClickHouse adapter (placeholder implementation)
  - MemoryDatabase: In-memory adapter for smoke-testing Broadside

The MemoryDatabase implementation stores all data in Go maps protected by a
read-write mutex. It mirrors the schema structure used in PostgreSQL (jobs, job
runs, annotations, job errors, job specs) and is intended for verifying logical
correctness before running load tests against real databases.

# Query Methods

GetJobs retrieves jobs matching the provided filters, sorted by the specified
order, with pagination via skip and take parameters. It supports filtering on
all job fields (jobId, queue, jobSet, owner, namespace, state, cpu, memory,
ephemeralStorage, gpu, priority, submitted, lastTransitionTime, priorityClass)
as well as job run fields (cluster, node) and annotations. Filter match types
include exact, anyOf, startsWith, contains, and numeric comparisons
(greaterThan, lessThan, greaterThanOrEqualTo, lessThanOrEqualTo).

When the activeJobSets parameter is true, only jobs belonging to "active" job
sets are returned. A job set is considered active if it contains at least one
job in a non-terminal state (QUEUED, LEASED, PENDING, or RUNNING). This mirrors
the behaviour of the Lookout PostgreSQL implementation, which performs an inner
join with a subquery selecting distinct (queue, jobSet) pairs that have active
jobs.

GetJobGroups aggregates jobs by a specified field (queue, jobSet, owner, namespace,
state, cluster, node, or an annotation key) and computes aggregate values. The
supported aggregates are:

  - state: returns a map of state names to job counts
  - submitted: returns the earliest submission time in the group
  - lastTransitionTime: returns the average last transition time in the group

# Usage

Create a database instance using the appropriate constructor:

	// For PostgreSQL
	db := db.NewPostgresDatabase(map[string]string{
	    "host":     "localhost",
	    "port":     "5432",
	    "database": "lookout",
	})

	// For ClickHouse
	db := db.NewClickHouseDatabase(map[string]string{
	    "host":     "localhost",
	    "port":     "9000",
	    "database": "lookout",
	})

	// For in-memory (smoke testing)
	db := db.NewMemoryDatabase()

	// Initialise schema
	if err := db.InitialiseSchema(ctx); err != nil {
	    return err
	}

	// Execute batch of ingestion queries
	queries := []db.IngestionQuery{
	    db.InsertJob{Job: &db.NewJob{...}},
	}
	if err := db.ExecuteIngestionQueryBatch(ctx, queries); err != nil {
	    return err
	}

	// Clean up
	defer db.Close()
*/
package db
