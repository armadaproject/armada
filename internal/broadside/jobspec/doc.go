/*
Package jobspec provides shared job specifications and utilities for Broadside
load testing.

This package contains all the configuration, constants, and helper functions
related to synthetic job generation that are shared between the ingester and
querier components. By centralising these definitions, we ensure consistency
across job creation and query patterns whilst avoiding circular dependencies.

# Job Identification

Jobs are identified using compact, time-ordered IDs encoded in Crockford Base32
format. The encoding includes the submission timestamp, queue index, job set
index, and sequence number, allowing deterministic recreation of job properties
without maintaining in-memory state.

# Job States

The JobState type defines the lifecycle states a job can be in:
  - StateLeased: Job has been leased to an executor
  - StatePending: Job is pending in the executor
  - StateRunning: Job is actively running
  - StateSucceeded: Job completed successfully
  - StateErrored: Job failed with an error
  - StateCancelled: Job was cancelled by the user
  - StatePreempted: Job was preempted by a higher priority job

Query state constants (QueryState*) provide the string representations used in
database queries, matching the lookout database schema.

# Deterministic Selection

The package provides proportion-based selection using modular arithmetic to
deterministically assign jobs to queues, job sets, and terminal states based
on configured proportions. This ensures reproducible load patterns across test
runs whilst avoiding the overhead of random number generation.

# Job Properties

Exported variable slices (NamespaceOptions, PriorityClassOptions, CpuOptions,
MemoryOptions, EphemeralStorageOptions, GpuOptions, PoolOptions,
AnnotationConfigs) and helper functions define the range of job properties used
in synthetic job generation. These are selected deterministically based on job
number to create realistic variation whilst maintaining reproducibility. The
slices are exported so that database implementations can reference them directly
for server-side SQL generation.
*/
package jobspec
