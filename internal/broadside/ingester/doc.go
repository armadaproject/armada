/*
Package ingester simulates job ingestion events for the Broadside load tester.

The ingester has two main phases:

 1. Setup Phase (Setup method) - Blocking initialisation that populates the
    database with historical jobs according to the configured proportions. For
    each (queue, job set) pair it calls database.PopulateHistoricalJobs once,
    delegating the actual insertion to the database layer rather than issuing
    per-job queries from Go.

 2. Run Phase (Run method) - Non-blocking simulation that submits new jobs at
    the configured rate and processes state transitions through the job
    lifecycle (queued -> leased -> pending -> running -> terminal state).

# Parallel Batch Execution

The ingester supports parallel batch execution to improve throughput when
database I/O is the bottleneck. By configuring NumWorkers > 1, queries are
distributed across multiple worker goroutines using consistent hashing by job
ID. This ensures that all queries for a given job are processed by the same
worker, maintaining ordering guarantees per job whilst enabling parallel
processing across different jobs.

# Architecture

The ingester uses a query router to distribute timestamped queries to worker
channels. Each worker maintains its own batch buffer and executes batches
independently. Batches are flushed when either the batch size is reached or a
flush timeout expires.

Job specifications and state transitions are provided by the jobspec package,
which contains all shared job-related configuration and utilities used by both
the ingester and querier components.

Metrics are collected throughout the simulation using histogram-based
collection (see the metrics subpackage) to track backlog size and batch
execution latency, providing insight into database performance under load.

# Backlog Management

The ingester supports configurable backlog handling to prevent memory issues:

  - ChannelBufferSizeMultiplier: Controls channel buffer size (default 10x batch size)
  - MaxBacklogSize: Maximum backlog before drop/block/error strategy activates
  - BacklogDropStrategy: "block" (default), "drop", or "error" when backlog is full

Backlog warnings are automatically logged when the backlog exceeds 80% of the
configured maximum, helping identify when the test runner becomes the bottleneck
rather than the database under test.

# Context and Shutdown

The ingester respects context cancellation for graceful shutdown. When the
context is cancelled, workers finish processing their current batches using
detached contexts to ensure clean database operations. This prevents "context
deadline exceeded" errors during test completion whilst still allowing timely
shutdown.
*/
package ingester
