package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/armadaproject/armada/internal/broadside/jobspec"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	lookoutmodel "github.com/armadaproject/armada/internal/lookoutingester/model"
)

// executePartitionedBatch executes a batch of ingestion queries using direct
// SQL that includes the submitted column. This bypasses LookoutDb for all
// operations except job_run updates (which don't need submitted in WHERE).
//
// The submitted column is required because:
//   - The job table PK is (job_id, submitted), so UPDATEs need it for partition pruning
//   - Child tables (job_run, job_spec, job_error) have a NOT NULL submitted column
func (p *PostgresDatabase) executePartitionedBatch(ctx context.Context, queries []IngestionQuery) error {
	submittedMap := buildJobSubmittedMap(queries)
	set, err := queriesToInstructionSet(queries)
	if err != nil {
		return err
	}

	// Phase 1: Create jobs and job specs (must exist before job_run references).
	var wg sync.WaitGroup
	var jobErr, specErr error
	wg.Go(func() { jobErr = p.createJobsPartitioned(ctx, set.JobsToCreate) })
	wg.Go(func() { specErr = p.createJobSpecsPartitioned(ctx, set.JobsToCreate, submittedMap) })
	wg.Wait()
	if jobErr != nil {
		return fmt.Errorf("creating jobs (partitioned): %w", jobErr)
	}
	if specErr != nil {
		return fmt.Errorf("creating job specs (partitioned): %w", specErr)
	}

	// Phase 2: Update jobs, create job_runs, create job_errors (parallel).
	var updateErr, runErr, errErr error
	wg.Go(func() { updateErr = p.updateJobsPartitioned(ctx, set.JobsToUpdate, submittedMap) })
	wg.Go(func() { runErr = p.createJobRunsPartitioned(ctx, set.JobRunsToCreate, submittedMap) })
	wg.Go(func() { errErr = p.createJobErrorsPartitioned(ctx, set.JobErrorsToCreate, submittedMap) })
	wg.Wait()
	if updateErr != nil {
		return fmt.Errorf("updating jobs (partitioned): %w", updateErr)
	}
	if runErr != nil {
		return fmt.Errorf("creating job runs (partitioned): %w", runErr)
	}
	if errErr != nil {
		return fmt.Errorf("creating job errors (partitioned): %w", errErr)
	}

	// Phase 3: Update job_runs. The job_run table is not partitioned and the
	// UPDATE WHERE clause uses run_id only, so LookoutDb works here.
	armadaCtx := armadacontext.FromGrpcCtx(ctx)
	p.lookoutDb.UpdateJobRuns(armadaCtx, set.JobRunsToUpdate)

	return nil
}

// buildJobSubmittedMap extracts the submitted time for each job ID from the
// query batch. Each query type carries a Submitted field; the first seen
// value for each job ID wins (they should all be consistent).
func buildJobSubmittedMap(queries []IngestionQuery) map[string]time.Time {
	m := make(map[string]time.Time)
	for _, q := range queries {
		switch v := q.(type) {
		case InsertJob:
			m[v.Job.JobID] = v.Job.Submitted
		case SetJobLeased:
			setIfAbsent(m, v.JobID, v.Submitted)
		case InsertJobRun:
			setIfAbsent(m, v.JobID, v.Submitted)
		case SetJobPending:
			setIfAbsent(m, v.JobID, v.Submitted)
		case SetJobRunning:
			setIfAbsent(m, v.JobID, v.Submitted)
		case SetJobSucceeded:
			setIfAbsent(m, v.JobID, v.Submitted)
		case SetJobErrored:
			setIfAbsent(m, v.JobID, v.Submitted)
		case SetJobCancelled:
			setIfAbsent(m, v.JobID, v.Submitted)
		case SetJobPreempted:
			setIfAbsent(m, v.JobID, v.Submitted)
		case SetJobRejected:
			setIfAbsent(m, v.JobID, v.Submitted)
		case UpdateJobPriority:
			setIfAbsent(m, v.JobID, v.Submitted)
		case InsertJobError:
			setIfAbsent(m, v.JobID, v.Submitted)
		}
	}
	return m
}

func setIfAbsent(m map[string]time.Time, key string, val time.Time) {
	if _, ok := m[key]; !ok {
		m[key] = val
	}
}

// createJobsPartitioned inserts jobs using CopyFrom directly into the named
// leaf partition (bypassing parent-table routing which can fail with COPY).
// Instructions are grouped by partition based on their submitted date.
func (p *PostgresDatabase) createJobsPartitioned(ctx context.Context, instructions []*lookoutmodel.CreateJobInstruction) error {
	if len(instructions) == 0 {
		return nil
	}

	groups := make(map[string][]*lookoutmodel.CreateJobInstruction)
	for _, instr := range instructions {
		part := partitionNameForTime(instr.Submitted)
		groups[part] = append(groups[part], instr)
	}

	columns := []string{
		"job_id", "queue", "owner", "namespace", "jobset",
		"cpu", "memory", "ephemeral_storage", "gpu", "priority",
		"submitted", "state", "last_transition_time", "last_transition_time_seconds",
		"priority_class", "annotations", "job_spec",
	}

	for partName, instrs := range groups {
		_, err := p.pool.CopyFrom(ctx,
			pgx.Identifier{partName},
			columns,
			pgx.CopyFromSlice(len(instrs), func(i int) ([]interface{}, error) {
				instr := instrs[i]
				return []interface{}{
					instr.JobId, instr.Queue, instr.Owner, instr.Namespace, instr.JobSet,
					instr.Cpu, instr.Memory, instr.EphemeralStorage, instr.Gpu, instr.Priority,
					instr.Submitted, instr.State, instr.LastTransitionTime, instr.LastTransitionTimeSeconds,
					instr.PriorityClass, instr.Annotations, instr.JobProto,
				}, nil
			}),
		)
		if err != nil {
			return fmt.Errorf("copying into partition %s: %w", partName, err)
		}
	}
	return nil
}

// partitionNameForTime returns the daily partition name for the given timestamp,
// using the same date-truncation logic as createDailyPartitions.
func partitionNameForTime(t time.Time) string {
	date := t.Truncate(24 * time.Hour)
	return fmt.Sprintf("job_p%s", date.Format("20060102"))
}

// createJobSpecsPartitioned inserts job_spec rows with submitted.
func (p *PostgresDatabase) createJobSpecsPartitioned(ctx context.Context, instructions []*lookoutmodel.CreateJobInstruction, submittedMap map[string]time.Time) error {
	if len(instructions) == 0 {
		return nil
	}
	_, err := p.pool.CopyFrom(ctx,
		pgx.Identifier{"job_spec"},
		[]string{"job_id", "job_spec", "submitted"},
		pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
			instr := instructions[i]
			return []interface{}{
				instr.JobId, instr.JobProto, submittedMap[instr.JobId],
			}, nil
		}),
	)
	return err
}

// createJobRunsPartitioned inserts job_run rows with submitted.
func (p *PostgresDatabase) createJobRunsPartitioned(ctx context.Context, instructions []*lookoutmodel.CreateJobRunInstruction, submittedMap map[string]time.Time) error {
	if len(instructions) == 0 {
		return nil
	}
	_, err := p.pool.CopyFrom(ctx,
		pgx.Identifier{"job_run"},
		[]string{"run_id", "job_id", "cluster", "node", "leased", "pool", "job_run_state", "submitted"},
		pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
			instr := instructions[i]
			return []interface{}{
				instr.RunId, instr.JobId, instr.Cluster, instr.Node,
				instr.Leased, instr.Pool, instr.JobRunState,
				submittedMap[instr.JobId],
			}, nil
		}),
	)
	return err
}

// createJobErrorsPartitioned inserts job_error rows with submitted.
func (p *PostgresDatabase) createJobErrorsPartitioned(ctx context.Context, instructions []*lookoutmodel.CreateJobErrorInstruction, submittedMap map[string]time.Time) error {
	if len(instructions) == 0 {
		return nil
	}
	_, err := p.pool.CopyFrom(ctx,
		pgx.Identifier{"job_error"},
		[]string{"job_id", "error", "submitted"},
		pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
			instr := instructions[i]
			return []interface{}{
				instr.JobId, instr.Error, submittedMap[instr.JobId],
			}, nil
		}),
	)
	return err
}

// updateJobsPartitioned updates job rows using a temp table with submitted
// in the WHERE clause for partition pruning.
func (p *PostgresDatabase) updateJobsPartitioned(ctx context.Context, instructions []*lookoutmodel.UpdateJobInstruction, submittedMap map[string]time.Time) error {
	if len(instructions) == 0 {
		return nil
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tmpTable := "job_update_partitioned_tmp"
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			job_id                       varchar(32),
			submitted                    timestamp,
			priority                     bigint,
			state                        smallint,
			cancelled                    timestamp,
			last_transition_time         timestamp,
			last_transition_time_seconds bigint,
			duplicate                    bool,
			latest_run_id                varchar(36),
			cancel_reason                varchar(512),
			cancel_user                  varchar(512)
		) ON COMMIT DROP`, tmpTable)); err != nil {
		return fmt.Errorf("creating temp table: %w", err)
	}

	if _, err := tx.CopyFrom(ctx,
		pgx.Identifier{tmpTable},
		[]string{
			"job_id", "submitted", "priority", "state", "cancelled",
			"last_transition_time", "last_transition_time_seconds",
			"duplicate", "latest_run_id", "cancel_reason", "cancel_user",
		},
		pgx.CopyFromSlice(len(instructions), func(i int) ([]interface{}, error) {
			instr := instructions[i]
			return []interface{}{
				instr.JobId, submittedMap[instr.JobId],
				instr.Priority, instr.State, instr.Cancelled,
				instr.LastTransitionTime, instr.LastTransitionTimeSeconds,
				instr.Duplicate, instr.LatestRunId, instr.CancelReason, instr.CancelUser,
			}, nil
		}),
	); err != nil {
		return fmt.Errorf("copying to temp table: %w", err)
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		UPDATE job
		SET
			priority                     = coalesce(tmp.priority, job.priority),
			state                        = coalesce(tmp.state, job.state),
			cancelled                    = coalesce(tmp.cancelled, job.cancelled),
			last_transition_time         = coalesce(tmp.last_transition_time, job.last_transition_time),
			last_transition_time_seconds = coalesce(tmp.last_transition_time_seconds, job.last_transition_time_seconds),
			duplicate                    = coalesce(tmp.duplicate, job.duplicate),
			latest_run_id                = coalesce(tmp.latest_run_id, job.latest_run_id),
			cancel_reason                = coalesce(tmp.cancel_reason, job.cancel_reason),
			cancel_user                  = coalesce(tmp.cancel_user, job.cancel_user)
		FROM %s AS tmp
		WHERE tmp.job_id = job.job_id AND tmp.submitted = job.submitted`, tmpTable)); err != nil {
		return fmt.Errorf("updating jobs from temp table: %w", err)
	}

	return tx.Commit(ctx)
}

// insertHistoricalJobChunkPartitioned inserts a chunk of historical jobs
// split per age bucket. Each bucket targets a single partition (fixed
// submitted value), avoiding cross-partition writes.
func (p *PostgresDatabase) insertHistoricalJobChunkPartitioned(ctx context.Context, params HistoricalJobsParams, startIdx, lastIdx int) error {
	nBuckets := len(params.JobAgeDays)
	for bucketIdx, ageDays := range params.JobAgeDays {
		if err := p.insertHistoricalJobChunkForBucket(ctx, params, startIdx, lastIdx, bucketIdx, nBuckets, ageDays); err != nil {
			return fmt.Errorf("inserting bucket %d (age %d days): %w", bucketIdx, ageDays, err)
		}
	}
	return nil
}

// insertHistoricalJobChunkForBucket inserts the subset of jobs in
// [startIdx, lastIdx] that belong to the given age bucket, using a fixed
// submitted value so all rows land in a single partition.
func (p *PostgresDatabase) insertHistoricalJobChunkForBucket(
	ctx context.Context, params HistoricalJobsParams,
	startIdx, lastIdx, bucketIdx, nBuckets, ageDays int,
) error {
	prefix := fmt.Sprintf("%04d%04d", params.QueueIdx, params.JobSetIdx)
	succeeded := params.SucceededThreshold
	errored := params.ErroredThreshold
	cancelled := params.CancelledThreshold

	cpuArr := int64SliceToSQL(jobspec.CpuOptions)
	memArr := int64SliceToSQL(jobspec.MemoryOptions)
	ephArr := int64SliceToSQL(jobspec.EphemeralStorageOptions)
	gpuArr := int64SliceToSQL(jobspec.GpuOptions)
	poolArr := stringSliceToSQL(jobspec.PoolOptions)
	nsArr := stringSliceToSQL(jobspec.NamespaceOptions)
	pcArr := stringSliceToSQL(jobspec.PriorityClassOptions)

	// Filter: only rows where i %% nBuckets = bucketIdx
	bucketFilter := fmt.Sprintf("WHERE i%%%d = %d", nBuckets, bucketIdx)

	// Compute the partition date using the same logic as createDailyPartitions
	// so we can insert directly into the named partition, bypassing the
	// parent table's partition routing entirely.
	baseTime := time.Now().Truncate(24 * time.Hour).AddDate(0, 0, -ageDays)
	baseTimeExpr := fmt.Sprintf("'%s'::timestamp", baseTime.Format("2006-01-02 15:04:05"))
	partitionName := fmt.Sprintf("job_p%s", baseTime.Format("20060102"))

	jobSQL := fmt.Sprintf(`
INSERT INTO %s (
    job_id, queue, owner, namespace, jobset,
    cpu, memory, ephemeral_storage, gpu, priority,
    submitted, state, last_transition_time, last_transition_time_seconds,
    priority_class, annotations, latest_run_id,
    cancelled, cancel_reason, cancel_user
)
SELECT
    '%s' || lpad(i::text, 10, '0'),
    '%s',
    '%s',
    (%s)[i%%%d+1],
    '%s',
    (%s)[i%%%d+1],
    (%s)[i%%%d+1],
    (%s)[i%%%d+1],
    (%s)[i%%%d+1],
    (i%%2000)+1,
    %s,
    CASE WHEN i%%1000 < %d THEN %d
         WHEN i%%1000 < %d THEN %d
         WHEN i%%1000 < %d THEN %d
         ELSE %d END,
    %s + INTERVAL '10 seconds',
    EXTRACT(EPOCH FROM %s + INTERVAL '10 seconds')::bigint,
    (%s)[i%%%d+1],
    %s,
    '%s' || lpad(i::text, 10, '0') || '00',
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d
         THEN %s + INTERVAL '10 seconds' END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d
         THEN 'user requested' END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d
         THEN '%s' END
FROM generate_series(%d, %d) AS i
%s
ON CONFLICT DO NOTHING`,
		partitionName,
		prefix,
		params.QueueName, params.QueueName,
		nsArr, len(jobspec.NamespaceOptions),
		params.JobSetName,
		cpuArr, len(jobspec.CpuOptions),
		memArr, len(jobspec.MemoryOptions),
		ephArr, len(jobspec.EphemeralStorageOptions),
		gpuArr, len(jobspec.GpuOptions),
		baseTimeExpr,
		succeeded, lookout.JobSucceededOrdinal,
		errored, lookout.JobFailedOrdinal,
		cancelled, lookout.JobCancelledOrdinal,
		lookout.JobPreemptedOrdinal,
		baseTimeExpr,
		baseTimeExpr,
		pcArr, len(jobspec.PriorityClassOptions),
		buildAnnotationSQL(),
		prefix,
		errored, cancelled, baseTimeExpr,
		errored, cancelled,
		errored, cancelled, params.QueueName,
		startIdx, lastIdx,
		bucketFilter,
	)

	jobSpecSQL := fmt.Sprintf(`
INSERT INTO job_spec (job_id, job_spec, submitted)
SELECT '%s' || lpad(i::text, 10, '0'), $1::bytea, %s
FROM generate_series(%d, %d) AS i
%s
ON CONFLICT DO NOTHING`,
		prefix, baseTimeExpr, startIdx, lastIdx, bucketFilter,
	)

	jobRunSQL := fmt.Sprintf(`
INSERT INTO job_run (
    run_id, job_id, cluster, node, leased, pending, started, finished,
    pool, job_run_state, error, debug, exit_code, submitted
)
SELECT
    '%s' || lpad(i::text, 10, '0') || '00',
    '%s' || lpad(i::text, 10, '0'),
    'broadside-cluster-' || (i%%40+1),
    'broadside-cluster-' || (i%%40+1) || '-node-' || (i%%80+1),
    %s + INTERVAL '1 second',
    %s + INTERVAL '2 seconds',
    %s + INTERVAL '3 seconds',
    %s + INTERVAL '10 seconds',
    (%s)[i%%%d+1],
    CASE WHEN i%%1000 < %d THEN 3
         WHEN i%%1000 < %d THEN 4
         WHEN i%%1000 < %d THEN 6
         ELSE 5 END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d THEN $1::bytea
         WHEN i%%1000 >= %d                  THEN $3::bytea
    END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d THEN $2::bytea END,
    CASE WHEN i%%1000 < %d THEN 0 END,
    %s
FROM generate_series(%d, %d) AS i
%s
ON CONFLICT DO NOTHING`,
		prefix, prefix,
		baseTimeExpr, baseTimeExpr, baseTimeExpr, baseTimeExpr,
		poolArr, len(jobspec.PoolOptions),
		succeeded, errored, cancelled,
		succeeded, errored,
		cancelled,
		succeeded, errored,
		errored,
		baseTimeExpr,
		startIdx, lastIdx,
		bucketFilter,
	)

	jobErrorSQL := fmt.Sprintf(`
INSERT INTO job_error (job_id, error, submitted)
SELECT '%s' || lpad(i::text, 10, '0'), $1::bytea, %s
FROM generate_series(%d, %d) AS i
WHERE i%%1000 >= %d AND i%%1000 < %d AND i%%%d = %d
ON CONFLICT DO NOTHING`,
		prefix, baseTimeExpr, startIdx, lastIdx,
		succeeded, errored, nBuckets, bucketIdx,
	)

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	type stmtArgs struct {
		sql  string
		args []any
	}
	stmts := []stmtArgs{
		{jobSQL, nil},
		{jobSpecSQL, []any{params.JobSpecBytes}},
		{jobRunSQL, []any{params.ErrorBytes, params.DebugBytes, params.PreemptionBytes}},
		{jobErrorSQL, []any{params.ErrorBytes}},
	}
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s.sql, s.args...); err != nil {
			return fmt.Errorf("executing historical jobs SQL (bucket %d): %w", bucketIdx, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}
