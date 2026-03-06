package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	broadsideconfiguration "github.com/armadaproject/armada/internal/broadside/configuration"
	"github.com/armadaproject/armada/internal/broadside/jobspec"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookout/schema"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	lookoutingestermetrics "github.com/armadaproject/armada/internal/lookoutingester/metrics"
	lookoutmodel "github.com/armadaproject/armada/internal/lookoutingester/model"
	serverconfiguration "github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

// PostgresDatabase implements the Database interface using PostgreSQL.
// It reuses the Lookout schema and query infrastructure to ensure
// realistic testing of production query patterns.
type PostgresDatabase struct {
	config                    map[string]string
	features                  broadsideconfiguration.FeatureToggles
	tuningSQLStatements       []string
	tuningRevertSQLStatements []string
	pool                      *pgxpool.Pool
	lookoutDb                 *lookoutdb.LookoutDb
	jobsRepository            *repository.SqlGetJobsRepository
	groupRepository           *repository.SqlGroupJobsRepository
	jobSpecRepository         *repository.SqlGetJobSpecRepository
	jobRunErrorRepository     *repository.SqlGetJobRunErrorRepository
	jobRunDebugRepository     *repository.SqlGetJobRunDebugMessageRepository
}

// NewPostgresDatabase creates a new PostgresDatabase instance.
// The config map should contain connection parameters compatible with libpq:
//   - host: database host (e.g., "localhost")
//   - port: database port (e.g., "5433")
//   - user: database user (e.g., "postgres")
//   - password: database password
//   - dbname: database name (e.g., "broadside_test")
//   - sslmode: SSL mode (e.g., "disable")
func NewPostgresDatabase(config map[string]string, features broadsideconfiguration.FeatureToggles, tuningSQLStatements []string, tuningRevertSQLStatements []string) *PostgresDatabase {
	return &PostgresDatabase{
		config:                    config,
		features:                  features,
		tuningSQLStatements:       tuningSQLStatements,
		tuningRevertSQLStatements: tuningRevertSQLStatements,
	}
}

// InitialiseSchema opens the connection pool, applies the Lookout database
// migrations, applies per-table autovacuum tuning SQL, and initialises the
// query repository.
func (p *PostgresDatabase) InitialiseSchema(ctx context.Context) error {
	pgConfig := serverconfiguration.PostgresConfig{
		Connection: p.config,
	}

	pool, err := database.OpenPgxPool(pgConfig)
	if err != nil {
		return fmt.Errorf("opening connection pool: %w", err)
	}
	p.pool = pool

	migrations, err := schema.LookoutMigrations()
	if err != nil {
		pool.Close()
		return fmt.Errorf("loading lookout migrations: %w", err)
	}

	armadaCtx := armadacontext.FromGrpcCtx(ctx)
	if err := database.UpdateDatabase(armadaCtx, p.pool, migrations); err != nil {
		pool.Close()
		return fmt.Errorf("applying migrations: %w", err)
	}

	if err := p.applyTuningSQL(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("applying tuning SQL: %w", err)
	}

	if p.features.HotColdSplit {
		if _, err := p.pool.Exec(ctx, hotColdMigrationSQL); err != nil {
			pool.Close()
			return fmt.Errorf("applying hot/cold split migration: %w", err)
		}
		logging.Info("Hot/cold split migration applied")
	}

	decompressor := &compress.NoOpDecompressor{}
	p.lookoutDb = lookoutdb.NewLookoutDb(p.pool, nil, lookoutingestermetrics.Get(), 16, 12)
	if p.features.HotColdSplit {
		tables := repository.NewTablesWithJobTable("job_all")
		p.jobsRepository = repository.NewSqlGetJobsRepositoryWithTables(p.pool, tables)
		p.groupRepository = repository.NewSqlGroupJobsRepositoryWithTables(p.pool, tables)
	} else {
		p.jobsRepository = repository.NewSqlGetJobsRepository(p.pool)
		p.groupRepository = repository.NewSqlGroupJobsRepository(p.pool)
	}
	p.jobSpecRepository = repository.NewSqlGetJobSpecRepository(p.pool, decompressor)
	p.jobRunErrorRepository = repository.NewSqlGetJobRunErrorRepository(p.pool, decompressor)
	p.jobRunDebugRepository = repository.NewSqlGetJobRunDebugMessageRepository(p.pool, decompressor)

	return nil
}

func (p *PostgresDatabase) applyTuningSQL(ctx context.Context) error {
	for i, stmt := range p.tuningSQLStatements {
		if _, err := p.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("executing tuning SQL statement %d: %w", i+1, err)
		}
		logging.Infof("Applied tuning SQL statement %d", i+1)
	}
	return nil
}

func (p *PostgresDatabase) revertTuningSQL(ctx context.Context) error {
	for i, stmt := range p.tuningRevertSQLStatements {
		if _, err := p.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("executing tuning revert SQL statement %d: %w", i+1, err)
		}
		logging.Infof("Executed tuning revert SQL statement %d", i+1)
	}
	return nil
}

// ExecuteIngestionQueryBatch executes a batch of ingestion queries using the
// same sequential phase ordering as the production Lookout ingester:
//
//  1. Create job rows and their specs (specs only for jobs whose InsertJobSpec
//     arrived in the same batch — avoids inserting null into job_spec.job_spec).
//  2. In parallel: update job rows (non-terminal only), create job runs, create job errors.
//  3. Update job runs.
//  4. (Hot/cold split only) Atomically apply terminal state updates and move those
//     jobs from job to job_historical.
//
// When HotColdSplit is enabled, terminal-state updates are separated out before
// Phase 2 so they never touch job (which is constrained to active states only).
// They are instead applied atomically in Phase 4 as part of the move operation.
//
// Job updates and job-run updates are conflated within the batch (last write
// wins) before being sent, preventing undefined behaviour when duplicate IDs
// appear in the UPDATE … FROM temp-table pattern.
func (p *PostgresDatabase) ExecuteIngestionQueryBatch(ctx context.Context, queries []IngestionQuery) error {
	set, err := queriesToInstructionSet(queries)
	if err != nil {
		return err
	}
	armadaCtx := armadacontext.FromGrpcCtx(ctx)

	jobUpdates := set.JobsToUpdate
	var terminalUpdates []*lookoutmodel.UpdateJobInstruction
	if p.features.HotColdSplit {
		jobUpdates = nil
		for _, u := range set.JobsToUpdate {
			if u.State != nil && isTerminalState(*u.State) {
				terminalUpdates = append(terminalUpdates, u)
			} else {
				jobUpdates = append(jobUpdates, u)
			}
		}
	}

	// Phase 1: job rows must be committed before job_run FK references them.
	var wg sync.WaitGroup
	wg.Go(func() { p.lookoutDb.CreateJobs(armadaCtx, set.JobsToCreate) })
	wg.Go(func() { p.lookoutDb.CreateJobSpecs(armadaCtx, set.JobsToCreate) })
	wg.Wait()

	// Phase 2: job runs, errors and job-state updates can proceed in parallel.
	// When HotColdSplit is enabled, jobUpdates contains only non-terminal updates,
	// so no terminal state is ever written to job (which has chk_job_active_state).
	wg.Go(func() { p.lookoutDb.UpdateJobs(armadaCtx, jobUpdates) })
	wg.Go(func() { p.lookoutDb.CreateJobRuns(armadaCtx, set.JobRunsToCreate) })
	wg.Go(func() { p.lookoutDb.CreateJobErrors(armadaCtx, set.JobErrorsToCreate) })
	wg.Wait()

	// Phase 3: job-run updates depend on job-run rows existing.
	p.lookoutDb.UpdateJobRuns(armadaCtx, set.JobRunsToUpdate)

	// Phase 4 (hot/cold split only): atomically apply terminal state updates and
	// move those jobs from job to job_historical in a single SQL statement.
	if p.features.HotColdSplit {
		if err := p.updateAndMoveTerminalJobs(ctx, terminalUpdates); err != nil {
			return fmt.Errorf("updating and moving terminal jobs: %w", err)
		}
	}

	return nil
}

// GetJobRunDebugMessage retrieves the debug message for a specific job run.
func (p *PostgresDatabase) GetJobRunDebugMessage(ctx context.Context, jobRunID string) (string, error) {
	armadaCtx := armadacontext.FromGrpcCtx(ctx)
	msg, err := p.jobRunDebugRepository.GetJobRunDebugMessage(armadaCtx, jobRunID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	return msg, nil
}

// GetJobRunError retrieves the error message for a specific job run.
func (p *PostgresDatabase) GetJobRunError(ctx context.Context, jobRunID string) (string, error) {
	armadaCtx := armadacontext.FromGrpcCtx(ctx)
	msg, err := p.jobRunErrorRepository.GetJobRunError(armadaCtx, jobRunID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	return msg, nil
}

// GetJobSpec retrieves the job specification for a specific job.
func (p *PostgresDatabase) GetJobSpec(ctx context.Context, jobID string) (*api.Job, error) {
	armadaCtx := armadacontext.FromGrpcCtx(ctx)
	return p.jobSpecRepository.GetJobSpec(armadaCtx, jobID)
}

// GetJobs retrieves jobs matching the given filters using the Lookout repository.
func (p *PostgresDatabase) GetJobs(ctx *context.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) ([]*model.Job, error) {
	armadaCtx := armadacontext.FromGrpcCtx(repository.ContextWithSlowQueryLoggingDisabled(*ctx))
	result, err := p.jobsRepository.GetJobs(armadaCtx, filters, activeJobSets, order, skip, take)
	if err != nil {
		return nil, err
	}
	return result.Jobs, nil
}

// GetJobGroups retrieves aggregated job groups using the Lookout repository.
func (p *PostgresDatabase) GetJobGroups(ctx *context.Context, filters []*model.Filter, order *model.Order, groupedField *model.GroupedField, aggregates []string, skip int, take int) ([]*model.JobGroup, error) {
	armadaCtx := armadacontext.FromGrpcCtx(repository.ContextWithSlowQueryLoggingDisabled(*ctx))

	result, err := p.groupRepository.GroupBy(armadaCtx, filters, false, order, groupedField, aggregates, skip, take)
	if err != nil {
		return nil, err
	}
	return result.Groups, nil
}

// TearDown truncates all tables to clean up after a test run.
// This is faster than dropping and recreating the database, and
// allows multiple test runs against the same database instance.
// When the HotColdSplit feature toggle is enabled, job_historical is also
// truncated and the hot/cold migration is reverted so the schema is left
// in its original state.
func (p *PostgresDatabase) TearDown(ctx context.Context) error {
	if err := p.revertTuningSQL(ctx); err != nil {
		return fmt.Errorf("reverting tuning SQL: %w", err)
	}

	tables := []string{
		"job_run",
		"job_spec",
		"job_error",
		"job",
		"job_deduplication",
	}
	if p.features.HotColdSplit {
		tables = append(tables, "job_historical")
	}

	for _, table := range tables {
		query := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table)
		if _, err := p.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("truncating table %s: %w", table, err)
		}
	}

	if p.features.HotColdSplit {
		if _, err := p.pool.Exec(ctx, hotColdRevertSQL); err != nil {
			return fmt.Errorf("reverting hot/cold split migration: %w", err)
		}
		logging.Info("Hot/cold split migration reverted")
	}

	return nil
}

// Close is a no-op: pgxpool.Close can hang on background health-check goroutines.
// Pool connections will be reclaimed by the OS on process exit.
func (p *PostgresDatabase) Close() {}

const (
	defaultHistoricalJobChunkSize = 500000
	maxChunkRetries               = 3
	retryBaseDelay                = 5 * time.Second
)

// PopulateHistoricalJobs inserts terminal historical jobs in chunks, with
// automatic resume on restart. Each chunk is a separate transaction so that
// progress survives interruptions. On entry the method queries the database
// to find the highest job index already present and resumes from there.
func (p *PostgresDatabase) PopulateHistoricalJobs(ctx context.Context, params HistoricalJobsParams) error {
	if params.NJobs == 0 {
		return nil
	}
	if strings.ContainsRune(params.QueueName, '\'') || strings.ContainsRune(params.JobSetName, '\'') {
		return fmt.Errorf("queue/jobset names must not contain single quotes")
	}

	chunkSize := params.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultHistoricalJobChunkSize
	}

	startFrom, err := p.detectHistoricalJobProgress(ctx, params.QueueIdx, params.JobSetIdx)
	if err != nil {
		return fmt.Errorf("detecting historical job progress: %w", err)
	}

	if startFrom >= params.NJobs {
		logging.Infof("Historical jobs: queue %q jobset %q: already complete (%d/%d)",
			params.QueueName, params.JobSetName, params.NJobs, params.NJobs)
		return nil
	}

	if startFrom > 0 {
		logging.Infof("Historical jobs: queue %q jobset %q: resuming from %d/%d (%.1f%%)",
			params.QueueName, params.JobSetName, startFrom, params.NJobs,
			float64(startFrom)/float64(params.NJobs)*100)
	}

	for chunkStart := startFrom; chunkStart < params.NJobs; chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > params.NJobs {
			chunkEnd = params.NJobs
		}
		chunkLastIdx := chunkEnd - 1

		if err := p.insertHistoricalJobChunkWithRetry(ctx, params, chunkStart, chunkLastIdx); err != nil {
			return fmt.Errorf("inserting historical jobs chunk [%d, %d] for queue %s jobset %s: %w",
				chunkStart, chunkLastIdx, params.QueueName, params.JobSetName, err)
		}

		logging.Infof("Historical jobs: queue %q jobset %q: %d/%d (%.1f%%)",
			params.QueueName, params.JobSetName, chunkEnd, params.NJobs,
			float64(chunkEnd)/float64(params.NJobs)*100)
	}

	return nil
}

// detectHistoricalJobProgress queries the database for the highest job index
// already inserted for the given (queueIdx, jobSetIdx) pair. Returns the
// index to resume from (i.e. maxExisting + 1), or 0 if nothing exists.
func (p *PostgresDatabase) detectHistoricalJobProgress(ctx context.Context, queueIdx, jobSetIdx int) (int, error) {
	prefix := fmt.Sprintf("%04d%04d", queueIdx, jobSetIdx)
	table := "job"
	if p.features.HotColdSplit {
		table = "job_historical"
	}
	var maxIdx int
	err := p.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COALESCE(MAX(RIGHT(job_id, 10)::int), -1) FROM %s WHERE job_id LIKE $1`, table),
		prefix+"%",
	).Scan(&maxIdx)
	if err != nil {
		return 0, fmt.Errorf("querying max job index for prefix %s: %w", prefix, err)
	}
	if maxIdx < 0 {
		return 0, nil
	}
	return maxIdx + 1, nil
}

// insertHistoricalJobChunkWithRetry attempts to insert a chunk, retrying on
// transient failures. Between retries it re-queries progress in case the
// previous attempt actually committed (e.g. connection dropped after the
// server processed COMMIT but before the client received the ack).
func (p *PostgresDatabase) insertHistoricalJobChunkWithRetry(ctx context.Context, params HistoricalJobsParams, chunkStart, chunkLastIdx int) error {
	var lastErr error
	for attempt := range maxChunkRetries + 1 {
		if attempt > 0 {
			progress, err := p.detectHistoricalJobProgress(ctx, params.QueueIdx, params.JobSetIdx)
			if err != nil {
				logging.WithError(err).Warn("Failed to re-check progress during retry")
			} else if progress > chunkLastIdx {
				return nil
			}

			backoff := time.Duration(attempt) * retryBaseDelay
			logging.Infof("Historical jobs: retrying chunk [%d, %d] (attempt %d/%d) after %v",
				chunkStart, chunkLastIdx, attempt+1, maxChunkRetries+1, backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		lastErr = p.insertHistoricalJobChunk(ctx, params, chunkStart, chunkLastIdx)
		if lastErr == nil {
			return nil
		}

		logging.WithError(lastErr).Warnf("Historical jobs: chunk [%d, %d] failed (attempt %d/%d)",
			chunkStart, chunkLastIdx, attempt+1, maxChunkRetries+1)
	}
	return lastErr
}

// insertHistoricalJobChunk inserts a single chunk [startIdx, lastIdx] of
// historical jobs in one transaction using server-side generate_series.
func (p *PostgresDatabase) insertHistoricalJobChunk(ctx context.Context, params HistoricalJobsParams, startIdx, lastIdx int) error {
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

	jobTable := "job"
	if p.features.HotColdSplit {
		jobTable = "job_historical"
	}

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
    NOW() - INTERVAL '24 hours',
    CASE WHEN i%%1000 < %d THEN %d
         WHEN i%%1000 < %d THEN %d
         WHEN i%%1000 < %d THEN %d
         ELSE %d END,
    NOW() - INTERVAL '24 hours' + INTERVAL '10 seconds',
    EXTRACT(EPOCH FROM NOW() - INTERVAL '24 hours' + INTERVAL '10 seconds')::bigint,
    (%s)[i%%%d+1],
    %s,
    '%s' || lpad(i::text, 10, '0') || '00',
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d
         THEN NOW() - INTERVAL '24 hours' + INTERVAL '10 seconds' END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d
         THEN 'user requested' END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d
         THEN '%s' END
FROM generate_series(%d, %d) AS i`,
		jobTable,
		prefix,
		params.QueueName, params.QueueName,
		nsArr, len(jobspec.NamespaceOptions),
		params.JobSetName,
		cpuArr, len(jobspec.CpuOptions),
		memArr, len(jobspec.MemoryOptions),
		ephArr, len(jobspec.EphemeralStorageOptions),
		gpuArr, len(jobspec.GpuOptions),
		succeeded, lookout.JobSucceededOrdinal,
		errored, lookout.JobFailedOrdinal,
		cancelled, lookout.JobCancelledOrdinal,
		lookout.JobPreemptedOrdinal,
		pcArr, len(jobspec.PriorityClassOptions),
		buildAnnotationSQL(),
		prefix,
		errored, cancelled,
		errored, cancelled,
		errored, cancelled, params.QueueName,
		startIdx, lastIdx,
	)

	jobSpecSQL := fmt.Sprintf(`
INSERT INTO job_spec (job_id, job_spec)
SELECT '%s' || lpad(i::text, 10, '0'), $1::bytea
FROM generate_series(%d, %d) AS i`,
		prefix, startIdx, lastIdx,
	)

	jobRunSQL := fmt.Sprintf(`
INSERT INTO job_run (
    run_id, job_id, cluster, node, leased, pending, started, finished,
    pool, job_run_state, error, debug, exit_code
)
SELECT
    '%s' || lpad(i::text, 10, '0') || '00',
    '%s' || lpad(i::text, 10, '0'),
    'broadside-cluster-' || (i%%40+1),
    'broadside-cluster-' || (i%%40+1) || '-node-' || (i%%80+1),
    NOW() - INTERVAL '24 hours' + INTERVAL '1 second',
    NOW() - INTERVAL '24 hours' + INTERVAL '2 seconds',
    NOW() - INTERVAL '24 hours' + INTERVAL '3 seconds',
    NOW() - INTERVAL '24 hours' + INTERVAL '10 seconds',
    (%s)[i%%%d+1],
    CASE WHEN i%%1000 < %d THEN 3
         WHEN i%%1000 < %d THEN 4
         WHEN i%%1000 < %d THEN 6
         ELSE 5 END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d THEN $1::bytea
         WHEN i%%1000 >= %d                  THEN $3::bytea
    END,
    CASE WHEN i%%1000 >= %d AND i%%1000 < %d THEN $2::bytea END,
    CASE WHEN i%%1000 < %d THEN 0 END
FROM generate_series(%d, %d) AS i`,
		prefix, prefix,
		poolArr, len(jobspec.PoolOptions),
		succeeded, errored, cancelled,
		succeeded, errored,
		cancelled,
		succeeded, errored,
		errored,
		startIdx, lastIdx,
	)

	jobErrorSQL := fmt.Sprintf(`
INSERT INTO job_error (job_id, error)
SELECT '%s' || lpad(i::text, 10, '0'), $1::bytea
FROM generate_series(%d, %d) AS i
WHERE i%%1000 >= %d AND i%%1000 < %d`,
		prefix, startIdx, lastIdx, succeeded, errored,
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
			return fmt.Errorf("executing historical jobs SQL: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func int64SliceToSQL(vals []int64) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = fmt.Sprintf("%d", v)
	}
	return "ARRAY[" + strings.Join(parts, ",") + "]"
}

func stringSliceToSQL(vals []string) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = "'" + v + "'"
	}
	return "ARRAY[" + strings.Join(parts, ",") + "]"
}

func buildAnnotationSQL() string {
	parts := make([]string, 0, len(jobspec.AnnotationConfigs)*2)
	for _, ac := range jobspec.AnnotationConfigs {
		parts = append(parts,
			fmt.Sprintf("'%s'", ac.Key),
			fmt.Sprintf("'value-' || (i%%%d)", ac.MaxUniqueValues),
		)
	}
	return "jsonb_build_object(" + strings.Join(parts, ", ") + ")"
}
