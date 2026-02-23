package db

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/internal/lookout/schema"
	"github.com/armadaproject/armada/internal/lookoutingester/lookoutdb"
	lookoutmodel "github.com/armadaproject/armada/internal/lookoutingester/model"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

// PostgresDatabase implements the Database interface using PostgreSQL.
// It reuses the Lookout schema and query infrastructure to ensure
// realistic testing of production query patterns.
type PostgresDatabase struct {
	config                map[string]string
	pool                  *pgxpool.Pool
	lookoutDb             *lookoutdb.LookoutDb
	jobsRepository        *repository.SqlGetJobsRepository
	groupRepository       *repository.SqlGroupJobsRepository
	jobSpecRepository     *repository.SqlGetJobSpecRepository
	jobRunErrorRepository *repository.SqlGetJobRunErrorRepository
	jobRunDebugRepository *repository.SqlGetJobRunDebugMessageRepository
}

// NewPostgresDatabase creates a new PostgresDatabase instance.
// The config map should contain connection parameters compatible with libpq:
//   - host: database host (e.g., "localhost")
//   - port: database port (e.g., "5433")
//   - user: database user (e.g., "postgres")
//   - password: database password
//   - dbname: database name (e.g., "broadside_test")
//   - sslmode: SSL mode (e.g., "disable")
func NewPostgresDatabase(config map[string]string) *PostgresDatabase {
	return &PostgresDatabase{config: config}
}

// InitialiseSchema opens the connection pool, applies the Lookout database
// migrations, and initialises the query repository.
func (p *PostgresDatabase) InitialiseSchema(ctx context.Context) error {
	pgConfig := configuration.PostgresConfig{
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

	decompressor := &compress.NoOpDecompressor{}
	p.lookoutDb = lookoutdb.NewLookoutDb(p.pool, nil, nil, 0)
	p.jobsRepository = repository.NewSqlGetJobsRepository(p.pool)
	p.groupRepository = repository.NewSqlGroupJobsRepository(p.pool)
	p.jobSpecRepository = repository.NewSqlGetJobSpecRepository(p.pool, decompressor)
	p.jobRunErrorRepository = repository.NewSqlGetJobRunErrorRepository(p.pool, decompressor)
	p.jobRunDebugRepository = repository.NewSqlGetJobRunDebugMessageRepository(p.pool, decompressor)

	return nil
}

// ExecuteIngestionQueryBatch executes a batch of ingestion queries.
// Jobs are grouped by type and inserted/updated in the appropriate order to maintain
// referential integrity.
func (p *PostgresDatabase) ExecuteIngestionQueryBatch(ctx context.Context, queries []IngestionQuery) error {
	// Group queries by type for batch processing
	var jobsToInsert []InsertJob
	var jobSpecsToInsert []InsertJobSpec
	var jobRunsToInsert []InsertJobRun
	var jobErrorsToInsert []InsertJobError
	var jobUpdates []IngestionQuery
	var jobRunUpdates []IngestionQuery

	for _, query := range queries {
		switch q := query.(type) {
		case InsertJob:
			jobsToInsert = append(jobsToInsert, q)
		case InsertJobSpec:
			jobSpecsToInsert = append(jobSpecsToInsert, q)
		case InsertJobRun:
			jobRunsToInsert = append(jobRunsToInsert, q)
		case InsertJobError:
			jobErrorsToInsert = append(jobErrorsToInsert, q)
		case UpdateJobPriority, SetJobCancelled, SetJobSucceeded, SetJobPreempted,
			SetJobRejected, SetJobErrored, SetJobRunning, SetJobPending, SetJobLeased:
			jobUpdates = append(jobUpdates, query)
		case SetJobRunStarted, SetJobRunPending, SetJobRunCancelled,
			SetJobRunFailed, SetJobRunSucceeded, SetJobRunPreempted:
			jobRunUpdates = append(jobRunUpdates, query)
		default:
			return fmt.Errorf("unknown ingestion query type: %T", query)
		}
	}

	// Insert jobs first (required by foreign key constraints)
	if err := p.insertJobs(ctx, jobsToInsert); err != nil {
		return fmt.Errorf("inserting jobs: %w", err)
	}

	// Insert job specs, runs, and errors in parallel (they all only depend on jobs existing)
	var specErr, runErr, errorErr error
	var wg sync.WaitGroup

	wg.Go(func() { specErr = p.insertJobSpecs(ctx, jobSpecsToInsert) })
	wg.Go(func() { runErr = p.insertJobRuns(ctx, jobRunsToInsert) })
	wg.Go(func() { errorErr = p.insertJobErrors(ctx, jobErrorsToInsert) })

	wg.Wait()

	if specErr != nil {
		return fmt.Errorf("inserting job specs: %w", specErr)
	}
	if runErr != nil {
		return fmt.Errorf("inserting job runs: %w", runErr)
	}
	if errorErr != nil {
		return fmt.Errorf("inserting job errors: %w", errorErr)
	}

	// Update jobs and job runs (can be done in parallel)
	var jobUpdateErr, jobRunUpdateErr error

	wg.Go(func() { jobUpdateErr = p.updateJobs(ctx, jobUpdates) })
	wg.Go(func() { jobRunUpdateErr = p.updateJobRuns(ctx, jobRunUpdates) })

	wg.Wait()

	if jobUpdateErr != nil {
		return fmt.Errorf("updating jobs: %w", jobUpdateErr)
	}
	if jobRunUpdateErr != nil {
		return fmt.Errorf("updating job runs: %w", jobRunUpdateErr)
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
func (p *PostgresDatabase) TearDown(ctx context.Context) error {
	tables := []string{
		"job_run",
		"job_spec",
		"job_error",
		"job",
		"job_deduplication",
	}

	for _, table := range tables {
		query := fmt.Sprintf("TRUNCATE TABLE %s CASCADE", table)
		if _, err := p.pool.Exec(ctx, query); err != nil {
			return fmt.Errorf("truncating table %s: %w", table, err)
		}
	}

	return nil
}

// Close closes the database connection pool.
func (p *PostgresDatabase) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}

// insertJobs batch inserts jobs using the COPY protocol for performance.
func (p *PostgresDatabase) insertJobs(ctx context.Context, jobs []InsertJob) error {
	if len(jobs) == 0 {
		return nil
	}

	// Use COPY protocol for bulk insert (much faster than individual INSERTs)
	rows := make([][]interface{}, len(jobs))
	for i, job := range jobs {
		// Convert annotations map to JSONB
		var annotations interface{}
		if len(job.Job.Annotations) > 0 {
			annotations = job.Job.Annotations
		}

		rows[i] = []interface{}{
			job.Job.JobID,
			job.Job.Queue,
			job.Job.Owner,
			job.Job.Namespace,
			job.Job.JobSet,
			job.Job.Cpu,
			job.Job.Memory,
			job.Job.EphemeralStorage,
			job.Job.Gpu,
			job.Job.Priority,
			job.Job.Submitted,
			0,                        // state (queued)
			job.Job.Submitted,        // last_transition_time
			job.Job.Submitted.Unix(), // last_transition_time_seconds
			job.Job.PriorityClass,
			annotations,
		}
	}

	_, err := p.pool.CopyFrom(
		ctx,
		[]string{"job"}, // table name
		[]string{
			"job_id",
			"queue",
			"owner",
			"namespace",
			"jobset",
			"cpu",
			"memory",
			"ephemeral_storage",
			"gpu",
			"priority",
			"submitted",
			"state",
			"last_transition_time",
			"last_transition_time_seconds",
			"priority_class",
			"annotations",
		},
		&copyFromRows{rows: rows},
	)
	if err != nil {
		return fmt.Errorf("copying %d jobs: %w", len(jobs), err)
	}

	return nil
}

// insertJobSpecs batch inserts job specifications.
func (p *PostgresDatabase) insertJobSpecs(ctx context.Context, specs []InsertJobSpec) error {
	if len(specs) == 0 {
		return nil
	}

	rows := make([][]interface{}, len(specs))
	for i, spec := range specs {
		rows[i] = []interface{}{
			spec.JobID,
			[]byte(spec.JobSpec),
		}
	}

	_, err := p.pool.CopyFrom(
		ctx,
		[]string{"job_spec"},
		[]string{"job_id", "job_spec"},
		&copyFromRows{rows: rows},
	)
	if err != nil {
		return fmt.Errorf("copying %d job specs: %w", len(specs), err)
	}

	return nil
}

// insertJobRuns batch inserts job run records.
func (p *PostgresDatabase) insertJobRuns(ctx context.Context, runs []InsertJobRun) error {
	if len(runs) == 0 {
		return nil
	}

	rows := make([][]interface{}, len(runs))
	for i, run := range runs {
		var node *string
		if run.Node != "" {
			node = &run.Node
		}
		var pool *string
		if run.Pool != "" {
			pool = &run.Pool
		}

		rows[i] = []interface{}{
			run.JobRunID,
			run.JobID,
			run.Cluster,
			node,
			run.Time, // leased time
			pool,
			0, // job_run_state (leased)
		}
	}

	_, err := p.pool.CopyFrom(
		ctx,
		[]string{"job_run"},
		[]string{
			"run_id",
			"job_id",
			"cluster",
			"node",
			"leased",
			"pool",
			"job_run_state",
		},
		&copyFromRows{rows: rows},
	)
	if err != nil {
		return fmt.Errorf("copying %d job runs: %w", len(runs), err)
	}

	return nil
}

// insertJobErrors batch inserts job error records.
func (p *PostgresDatabase) insertJobErrors(ctx context.Context, errors []InsertJobError) error {
	if len(errors) == 0 {
		return nil
	}

	rows := make([][]interface{}, len(errors))
	for i, err := range errors {
		rows[i] = []interface{}{
			err.JobID,
			err.Error,
		}
	}

	_, err := p.pool.CopyFrom(
		ctx,
		[]string{"job_error"},
		[]string{"job_id", "error"},
		&copyFromRows{rows: rows},
	)
	if err != nil {
		return fmt.Errorf("copying %d job errors: %w", len(errors), err)
	}

	return nil
}

// updateJobs converts Broadside query types to Lookout instructions
// and delegates to the production Lookout ingester for batch updates.
func (p *PostgresDatabase) updateJobs(ctx context.Context, queries []IngestionQuery) error {
	if len(queries) == 0 {
		return nil
	}

	// Convert Broadside queries to Lookout UpdateJobInstruction format
	instructions := make([]*lookoutmodel.UpdateJobInstruction, len(queries))
	for i, query := range queries {
		instruction := &lookoutmodel.UpdateJobInstruction{}
		switch q := query.(type) {
		case UpdateJobPriority:
			instruction.JobId = q.JobID
			instruction.Priority = &q.Priority
		case SetJobCancelled:
			instruction.JobId = q.JobID
			state := int32(6)
			instruction.State = &state
			instruction.Cancelled = &q.Time
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
			instruction.CancelReason = &q.CancelReason
			instruction.CancelUser = &q.CancelUser
		case SetJobSucceeded:
			instruction.JobId = q.JobID
			state := int32(5)
			instruction.State = &state
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
		case SetJobPreempted:
			instruction.JobId = q.JobID
			state := int32(8)
			instruction.State = &state
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
		case SetJobRejected:
			instruction.JobId = q.JobID
			state := int32(7)
			instruction.State = &state
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
		case SetJobErrored:
			instruction.JobId = q.JobID
			state := int32(4)
			instruction.State = &state
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
		case SetJobRunning:
			instruction.JobId = q.JobID
			state := int32(3)
			instruction.State = &state
			instruction.LatestRunId = &q.LatestRunID
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
		case SetJobPending:
			instruction.JobId = q.JobID
			state := int32(2)
			instruction.State = &state
			instruction.LatestRunId = &q.RunID
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
		case SetJobLeased:
			instruction.JobId = q.JobID
			state := int32(1)
			instruction.State = &state
			instruction.LatestRunId = &q.RunID
			instruction.LastTransitionTime = &q.Time
			seconds := q.Time.Unix()
			instruction.LastTransitionTimeSeconds = &seconds
		default:
			return fmt.Errorf("unknown job update query type: %T", query)
		}
		instructions[i] = instruction
	}

	// Delegate to the production Lookout ingester
	armadaCtx := armadacontext.FromGrpcCtx(ctx)
	return p.lookoutDb.UpdateJobsBatch(armadaCtx, instructions)
}

// updateJobRuns converts Broadside query types to Lookout instructions
// and delegates to the production Lookout ingester for batch updates.
func (p *PostgresDatabase) updateJobRuns(ctx context.Context, queries []IngestionQuery) error {
	if len(queries) == 0 {
		return nil
	}

	// Convert Broadside queries to Lookout UpdateJobRunInstruction format
	instructions := make([]*lookoutmodel.UpdateJobRunInstruction, len(queries))
	for i, query := range queries {
		instruction := &lookoutmodel.UpdateJobRunInstruction{}
		switch q := query.(type) {
		case SetJobRunStarted:
			instruction.RunId = q.JobRunID
			instruction.Node = &q.Node
			instruction.Started = &q.Time
			state := int32(2)
			instruction.JobRunState = &state
		case SetJobRunPending:
			instruction.RunId = q.JobRunID
			instruction.Pending = &q.Time
			state := int32(1)
			instruction.JobRunState = &state
		case SetJobRunCancelled:
			instruction.RunId = q.JobRunID
			instruction.Finished = &q.Time
			state := int32(6)
			instruction.JobRunState = &state
		case SetJobRunFailed:
			instruction.RunId = q.JobRunID
			instruction.Finished = &q.Time
			instruction.Error = q.Error
			instruction.Debug = q.Debug
			instruction.ExitCode = &q.ExitCode
			state := int32(4)
			instruction.JobRunState = &state
		case SetJobRunSucceeded:
			instruction.RunId = q.JobRunID
			instruction.Finished = &q.Time
			instruction.ExitCode = &q.ExitCode
			state := int32(3)
			instruction.JobRunState = &state
		case SetJobRunPreempted:
			instruction.RunId = q.JobRunID
			instruction.Finished = &q.Time
			instruction.Error = q.Error
			state := int32(5)
			instruction.JobRunState = &state
		default:
			return fmt.Errorf("unknown job run update query type: %T", query)
		}
		instructions[i] = instruction
	}

	// Delegate to the production Lookout ingester
	armadaCtx := armadacontext.FromGrpcCtx(ctx)
	return p.lookoutDb.UpdateJobRunsBatch(armadaCtx, instructions)
}

// copyFromRows implements pgx.CopyFromSource for batch inserts.
type copyFromRows struct {
	rows [][]interface{}
	idx  int
}

func (c *copyFromRows) Next() bool {
	c.idx++
	return c.idx <= len(c.rows)
}

func (c *copyFromRows) Values() ([]interface{}, error) {
	if c.idx > len(c.rows) {
		return nil, fmt.Errorf("index out of range")
	}
	return c.rows[c.idx-1], nil
}

func (c *copyFromRows) Err() error {
	return nil
}
