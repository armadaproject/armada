package scheduleringester

import (
	"context"
	"time"

	"github.com/G-Research/armada/internal/common/database"
	"github.com/G-Research/armada/internal/scheduler/sqlc"

	"github.com/hashicorp/go-multierror"

	"github.com/G-Research/armada/internal/common/armadaerrors"

	"github.com/G-Research/armada/internal/scheduler"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"

	"github.com/G-Research/armada/internal/common/ingest"
	"github.com/G-Research/armada/internal/common/ingest/metrics"
)

// SchedulerDb writes DbOperations into postgres.
type SchedulerDb struct {
	// Connection to the postgres database.
	db             *pgxpool.Pool
	metrics        *metrics.Metrics
	initialBackOff time.Duration
	maxBackOff     time.Duration
}

func NewSchedulerDb(db *pgxpool.Pool, metrics *metrics.Metrics, initialBackOff time.Duration, maxBackOff time.Duration) ingest.Sink[*DbOperationsWithMessageIds] {
	return &SchedulerDb{db: db, metrics: metrics, initialBackOff: initialBackOff, maxBackOff: maxBackOff}
}

func (s *SchedulerDb) Store(ctx context.Context, instructions *DbOperationsWithMessageIds) error {
	var result *multierror.Error = nil
	for _, dbOp := range instructions.Ops {
		err := ingest.WithRetry(func() (bool, error) {
			err := s.WriteDbOp(ctx, dbOp)
			shouldRetry := armadaerrors.IsNetworkError(err) || armadaerrors.IsRetryablePostgresError(err)
			return shouldRetry, err
		}, s.initialBackOff, s.maxBackOff)
		multierror.Append(result, err)
	}
	return result.ErrorOrNil()
}

func (s *SchedulerDb) WriteDbOp(ctx context.Context, op DbOperation) error {
	queries := sqlc.New(s.db)
	switch o := op.(type) {
	case InsertJobs:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, s.db, "jobs", scheduler.JobsSchema(), records)
		if err != nil {
			return err
		}
	case InsertRuns:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, s.db, "runs", scheduler.RunsSchema(), records)
		if err != nil {
			return err
		}
	case InsertRunAssignments:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, s.db, "job_run_assignments", scheduler.JobRunAssignmentSchema(), records)
		if err != nil {
			return err
		}
	case UpdateJobSetPriorities:
		for jobSet, priority := range o {
			err := queries.UpdateJobPriorityByJobSet(
				ctx,
				sqlc.UpdateJobPriorityByJobSetParams{
					JobSet:   jobSet,
					Priority: priority,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkJobSetsCancelled:
		jobSets := maps.Keys(o)
		err := queries.MarkJobsCancelledBySets(ctx, jobSets)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsCancelledBySets(ctx, jobSets)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsCancelled:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsCancelledById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsCancelledByJobId(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsSucceeded:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsSucceededById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkJobsFailed:
		jobIds := maps.Keys(o)
		err := queries.MarkJobsFailedById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case UpdateJobPriorities:
		// TODO: This will be slow if there's a large number of ids.
		// Could be addressed by using a separate table for priority + upsert.
		for jobId, priority := range o {
			err := queries.UpdateJobPriorityById(ctx, sqlc.UpdateJobPriorityByIdParams{
				JobID:    jobId,
				Priority: priority,
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkRunsSucceeded:
		runIds := maps.Keys(o)
		err := queries.MarkJobRunsSucceededById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsFailed:
		runIds := maps.Keys(o)
		err := queries.MarkJobRunsFailedById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case MarkRunsRunning:
		runIds := maps.Keys(o)
		err := queries.MarkJobRunsRunningById(ctx, runIds)
		if err != nil {
			return errors.WithStack(err)
		}
	case InsertJobErrors:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, s.db, "job_errors", scheduler.JobErrorsSchema(), records)
		if err != nil {
			return err
		}
	case InsertJobRunErrors:
		records := make([]any, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := database.Upsert(ctx, s.db, "job_run_errors", scheduler.JobRunErrorsSchema(), records)
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("received unexpected op %+v", op)
	}
	return nil
}
