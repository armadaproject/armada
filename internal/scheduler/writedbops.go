package scheduler

import (
	"context"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// TODO: The caller of this function should keep retrying on transient failures.
func WriteDbOp(ctx context.Context, db *pgxpool.Pool, op DbOperation) error {
	switch o := op.(type) {
	case InsertJobs:
		records := make([]interface{}, len(o))
		i := 0
		for _, job := range o {
			records[i] = *job
			i++
		}
		err := Upsert(ctx, db, "jobs", JobsSchema(), records)
		if err != nil {
			return err
		}
	case InsertRuns:
		records := make([]interface{}, len(o))
		i := 0
		for _, run := range o {
			records[i] = *run
			i++
		}
		err := Upsert(ctx, db, "runs", RunsSchema(), records)
		if err != nil {
			return err
		}
	case InsertRunAssignments:
		records := make([]interface{}, len(o))
		i := 0
		for _, jobRunAssignment := range o {
			records[i] = *jobRunAssignment
			i++
		}
		err := Upsert(ctx, db, "job_run_assignments", JobRunAssignmentSchema(), records)
		if err != nil {
			return err
		}
	case UpdateJobSetPriorities:
		queries := New(db)
		for jobSet, priority := range o {
			err := queries.UpdateJobPriorityByJobSet(
				ctx,
				UpdateJobPriorityByJobSetParams{
					JobSet:   jobSet,
					Priority: priority,
				},
			)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	case MarkJobSetsCancelled:
	case MarkJobsCancelled:
	case MarkJobsSucceeded:
	case MarkJobsFailed:
	case UpdateJobPriorities:
	case MarkRunsSucceeded:
	case MarkRunsFailed:
	case MarkRunsRunning:
	case InsertJobErrors:
	case InsertJobRunErrors:
	default:
		return errors.Errorf("received unexpected op %+v", op)
	}
	return nil
}

func (srv *Ingester) ProcessBatch(ctx context.Context, batch *Batch) error {
	log := ctxlogrus.Extract(ctx)
	queries := New(srv.Db)

	// Jobs

	log.Infof("wrote %d jobs into postgres", len(records))

	// Job runs

	// Reprioritisations
	for _, reprioritisation := range batch.Reprioritisations {

		// TODO: This will be slow if there's a large number of ids.
		// Could be addressed by using a separate table for priority + upsert.
		for jobId, priority := range reprioritisation.PrioritiesByJob {
			err := queries.UpdateJobPriorityById(ctx, UpdateJobPriorityByIdParams{
				JobID:    jobId,
				Priority: priority,
			})
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	// Job sets cancelled
	jobSets := make([]string, 0, len(batch.JobSetsCancelled))
	for jobSet, _ := range batch.JobSetsCancelled {
		jobSets = append(jobSets, jobSet)
	}
	if len(jobSets) > 0 {
		err := queries.MarkJobsCancelledBySets(ctx, jobSets)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsCancelledBySets(ctx, jobSets)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Jobs cancelled
	if len(batch.JobsCancelled) > 0 {
		jobIds := idsFromMap(batch.JobsCancelled)
		err := queries.MarkJobsCancelledById(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
		err = queries.MarkJobRunsCancelledByJobId(ctx, jobIds)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Jobs succeeded
	err = queries.MarkJobsSucceededById(ctx, idsFromMap(batch.JobsSucceeded))
	if err != nil {
		return errors.WithStack(err)
	}

	// Job errors
	records = make([]interface{}, len(batch.JobErrors))
	failedJobIds := make([]uuid.UUID, 0)
	for i, jobError := range batch.JobErrors {
		records[i] = jobError
		if jobError.Terminal {
			failedJobIds = append(failedJobIds, jobError.JobID)
		}
	}
	err = Upsert(ctx, srv.Db, "job_errors", JobErrorsSchema(), records)
	if err != nil {
		return err // TODO: Keep retrying on transient failures.
	}
	log.Infof("wrote %d errors into postgres", len(records))

	// For terminal errors, mark the corresponding job as failed.
	if len(failedJobIds) > 0 {
		err := queries.MarkJobsFailedById(ctx, failedJobIds)
		if err != nil {
			return err
		}
	}

	// Job runs running
	if len(batch.JobRunsRunning) > 0 {
		err := queries.MarkJobRunsRunningById(ctx, idsFromMap(batch.JobRunsRunning))
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Job runs succeeded
	if len(batch.JobRunsSucceeded) > 0 {
		err := queries.MarkJobRunsSucceededById(ctx, idsFromMap(batch.JobRunsSucceeded))
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Job run errors
	records = make([]interface{}, len(batch.JobRunErrors))
	failedJobRunIds := make([]uuid.UUID, 0)
	for i, jobRunError := range batch.JobRunErrors {
		records[i] = jobRunError
		if jobRunError.Terminal {
			failedJobRunIds = append(failedJobRunIds, jobRunError.RunID)
		}
	}
	err = Upsert(ctx, srv.Db, "job_run_errors", JobRunErrorsSchema(), records)
	if err != nil {
		return err // TODO: Keep retrying on transient failures.
	}
	log.Infof("wrote %d run errors into postgres", len(records))

	// For terminal errors, mark the corresponding job as failed.
	if len(failedJobRunIds) > 0 {
		queries.MarkJobRunsFailedById(ctx, failedJobRunIds)
	}

	return nil
}

func idsFromMap(set map[uuid.UUID]bool) []uuid.UUID {
	ids := make([]uuid.UUID, len(set))
	i := 0
	for id := range set {
		ids[i] = id
		i++
	}
	return ids
}
