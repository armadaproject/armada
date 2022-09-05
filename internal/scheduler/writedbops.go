package scheduler

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
)

// TODO: The caller of this function should keep retrying on transient failures.
func WriteDbOp(ctx context.Context, db *pgxpool.Pool, op DbOperation) error {
	queries := New(db)
	switch o := op.(type) {
	case InsertJobs:
		records := make([]interface{}, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := Upsert(ctx, db, "jobs", JobsSchema(), records)
		if err != nil {
			return err
		}
	case InsertRuns:
		records := make([]interface{}, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := Upsert(ctx, db, "runs", RunsSchema(), records)
		if err != nil {
			return err
		}
	case InsertRunAssignments:
		records := make([]interface{}, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := Upsert(ctx, db, "job_run_assignments", JobRunAssignmentSchema(), records)
		if err != nil {
			return err
		}
	case UpdateJobSetPriorities:
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
			err := queries.UpdateJobPriorityById(ctx, UpdateJobPriorityByIdParams{
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
		records := make([]interface{}, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := Upsert(ctx, db, "job_errors", JobErrorsSchema(), records)
		if err != nil {
			return err
		}
	case InsertJobRunErrors:
		records := make([]interface{}, len(o))
		i := 0
		for _, v := range o {
			records[i] = *v
			i++
		}
		err := Upsert(ctx, db, "job_run_errors", JobRunErrorsSchema(), records)
		if err != nil {
			return err
		}
	default:
		return errors.Errorf("received unexpected op %+v", op)
	}
	return nil
}
