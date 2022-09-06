package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/constraints"
)

func TestWriteOps(t *testing.T) {
	jobIds := make([]uuid.UUID, 10)
	for i := range jobIds {
		jobIds[i] = uuid.New()
	}
	runIds := make([]uuid.UUID, 10)
	for i := range runIds {
		runIds[i] = uuid.New()
	}
	tests := map[string]struct {
		Ops []DbOperation
	}{
		"InsertJobs": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
			},
			InsertJobs{
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
		}},
		"InsertRuns": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			InsertRuns{
				runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]},
				runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]},
			},
			InsertRuns{
				runIds[2]: &Run{JobID: jobIds[2], RunID: runIds[2]},
				runIds[3]: &Run{JobID: jobIds[3], RunID: runIds[3]},
			},
		}},
		"UpdateJobSetPriorities": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			UpdateJobSetPriorities{"set1": 1},
		}},
		"UpdateJobPriorities": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			UpdateJobPriorities{
				jobIds[0]: 1,
				jobIds[1]: 3,
			},
		}},
		"MarkJobSetsCancelled": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			InsertRuns{
				runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0], JobSet: "set1"},
				runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1], JobSet: "set2"},
				runIds[2]: &Run{JobID: jobIds[2], RunID: runIds[2], JobSet: "set1"},
				runIds[3]: &Run{JobID: jobIds[3], RunID: runIds[3], JobSet: "set2"},
			},
			MarkJobSetsCancelled{"set1": true},
		}},
		"MarkJobsCancelled": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			InsertRuns{
				runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]},
				runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]},
				runIds[2]: &Run{JobID: jobIds[2], RunID: runIds[2]},
				runIds[3]: &Run{JobID: jobIds[3], RunID: runIds[3]},
			},
			MarkJobsCancelled{
				jobIds[0]: true,
				jobIds[1]: true,
			},
		}},
		"MarkJobsSucceeded": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			MarkJobsSucceeded{
				jobIds[0]: true,
				jobIds[1]: true,
			},
		}},
		"MarkJobsFailed": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			MarkJobsFailed{
				jobIds[0]: true,
				jobIds[1]: true,
			},
		}},
		"MarkRunsSucceeded": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			InsertRuns{
				runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]},
				runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]},
				runIds[2]: &Run{JobID: jobIds[2], RunID: runIds[2]},
				runIds[3]: &Run{JobID: jobIds[3], RunID: runIds[3]},
			},
			MarkRunsSucceeded{
				runIds[0]: true,
				runIds[1]: true,
			},
		}},
		"MarkRunsFailed": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			InsertRuns{
				runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]},
				runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]},
				runIds[2]: &Run{JobID: jobIds[2], RunID: runIds[2]},
				runIds[3]: &Run{JobID: jobIds[3], RunID: runIds[3]},
			},
			MarkRunsFailed{
				runIds[0]: true,
				runIds[1]: true,
			},
		}},
		"MarkRunsRunning": {Ops: []DbOperation{
			InsertJobs{
				jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"},
				jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"},
				jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"},
				jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"},
			},
			InsertRuns{
				runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]},
				runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]},
				runIds[2]: &Run{JobID: jobIds[2], RunID: runIds[2]},
				runIds[3]: &Run{JobID: jobIds[3], RunID: runIds[3]},
			},
			MarkRunsRunning{
				runIds[0]: true,
				runIds[1]: true,
			},
		}},
		"InsertJobErrors": {Ops: []DbOperation{
			InsertJobErrors{
				0: &JobError{ID: 0, JobID: jobIds[0]},
			},
			InsertJobErrors{
				1: &JobError{ID: 1, JobID: jobIds[1]},
			},
		}},
		"InsertJobRunErrors": {Ops: []DbOperation{
			InsertJobRunErrors{
				0: &JobRunError{ID: 0, RunID: runIds[0]},
			},
			InsertJobRunErrors{
				1: &JobRunError{ID: 1, RunID: runIds[1]},
			},
		}},
		"InsertRunAssignments": {Ops: []DbOperation{
			InsertRunAssignments{
				runIds[0]: &JobRunAssignment{RunID: runIds[0]},
			},
			InsertRunAssignments{
				runIds[1]: &JobRunAssignment{RunID: runIds[1]},
			},
		}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := withSetup(func(_ *Queries, db *pgxpool.Pool) error {
				serials := make(map[string]int64)
				for _, op := range tc.Ops {
					err := assertOpSuccess(t, db, serials, addDefaultValues(op))
					if err != nil {
						return err
					}
				}
				return nil
			})
			assert.NoError(t, err)
		})
	}
}

func addDefaultValues(op DbOperation) DbOperation {
	switch o := op.(type) {
	case InsertJobs:
		for _, job := range o {
			job.Groups = make([]string, 0)
			job.SubmitMessage = make([]byte, 0)
			job.SchedulingInfo = make([]byte, 0)
		}
	case InsertRuns:
	case InsertRunAssignments:
		for _, v := range o {
			v.Assignment = make([]byte, 0)
		}
	case UpdateJobSetPriorities:
	case MarkJobSetsCancelled:
	case MarkJobsCancelled:
	case MarkJobsSucceeded:
	case MarkJobsFailed:
	case UpdateJobPriorities:
	case MarkRunsSucceeded:
	case MarkRunsFailed:
	case MarkRunsRunning:
	case InsertJobErrors:
		for _, e := range o {
			e.Error = make([]byte, 0)
		}
	case InsertJobRunErrors:
		for _, e := range o {
			e.Error = make([]byte, 0)
		}
	}
	return op
}

func assertOpSuccess(t *testing.T, db *pgxpool.Pool, serials map[string]int64, op DbOperation) error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	// Apply the op to the database.
	err := WriteDbOp(ctx, db, op)
	if err != nil {
		return err
	}

	// Read back the state from the db to compare.
	queries := New(db)
	switch expected := op.(type) {
	case InsertJobs:
		jobs, err := queries.SelectNewJobs(ctx, serials["jobs"])
		if err != nil {
			return errors.WithStack(err)
		}
		actual := make(InsertJobs)
		for _, job := range jobs {
			job := job
			actual[job.JobID] = &job
			serials["jobs"] = max(serials["jobs"], job.Serial)
			if v, ok := expected[job.JobID]; ok {
				v.Serial = job.Serial
				v.LastModified = job.LastModified
			}
		}
		assert.Equal(t, expected, actual)
	case InsertRuns:
		jobs, err := queries.SelectNewJobs(ctx, 0)
		if err != nil {
			return errors.WithStack(err)
		}
		jobIds := make([]uuid.UUID, 0)
		for _, job := range jobs {
			jobIds = append(jobIds, job.JobID)
		}

		runs, err := queries.SelectNewRunsForJobs(ctx, SelectNewRunsForJobsParams{
			Serial: serials["runs"],
			JobIds: jobIds,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		actual := make(InsertRuns)
		for _, run := range runs {
			run := run
			actual[run.RunID] = &run
			serials["runs"] = max(serials["runs"], run.Serial)
			if v, ok := expected[run.RunID]; ok {
				v.Serial = run.Serial
				v.LastModified = run.LastModified
			}
		}
		assert.Equal(t, expected, actual)
	case InsertRunAssignments:
		as, err := queries.SelectNewRunAssignments(ctx, serials["assignments"])
		if err != nil {
			return errors.WithStack(err)
		}
		actual := make(InsertRunAssignments)
		for _, a := range as {
			a := a
			actual[a.RunID] = &a
			serials["assignments"] = max(serials["assignments"], a.Serial)
			if e, ok := expected[a.RunID]; ok {
				e.Serial = a.Serial
				e.LastModified = a.LastModified
			}
		}
		assert.Equal(t, expected, actual)
	case UpdateJobSetPriorities:
		jobs, err := queries.SelectNewJobs(ctx, serials["jobs"])
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		for _, job := range jobs {
			if e, ok := expected[job.JobSet]; ok {
				assert.Equal(t, e, job.Priority)
				numChanged++
			}
		}
		assert.Greater(t, numChanged, 0)
	case MarkJobSetsCancelled:
		jobs, err := queries.SelectNewJobs(ctx, serials["jobs"])
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		jobIds := make([]uuid.UUID, 0)
		for _, job := range jobs {
			if _, ok := expected[job.JobSet]; ok {
				assert.True(t, job.Cancelled)
				numChanged++
				jobIds = append(jobIds, job.JobID)
			}
		}
		assert.Greater(t, numChanged, 0)

		runs, err := queries.SelectNewRunsForJobs(ctx, SelectNewRunsForJobsParams{
			Serial: serials["runs"],
			JobIds: jobIds,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		assert.Greater(t, len(runs), 0)
		for _, run := range runs {
			assert.True(t, run.Cancelled)
		}
	case MarkJobsCancelled:
		jobs, err := queries.SelectNewJobs(ctx, serials["jobs"])
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		jobIds := make([]uuid.UUID, 0)
		for _, job := range jobs {
			if _, ok := expected[job.JobID]; ok {
				assert.True(t, job.Cancelled)
				numChanged++
				jobIds = append(jobIds, job.JobID)
			}
		}
		assert.Equal(t, len(expected), numChanged)

		runs, err := queries.SelectNewRunsForJobs(ctx, SelectNewRunsForJobsParams{
			Serial: serials["runs"],
			JobIds: jobIds,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		assert.Greater(t, len(runs), 0)
		for _, run := range runs {
			assert.True(t, run.Cancelled)
		}
	case MarkJobsSucceeded:
		jobs, err := queries.SelectNewJobs(ctx, serials["jobs"])
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		for _, job := range jobs {
			if _, ok := expected[job.JobID]; ok {
				assert.True(t, job.Succeeded)
				numChanged++
			}
		}
		assert.Equal(t, len(expected), numChanged)
	case MarkJobsFailed:
		jobs, err := queries.SelectNewJobs(ctx, serials["jobs"])
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		for _, job := range jobs {
			if _, ok := expected[job.JobID]; ok {
				assert.True(t, job.Failed)
				numChanged++
			}
		}
		assert.Equal(t, len(expected), numChanged)
	case UpdateJobPriorities:
		jobs, err := queries.SelectNewJobs(ctx, serials["jobs"])
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		for _, job := range jobs {
			if priority, ok := expected[job.JobID]; ok {
				assert.Equal(t, priority, job.Priority)
				numChanged++
			}
		}
		assert.Equal(t, len(expected), numChanged)
	case MarkRunsSucceeded:
		jobs, err := queries.SelectNewJobs(ctx, 0)
		if err != nil {
			return errors.WithStack(err)
		}
		jobIds := make([]uuid.UUID, 0)
		for _, job := range jobs {
			jobIds = append(jobIds, job.JobID)
		}

		runs, err := queries.SelectNewRunsForJobs(ctx, SelectNewRunsForJobsParams{
			Serial: serials["runs"],
			JobIds: jobIds,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		for _, run := range runs {
			if _, ok := expected[run.RunID]; ok {
				assert.True(t, run.Succeeded)
				numChanged++
			}
		}
		assert.Equal(t, len(expected), len(runs))
	case MarkRunsFailed:
		jobs, err := queries.SelectNewJobs(ctx, 0)
		if err != nil {
			return errors.WithStack(err)
		}
		jobIds := make([]uuid.UUID, 0)
		for _, job := range jobs {
			jobIds = append(jobIds, job.JobID)
		}

		runs, err := queries.SelectNewRunsForJobs(ctx, SelectNewRunsForJobsParams{
			Serial: serials["runs"],
			JobIds: jobIds,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		for _, run := range runs {
			if _, ok := expected[run.RunID]; ok {
				assert.True(t, run.Failed)
				numChanged++
			}
		}
		assert.Equal(t, len(expected), len(runs))
	case MarkRunsRunning:
		jobs, err := queries.SelectNewJobs(ctx, 0)
		if err != nil {
			return errors.WithStack(err)
		}
		jobIds := make([]uuid.UUID, 0)
		for _, job := range jobs {
			jobIds = append(jobIds, job.JobID)
		}

		runs, err := queries.SelectNewRunsForJobs(ctx, SelectNewRunsForJobsParams{
			Serial: serials["runs"],
			JobIds: jobIds,
		})
		if err != nil {
			return errors.WithStack(err)
		}
		numChanged := 0
		for _, run := range runs {
			if _, ok := expected[run.RunID]; ok {
				assert.True(t, run.Running)
				numChanged++
			}
		}
		assert.Equal(t, len(expected), len(runs))
	case InsertJobErrors:
		as, err := queries.SelectNewJobErrors(ctx, serials["job_errors"])
		if err != nil {
			return errors.WithStack(err)
		}
		actual := make(InsertJobErrors)
		for _, a := range as {
			a := a
			actual[a.ID] = &a
			serials["job_errors"] = max(serials["job_errors"], a.Serial)
			if v, ok := expected[a.ID]; ok {
				v.Serial = a.Serial
				v.LastModified = a.LastModified
			}
		}
		assert.Equal(t, expected, actual)
	case InsertJobRunErrors:
		as, err := queries.SelectNewRunErrors(ctx, serials["run_errors"])
		if err != nil {
			return errors.WithStack(err)
		}
		actual := make(InsertJobRunErrors)
		for _, a := range as {
			a := a
			actual[a.ID] = &a
			serials["run_errors"] = max(serials["run_errors"], a.Serial)
			if v, ok := expected[a.ID]; ok {
				v.Serial = a.Serial
				v.LastModified = a.LastModified
			}
		}
		assert.Equal(t, expected, actual)
	default:
		return errors.Errorf("received unexpected op %+v", op)
	}
	return nil
}

func max[E constraints.Ordered](a, b E) E {
	if a > b {
		return a
	}
	return b
}
