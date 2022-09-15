package scheduler

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
)

func TestMerge(t *testing.T) {
	jobId1 := uuid.New()
	jobId2 := uuid.New()
	jobId3 := uuid.New()
	markJobsCancelled1 := MarkJobsCancelled{jobId1: false, jobId2: false}
	markJobsCancelled2 := MarkJobsCancelled{jobId2: true, jobId3: true}
	ok := markJobsCancelled1.Merge(markJobsCancelled2)
	assert.True(t, ok)
	assert.Equal(t, MarkJobsCancelled{jobId1: false, jobId2: true, jobId3: true}, markJobsCancelled1)

	jobId4 := uuid.New()
	markJobsSucceeded1 := MarkJobsSucceeded{jobId1: true, jobId4: true}
	ok = markJobsCancelled1.Merge(markJobsSucceeded1)
	assert.False(t, ok)
	assert.Equal(t, MarkJobsCancelled{jobId1: false, jobId2: true, jobId3: true}, markJobsCancelled1)
}

// Test that db op optimisation
// 1. produces the expected number of ops after optimisations and
// 2. results in the same end state as if no optimisation had been applied.
func TestDbOperationOptimisation(t *testing.T) {
	jobIds := make([]uuid.UUID, 10)
	for i := range jobIds {
		jobIds[i] = uuid.New()
	}
	runIds := make([]uuid.UUID, 10)
	for i := range runIds {
		runIds[i] = uuid.New()
	}
	tests := map[string]struct {
		N   int           // Expected number of ops after optimisation.
		Ops []DbOperation // Ops sequence to optimise.
	}{
		"InsertJobs": {N: 1, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set2"}}, // 1
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[3]: &Job{JobID: jobIds[3], JobSet: "set2"}}, // 1
		}},
		"InsertJobs, InsertRuns": {N: 2, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1]}},                   // 2
			InsertRuns{runIds[1]: &Run{JobID: jobIds[0], RunID: runIds[1]}}, // 2
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2]}},                   // 2
			InsertRuns{runIds[2]: &Run{JobID: jobIds[2], RunID: runIds[2]}}, // 2
		}},
		"UpdateJobSetPriorities": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			UpdateJobSetPriorities{"set1": 1},                             // 2
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set1"}}, // 3
			UpdateJobSetPriorities{"set2": 2},                             // 3
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"}}, // 3
		}},
		"UpdateJobSetPriorities, UpdateJobPriorities": {N: 4, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set1"}}, // 1
			UpdateJobPriorities{jobIds[0]: 1},                             // 2
			UpdateJobSetPriorities{"set1": 2},                             // 3
			UpdateJobPriorities{jobIds[1]: 3},                             // 4
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set2"}}, // 4
			UpdateJobPriorities{jobIds[1]: 4},                             // 4
			UpdateJobPriorities{jobIds[2]: 5},                             // 4
		}},
		"MarkJobSetsCancelled": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			MarkJobSetsCancelled{"set1": true},                            // 2
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set1"}}, // 3
			MarkJobSetsCancelled{"set2": true},                            // 3
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"}}, // 3
		}},
		"MarkJobSetsCancelled, MarkJobsCancelled": {N: 4, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1], JobSet: "set1"}}, // 1
			MarkJobsCancelled{jobIds[0]: true},                            // 2
			MarkJobSetsCancelled{"set1": true},                            // 3
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2], JobSet: "set1"}}, // 4
			MarkJobsCancelled{jobIds[1]: true},                            // 4
			MarkJobsCancelled{jobIds[2]: true},                            // 4
		}},
		"MarkJobsSucceeded": {N: 2, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0]}}, // 1
			MarkJobsSucceeded{jobIds[0]: true},            // 2
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1]}}, // 2
			MarkJobsSucceeded{jobIds[1]: true},            // 2
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2]}}, // 2
		}},
		"MarkJobsFailed": {N: 2, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0]}}, // 1
			MarkJobsFailed{jobIds[0]: true},               // 2
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1]}}, // 2
			MarkJobsFailed{jobIds[1]: true},               // 2
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2]}}, // 2
		}},
		"MarkRunsSucceeded": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			MarkRunsSucceeded{runIds[0]: true},                              // 3
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1]}},                   // 3
			InsertRuns{runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]}}, // 3
			MarkRunsSucceeded{runIds[1]: true},                              // 3
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2]}},                   // 3
		}},
		"MarkRunsFailed": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			MarkRunsFailed{runIds[0]: true},                                 // 3
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1]}},                   // 3
			InsertRuns{runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]}}, // 3
			MarkRunsFailed{runIds[1]: true},                                 // 3
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2]}},                   // 3
		}},
		"MarkRunsRunning": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			MarkRunsRunning{runIds[0]: true},                                // 3
			InsertJobs{jobIds[1]: &Job{JobID: jobIds[1]}},                   // 3
			InsertRuns{runIds[1]: &Run{JobID: jobIds[1], RunID: runIds[1]}}, // 3
			MarkRunsRunning{runIds[1]: true},                                // 3
			InsertJobs{jobIds[2]: &Job{JobID: jobIds[2]}},                   // 3
		}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Compute expected end state.
			expected := newMockDb()
			err := expected.applySeveral(tc.Ops)
			if !assert.NoError(t, err) {
				return
			}

			// Re-order and merge ops.
			// Mutates the original ops defined in the test case.
			var optimised []DbOperation
			for _, op := range tc.Ops {
				optimised = AppendDbOperation(optimised, op)
			}

			// Print the optimised ops to help debug.
			fmt.Printf("[%s] optimised ops:\n", name)
			for i, op := range optimised {
				fmt.Printf("%d:\t%T\n", i, op)
			}

			// Compute end state of optimised sequence.
			actual := newMockDb()
			err = actual.applySeveral(optimised)
			if !assert.NoError(t, err) {
				return
			}

			assert.Equal(t, tc.N, len(optimised))
			assertDbEquals(t, expected, actual)
		})
	}
}

func TestInsertJobCancel(t *testing.T) {
	// Submit jobs to two different job sets.
	var ops []DbOperation
	expectedCancelledIds := make(map[uuid.UUID]bool)
	for i := 0; i < 2; i++ {
		job := &Job{JobID: uuid.New(), JobSet: "set1"}
		expectedCancelledIds[job.JobID] = true
		ops = append(ops, InsertJobs{job.JobID: job})
	}
	for i := 0; i < 2; i++ {
		job := &Job{JobID: uuid.New(), JobSet: "set2"}
		ops = append(ops, InsertJobs{job.JobID: job})
	}

	// Cancel one job set.
	ops = append(ops, MarkJobSetsCancelled{"set1": true})

	// Submit some more jobs to both job sets.
	for i := 0; i < 2; i++ {
		job := &Job{JobID: uuid.New(), JobSet: "set2"}
		ops = append(ops, InsertJobs{job.JobID: job})
	}
	for i := 0; i < 2; i++ {
		job := &Job{JobID: uuid.New(), JobSet: "set1"}
		ops = append(ops, InsertJobs{job.JobID: job})
	}

	// Apply ops to a database.
	expectedDb := newMockDb()
	err := expectedDb.applySeveral(ops)
	if !assert.NoError(t, err) {
		return
	}

	// Check that the mockDb did the right thing.
	for jobId, job := range expectedDb.Jobs {
		_, ok := expectedCancelledIds[jobId]
		if job.Cancelled {
			assert.True(t, ok)
		} else {
			assert.False(t, ok)
		}
	}

	// Produce a set of optimised ops.
	// Note that this mutates the previously defined ops.
	var optimisedOps []DbOperation
	for _, op := range ops {
		optimisedOps = AppendDbOperation(optimisedOps, op)
	}

	// Apply the optimised ops to a fresh db and check that the end result is the same.
	actualDb := newMockDb()
	err = actualDb.applySeveral(optimisedOps)
	if !assert.NoError(t, err) {
		return
	}
	assertDbEquals(t, expectedDb, actualDb)
}

type mockDb struct {
	Jobs           map[uuid.UUID]*Job
	Runs           map[uuid.UUID]*Run
	RunAssignments map[uuid.UUID]*JobRunAssignment
	JobErrors      map[int32]*JobError
	JobRunErrors   map[int32]*JobRunError
}

func newMockDb() *mockDb {
	return &mockDb{
		Jobs:           make(map[uuid.UUID]*Job),
		Runs:           make(map[uuid.UUID]*Run),
		RunAssignments: make(map[uuid.UUID]*JobRunAssignment),
		JobErrors:      make(map[int32]*JobError),
		JobRunErrors:   make(map[int32]*JobRunError),
	}
}

func assertDbEquals(t *testing.T, expected, actual *mockDb) {
	assert.Equal(t, expected.Jobs, actual.Jobs)
	assert.Equal(t, expected.Runs, actual.Runs)
	assert.Equal(t, expected.RunAssignments, actual.RunAssignments)
	assert.Equal(t, expected.JobErrors, actual.JobErrors)
	assert.Equal(t, expected.JobRunErrors, actual.JobRunErrors)
}

func (db *mockDb) applySeveral(ops []DbOperation) error {
	for _, op := range ops {
		err := db.apply(op)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *mockDb) apply(op DbOperation) error {
	switch o := op.(type) {
	case InsertJobs:
		n := len(db.Jobs)
		for _, job := range o {
			job := *job // Copy primitive types
			db.Jobs[job.JobID] = &job
		}
		if len(db.Jobs) != n+len(o) {
			return errors.New("duplicate job id")
		}
	case InsertRuns:
		n := len(db.Runs)
		for _, run := range o {
			run := *run // Copy primitive types
			db.Runs[run.RunID] = &run
		}
		if len(db.Runs) != n+len(o) {
			return errors.New("duplicate run id")
		}
	case InsertRunAssignments:
		n := len(db.RunAssignments)
		maps.Copy(db.RunAssignments, o)
		if len(db.RunAssignments) != n+len(o) {
			return errors.New("duplicate run id (assignment)")
		}
	case UpdateJobSetPriorities:
		for jobSet, priority := range o {
			for _, job := range db.Jobs {
				if job.JobSet == jobSet {
					job.Priority = priority
				}
			}
		}
	case MarkJobSetsCancelled:
		for jobSet := range o {
			for _, job := range db.Jobs {
				if job.JobSet == jobSet {
					job.Cancelled = true
				}
			}
		}
	case MarkJobsCancelled:
		for jobId := range o {
			if job, ok := db.Jobs[jobId]; ok {
				job.Cancelled = true
			} else {
				return errors.Errorf("job %s not in db", jobId)
			}
		}
	case MarkJobsSucceeded:
		for jobId := range o {
			if job, ok := db.Jobs[jobId]; ok {
				job.Succeeded = true
			} else {
				return errors.Errorf("job %s not in db", jobId)
			}
		}
	case MarkJobsFailed:
		for jobId := range o {
			if job, ok := db.Jobs[jobId]; ok {
				job.Failed = true
			} else {
				return errors.Errorf("job %s not in db", jobId)
			}
		}
	case UpdateJobPriorities:
		for jobId, priority := range o {
			if job, ok := db.Jobs[jobId]; ok {
				job.Priority = priority
			} else {
				return errors.Errorf("job %s not in db", jobId)
			}
		}
	case MarkRunsSucceeded:
		for runId := range o {
			if run, ok := db.Runs[runId]; ok {
				run.Succeeded = true
			} else {
				return errors.Errorf("run %s not in db", runId)
			}
		}
	case MarkRunsFailed:
		for runId := range o {
			if run, ok := db.Runs[runId]; ok {
				run.Succeeded = true
			} else {
				return errors.Errorf("run %s not in db", runId)
			}
		}
	case MarkRunsRunning:
		for runId := range o {
			if run, ok := db.Runs[runId]; ok {
				run.Running = true
			} else {
				return errors.Errorf("run %s not in db", runId)
			}
		}
	case InsertJobErrors:
		n := len(db.JobErrors)
		maps.Copy(db.JobErrors, o)
		if len(db.JobErrors) != n+len(o) {
			return errors.New("duplicate job error")
		}
	case InsertJobRunErrors:
		n := len(db.JobRunErrors)
		maps.Copy(db.JobRunErrors, o)
		if len(db.JobRunErrors) != n+len(o) {
			return errors.New("duplicate job run error")
		}
	}
	return nil
}
