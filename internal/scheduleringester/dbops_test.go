package scheduleringester

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/util"
	schedulerdb "github.com/armadaproject/armada/internal/scheduler/database"
)

func TestMerge(t *testing.T) {
	jobId1 := util.NewULID()
	jobId2 := util.NewULID()
	jobId3 := util.NewULID()
	markJobsCancelled1 := MarkJobsCancelRequested{jobId1: false, jobId2: false}
	markJobsCancelled2 := MarkJobsCancelRequested{jobId2: true, jobId3: true}
	ok := markJobsCancelled1.Merge(markJobsCancelled2)
	assert.True(t, ok)
	assert.Equal(t, MarkJobsCancelRequested{jobId1: false, jobId2: true, jobId3: true}, markJobsCancelled1)

	jobId4 := util.NewULID()
	markJobsSucceeded1 := MarkJobsSucceeded{jobId1: true, jobId4: true}
	ok = markJobsCancelled1.Merge(markJobsSucceeded1)
	assert.False(t, ok)
	assert.Equal(t, MarkJobsCancelRequested{jobId1: false, jobId2: true, jobId3: true}, markJobsCancelled1)
}

// Test that db op optimisation
// 1. produces the expected number of ops after optimisations and
// 2. results in the same end state as if no optimisation had been applied.
func TestDbOperationOptimisation(t *testing.T) {
	jobIds := make([]string, 10)
	for i := range jobIds {
		jobIds[i] = util.NewULID()
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
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1], JobSet: "set2"}}, // 1
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[3]: &schedulerdb.Job{JobID: jobIds[3], JobSet: "set2"}}, // 1
		}},
		"InsertJobs, InsertRuns": {N: 2, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &schedulerdb.Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1]}},                   // 2
			InsertRuns{runIds[1]: &schedulerdb.Run{JobID: jobIds[0], RunID: runIds[1]}}, // 2
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2]}},                   // 2
			InsertRuns{runIds[2]: &schedulerdb.Run{JobID: jobIds[2], RunID: runIds[2]}}, // 2
		}},
		"UpdateJobSetPriorities": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			UpdateJobSetPriorities{"set1": 1},                                         // 2
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1], JobSet: "set1"}}, // 3
			UpdateJobSetPriorities{"set2": 2},                                         // 3
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2], JobSet: "set1"}}, // 3
		}},
		"UpdateJobSetPriorities, UpdateJobPriorities": {N: 4, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1], JobSet: "set1"}}, // 1
			UpdateJobPriorities{jobIds[0]: 1},                                         // 2
			UpdateJobSetPriorities{"set1": 2},                                         // 3
			UpdateJobPriorities{jobIds[1]: 3},                                         // 4
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2], JobSet: "set2"}}, // 4
			UpdateJobPriorities{jobIds[1]: 4},                                         // 4
			UpdateJobPriorities{jobIds[2]: 5},                                         // 4
		}},
		"MarkJobSetsCancelRequested": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			MarkJobSetsCancelRequested{"set1": true},                                  // 2
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1], JobSet: "set1"}}, // 3
			MarkJobSetsCancelRequested{"set2": true},                                  // 3
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2], JobSet: "set1"}}, // 3
		}},
		"MarkJobSetsCancelRequested, MarkJobsCancelRequested": {N: 4, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0], JobSet: "set1"}}, // 1
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1], JobSet: "set1"}}, // 1
			MarkJobsCancelRequested{jobIds[0]: true},                                  // 2
			MarkJobSetsCancelRequested{"set1": true},                                  // 3
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2], JobSet: "set1"}}, // 4
			MarkJobsCancelRequested{jobIds[1]: true},                                  // 4
			MarkJobsCancelRequested{jobIds[2]: true},                                  // 4
		}},
		"MarkJobsSucceeded": {N: 2, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0]}}, // 1
			MarkJobsSucceeded{jobIds[0]: true},                        // 2
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1]}}, // 2
			MarkJobsSucceeded{jobIds[1]: true},                        // 2
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2]}}, // 2
		}},
		"MarkJobsFailed": {N: 2, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0]}}, // 1
			MarkJobsFailed{jobIds[0]: true},                           // 2
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1]}}, // 2
			MarkJobsFailed{jobIds[1]: true},                           // 2
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2]}}, // 2
		}},
		"MarkJobsCancelled": {N: 2, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0]}}, // 1
			MarkJobsCancelled{jobIds[0]: true},                        // 2
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1]}}, // 2
			MarkJobsCancelled{jobIds[1]: true},                        // 2
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2]}}, // 2
		}},
		"MarkRunsSucceeded": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &schedulerdb.Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			MarkRunsSucceeded{runIds[0]: true},                                          // 3
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1]}},                   // 3
			InsertRuns{runIds[1]: &schedulerdb.Run{JobID: jobIds[1], RunID: runIds[1]}}, // 3
			MarkRunsSucceeded{runIds[1]: true},                                          // 3
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2]}},                   // 3
		}},
		"MarkRunsFailed": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &schedulerdb.Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			MarkRunsFailed{runIds[0]: &JobRunFailed{true}},                              // 3
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1]}},                   // 3
			InsertRuns{runIds[1]: &schedulerdb.Run{JobID: jobIds[1], RunID: runIds[1]}}, // 3
			MarkRunsFailed{runIds[1]: &JobRunFailed{true}},                              // 3
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2]}},                   // 3
		}},
		"MarkRunsRunning": {N: 3, Ops: []DbOperation{
			InsertJobs{jobIds[0]: &schedulerdb.Job{JobID: jobIds[0]}},                   // 1
			InsertRuns{runIds[0]: &schedulerdb.Run{JobID: jobIds[0], RunID: runIds[0]}}, // 2
			MarkRunsRunning{runIds[0]: true},                                            // 3
			InsertJobs{jobIds[1]: &schedulerdb.Job{JobID: jobIds[1]}},                   // 3
			InsertRuns{runIds[1]: &schedulerdb.Run{JobID: jobIds[1], RunID: runIds[1]}}, // 3
			MarkRunsRunning{runIds[1]: true},                                            // 3
			InsertJobs{jobIds[2]: &schedulerdb.Job{JobID: jobIds[2]}},                   // 3
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

func TestInsertJobRequestCancel(t *testing.T) {
	// Submit jobs to two different job sets.
	var ops []DbOperation
	expectedCancelledIds := make(map[string]bool)
	for i := 0; i < 2; i++ {
		job := &schedulerdb.Job{JobID: util.NewULID(), JobSet: "set1"}
		expectedCancelledIds[job.JobID] = true
		ops = append(ops, InsertJobs{job.JobID: job})
	}
	for i := 0; i < 2; i++ {
		job := &schedulerdb.Job{JobID: util.NewULID(), JobSet: "set2"}
		ops = append(ops, InsertJobs{job.JobID: job})
	}

	// Cancel one job set.
	ops = append(ops, MarkJobSetsCancelRequested{"set1": true})

	// Submit some more jobs to both job sets.
	for i := 0; i < 2; i++ {
		job := &schedulerdb.Job{JobID: util.NewULID(), JobSet: "set2"}
		ops = append(ops, InsertJobs{job.JobID: job})
	}
	for i := 0; i < 2; i++ {
		job := &schedulerdb.Job{JobID: util.NewULID(), JobSet: "set1"}
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
		if job.CancelRequested {
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
	Jobs map[string]*schedulerdb.Job
	Runs map[uuid.UUID]*schedulerdb.Run
}

func newMockDb() *mockDb {
	return &mockDb{
		Jobs: make(map[string]*schedulerdb.Job),
		Runs: make(map[uuid.UUID]*schedulerdb.Run),
	}
}

func assertDbEquals(t *testing.T, expected, actual *mockDb) {
	assert.Equal(t, expected.Jobs, actual.Jobs)
	assert.Equal(t, expected.Runs, actual.Runs)
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
	case UpdateJobSetPriorities:
		for jobSet, priority := range o {
			for _, job := range db.Jobs {
				if job.JobSet == jobSet {
					job.Priority = priority
				}
			}
		}
	case MarkJobSetsCancelRequested:
		for jobSet := range o {
			for _, job := range db.Jobs {
				if job.JobSet == jobSet {
					job.CancelRequested = true
				}
			}
		}
	case MarkJobsCancelRequested:
		for jobId := range o {
			if job, ok := db.Jobs[jobId]; ok {
				job.CancelRequested = true
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
	}
	return nil
}
