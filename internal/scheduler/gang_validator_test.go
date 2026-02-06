package scheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestGangValidator_Validate(t *testing.T) {
	nonGangJob1 := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)
	nonGangJob2 := testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass1)

	tests := map[string]struct {
		existingJobs              []*jobdb.Job
		jobsToValidate            []*jobdb.Job
		expectedInvalidJobIndexes []int
	}{
		"valid gang submission - single submission": {
			jobsToValidate: createNQueuedGangJob(2, "id-1", 2),
		},
		"valid gang submission - partial submission": {
			jobsToValidate: createNQueuedGangJob(1, "id-1", 2),
		},
		"valid gang submission - multiple submission": {
			existingJobs:   createNQueuedGangJob(1, "id-1", 2),
			jobsToValidate: createNQueuedGangJob(1, "id-1", 2),
		},
		"valid non-gang jobs": {
			jobsToValidate: []*jobdb.Job{nonGangJob1, nonGangJob2},
		},
		"invalid gang submission - single submission - exceeds cardinality": {
			jobsToValidate:            createNQueuedGangJob(3, "id-1", 2),
			expectedInvalidJobIndexes: []int{0, 1, 2},
		},
		"invalid gang submission - single submission - inconsistent gang info": {
			jobsToValidate:            armadaslices.Concatenate(createNQueuedGangJob(1, "id-1", 2), createNQueuedGangJob(1, "id-1", 3)),
			expectedInvalidJobIndexes: []int{0, 1},
		},
		"invalid gang submission - multiple submission - exceeds cardinality": {
			existingJobs:              createNQueuedGangJob(2, "id-1", 2),
			jobsToValidate:            createNQueuedGangJob(1, "id-1", 2),
			expectedInvalidJobIndexes: []int{0},
		},
		"invalid gang submission - multiple submission - inconsistent gang info": {
			existingJobs:              createNQueuedGangJob(1, "id-1", 2),
			jobsToValidate:            createNQueuedGangJob(1, "id-1", 3),
			expectedInvalidJobIndexes: []int{0},
		},
		"invalid gang submission - multiple submission - adding to running gang": {
			existingJobs:              createNRunningGangJobs(1, "id-1", 2),
			jobsToValidate:            createNQueuedGangJob(1, "id-1", 2),
			expectedInvalidJobIndexes: []int{0},
		},
		"invalid gang submission - single submission - inconsistent priority class": {
			jobsToValidate: armadaslices.Concatenate(
				createNQueuedGangJobWithPriorityClass(1, "id-1", 2, testfixtures.PriorityClass1),
				createNQueuedGangJobWithPriorityClass(1, "id-1", 2, testfixtures.PriorityClass2),
			),
			expectedInvalidJobIndexes: []int{0, 1},
		},
		"invalid gang submission - multiple submission - inconsistent priority class": {
			existingJobs:              createNQueuedGangJobWithPriorityClass(1, "id-1", 2, testfixtures.PriorityClass1),
			jobsToValidate:            createNQueuedGangJobWithPriorityClass(1, "id-1", 2, testfixtures.PriorityClass2),
			expectedInvalidJobIndexes: []int{0},
		},
		"mixed valid and invalid - single submission": {
			jobsToValidate: armadaslices.Concatenate(
				createNQueuedGangJob(2, "id-1", 2),
				createNQueuedGangJob(3, "id-2", 2),
				createNQueuedGangJob(3, "id-3", 2),
				[]*jobdb.Job{nonGangJob1, nonGangJob2},
			),
			expectedInvalidJobIndexes: []int{2, 3, 4, 5, 6, 7},
		},
		"mixed valid and invalid - multiple submission": {
			existingJobs: armadaslices.Concatenate(
				createNRunningGangJobs(1, "id-1", 2),
				createNQueuedGangJob(2, "id-2", 2),
			),
			jobsToValidate: armadaslices.Concatenate(
				createNQueuedGangJob(1, "id-1", 2),
				createNQueuedGangJob(1, "id-2", 2),
				createNQueuedGangJob(2, "id-3", 2),
				[]*jobdb.Job{nonGangJob1, nonGangJob2},
			),
			expectedInvalidJobIndexes: []int{0, 1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gangValidator := NewGangValidator()

			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()
			err := txn.Upsert(append(tc.existingJobs, tc.jobsToValidate...))
			assert.NoError(t, err)
			txn.Commit()
			txn = jobDb.ReadTxn()

			result, err := gangValidator.Validate(txn, tc.jobsToValidate)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.expectedInvalidJobIndexes), len(result))

			invalidJobIds := armadaslices.Map(result, func(r *invalidGangJobDetails) string {
				return r.jobId
			})

			for _, index := range tc.expectedInvalidJobIndexes {
				job := tc.jobsToValidate[index]
				assert.Contains(t, invalidJobIds, job.Id())
			}
		})
	}
}

func TestGangValidator_Validate_ErrorsOnJobsMissingFromTxn(t *testing.T) {
	gangValidator := NewGangValidator()
	jobsToValidate := createNQueuedGangJob(2, "id-1", 2)

	jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
	// Don't add jobs to txn
	txn := jobDb.ReadTxn()

	result, err := gangValidator.Validate(txn, jobsToValidate)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func createGangJobs(count int, gangId string, cardinality int, queued bool, priorityClass string) []*jobdb.Job {
	jobs := make([]*jobdb.Job, 0, count)
	for i := 0; i < count; i++ {
		job := testfixtures.Test1Cpu4GiJob("queue", priorityClass).WithQueued(queued)
		jobs = append(jobs, job)
	}
	return testfixtures.WithGangJobDetails(jobs, gangId, cardinality, "node-uniformity")
}

func createNQueuedGangJob(count int, gangId string, cardinality int) []*jobdb.Job {
	return createGangJobs(count, gangId, cardinality, true, testfixtures.PriorityClass1)
}

func createNRunningGangJobs(count int, gangId string, cardinality int) []*jobdb.Job {
	return createGangJobs(count, gangId, cardinality, false, testfixtures.PriorityClass1)
}

func createNQueuedGangJobWithPriorityClass(count int, gangId string, cardinality int, priorityClass string) []*jobdb.Job {
	return createGangJobs(count, gangId, cardinality, true, priorityClass)
}
