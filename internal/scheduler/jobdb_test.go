package scheduler

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/util"
)

func TestJobDbSchema(t *testing.T) {
	err := jobDbSchema().Validate()
	assert.NoError(t, err)
}

var job1 = &SchedulerJob{
	JobId:             uuid.NewString(),
	Queue:             "A",
	Priority:          0,
	Timestamp:         10,
	Node:              "",
	jobSchedulingInfo: nil,
}

var job2 = &SchedulerJob{
	JobId:             uuid.NewString(),
	Queue:             "A",
	Priority:          0,
	Timestamp:         10,
	Node:              "",
	jobSchedulingInfo: nil,
}

func TestBatchDelete(t *testing.T) {
	tests := map[string]struct {
		initialJobs          []*SchedulerJob
		idsToDelete          []string
		expectedRemainingIds []string
	}{
		"Delete all jobs": {
			initialJobs: []*SchedulerJob{job1, job2},
			idsToDelete: []string{job1.JobId, job2.JobId},
		},
		"Delete one job": {
			initialJobs:          []*SchedulerJob{job1, job2},
			idsToDelete:          []string{job1.JobId},
			expectedRemainingIds: []string{job2.JobId},
		},
		"Delete non-existent job": {
			initialJobs:          []*SchedulerJob{job1, job2},
			idsToDelete:          []string{"notaJobId", job1.JobId},
			expectedRemainingIds: []string{job2.JobId},
		},
		"delete nothing": {
			initialJobs:          []*SchedulerJob{job1, job2},
			idsToDelete:          []string{},
			expectedRemainingIds: []string{job1.JobId, job2.JobId},
		},
		"empty db": {
			idsToDelete:          []string{job1.JobId},
			expectedRemainingIds: []string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Set up JobDb
			jobDb := createPopulatedJobDb(t, tc.initialJobs)

			// delete
			txn := jobDb.WriteTxn()
			err := jobDb.BatchDelete(txn, tc.idsToDelete)
			require.NoError(t, err)

			// check that db is as we expect
			allJobs, err := jobDb.GetAll(txn)
			outstandingIds := stringSet(tc.expectedRemainingIds)
			require.NoError(t, err)
			for _, job := range allJobs {
				_, ok := outstandingIds[job.JobId]
				assert.True(t, ok)
				delete(outstandingIds, job.JobId)
			}
			assert.Equal(t, 0, len(outstandingIds))
		})
	}
}

func TestUpsert(t *testing.T) {
	tests := map[string]struct {
		initialJobs  []*SchedulerJob
		jobsToUpsert []*SchedulerJob
	}{
		"Insert new job": {
			initialJobs:  []*SchedulerJob{},
			jobsToUpsert: []*SchedulerJob{job1, job2},
		},
		"modify existing job": {
			initialJobs: []*SchedulerJob{},
			jobsToUpsert: []*SchedulerJob{{
				JobId:  job1.JobId,
				Queue:  "some queue",
				Jobset: "some jobset",
			}},
		},
		"insert nothing": {
			initialJobs:  []*SchedulerJob{},
			jobsToUpsert: []*SchedulerJob{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Set up JobDb
			jobDb := createPopulatedJobDb(t, tc.initialJobs)

			// upsert
			txn := jobDb.WriteTxn()
			err := jobDb.Upsert(txn, tc.jobsToUpsert)
			require.NoError(t, err)

			// check that all jobs were updated
			for _, job := range tc.jobsToUpsert {
				retrievedJob, err := jobDb.GetById(txn, job.JobId)
				require.NoError(t, err)
				assert.Equal(t, job, retrievedJob)
			}
		})
	}
}

func TestGetById(t *testing.T) {
	tests := map[string]struct {
		initialJobs   []*SchedulerJob
		jobToRetrieve string
		jobPresent    bool
	}{
		"Job Present": {
			initialJobs:   []*SchedulerJob{job1, job2},
			jobToRetrieve: job1.JobId,
			jobPresent:    true,
		},
		"Job Missing": {
			initialJobs:   []*SchedulerJob{job1, job2},
			jobToRetrieve: "notAJob",
			jobPresent:    false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Set up JobDb
			jobDb := createPopulatedJobDb(t, tc.initialJobs)

			// upsert
			txn := jobDb.ReadTxn()
			job, err := jobDb.GetById(txn, tc.jobToRetrieve)
			require.NoError(t, err)

			if tc.jobPresent {
				assert.NotNil(t, job)
				assert.Equal(t, tc.jobToRetrieve, job.JobId)
			} else {
				assert.Nil(t, job)
			}
		})
	}
}

func TestGetAll(t *testing.T) {
	jobs := []*SchedulerJob{job1, job2}
	jobDb := createPopulatedJobDb(t, jobs)

	txn := jobDb.ReadTxn()
	retrievedJobs, err := jobDb.GetAll(txn)
	require.NoError(t, err)

	slices.SortFunc(jobs, func(a *SchedulerJob, b *SchedulerJob) bool { return a.JobId > b.JobId })
	slices.SortFunc(retrievedJobs, func(a *SchedulerJob, b *SchedulerJob) bool { return a.JobId > b.JobId })
	assert.Equal(t, jobs, retrievedJobs)
}

func TestJobQueuePriorityClassIterator(t *testing.T) {
	// jobs in the db at the start of the test
	initialJobs := []*SchedulerJob{
		// Jobs on queue A
		{
			JobId:     util.NewULID(),
			Queue:     "A",
			Priority:  0,
			Timestamp: 0,
			Queued:    false,
		},
		{
			JobId:     util.NewULID(),
			Queue:     "A",
			Priority:  0,
			Timestamp: 0,
			Queued:    true,
		},
		{
			JobId:     util.NewULID(),
			Queue:     "A",
			Priority:  0,
			Timestamp: 1,
			Queued:    true,
		},
		{
			JobId:     util.NewULID(),
			Queue:     "A",
			Priority:  1,
			Timestamp: 0,
			Queued:    true,
		},

		// Jobs on Queue B
		{
			JobId:     util.NewULID(),
			Queue:     "B",
			Priority:  0,
			Timestamp: 0,
			Queued:    true,
		},
	}

	tests := map[string]struct {
		Queue              string
		ExpectedJobIndexes []int
	}{
		"Queue A": {
			Queue:              "A",
			ExpectedJobIndexes: []int{1, 2, 3},
		},
		"Queue B Queued": {
			Queue:              "B",
			ExpectedJobIndexes: []int{4},
		},
		"Unknown Queue": {
			Queue:              "C",
			ExpectedJobIndexes: []int{},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Shuffle and insert jobs.
			jobs := slices.Clone(initialJobs)
			slices.SortFunc(jobs, func(a, b *SchedulerJob) bool { return rand.Float64() < 0.5 })
			jobDb := createPopulatedJobDb(t, jobs)

			// Test that jobs are returned in expected order.
			txn := jobDb.ReadTxn()
			it, err := NewJobQueueIterator(txn, tc.Queue)
			if !assert.NoError(t, err) {
				return
			}
			for _, i := range tc.ExpectedJobIndexes {
				item := it.NextJobItem()
				if !assert.Equal(t, initialJobs[i], item) {
					return
				}
			}
			assert.Nil(t, it.NextJobItem())
		})
	}
}

func createPopulatedJobDb(t *testing.T, initialJobs []*SchedulerJob) *JobDb {
	jobDb, err := NewJobDb()
	require.NoError(t, err)
	txn := jobDb.WriteTxn()
	err = jobDb.Upsert(txn, initialJobs)
	require.NoError(t, err)
	txn.Commit()
	return jobDb
}
