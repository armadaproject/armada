package jobdb

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
	id:                uuid.NewString(),
	queue:             "A",
	priority:          0,
	timestamp:         10,
	jobSchedulingInfo: nil,
}

var job2 = &SchedulerJob{
	id:                uuid.NewString(),
	queue:             "A",
	priority:          0,
	timestamp:         10,
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
			idsToDelete: []string{job1.id, job2.id},
		},
		"Delete one job": {
			initialJobs:          []*SchedulerJob{job1, job2},
			idsToDelete:          []string{job1.id},
			expectedRemainingIds: []string{job2.id},
		},
		"Delete non-existent job": {
			initialJobs:          []*SchedulerJob{job1, job2},
			idsToDelete:          []string{"notaJobId", job1.id},
			expectedRemainingIds: []string{job2.id},
		},
		"delete nothing": {
			initialJobs:          []*SchedulerJob{job1, job2},
			idsToDelete:          []string{},
			expectedRemainingIds: []string{job1.id, job2.id},
		},
		"empty db": {
			idsToDelete:          []string{job1.id},
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
				_, ok := outstandingIds[job.id]
				assert.True(t, ok)
				delete(outstandingIds, job.id)
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
				id:     job1.id,
				queue:  "some queue",
				jobset: "some jobset",
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
				retrievedJob, err := jobDb.GetById(txn, job.id)
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
			jobToRetrieve: job1.id,
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
				assert.Equal(t, tc.jobToRetrieve, job.id)
			} else {
				assert.Nil(t, job)
			}
		})
	}
}

func TestLookupByRun(t *testing.T) {
	job := &SchedulerJob{
		id:        uuid.NewString(),
		queue:     "A",
		priority:  0,
		timestamp: 10,
	}

	run := &JobRun{
		id: uuid.New(),
	}

	// set up the job db with a single job
	jobDb, err := NewJobDb()
	require.NoError(t, err)
	txn := jobDb.WriteTxn()
	err = jobDb.Upsert(txn, []*SchedulerJob{job})
	require.NoError(t, err)

	// try to lookup the job by run id- this should be nil as no run exists yet
	retrievedJob, err := jobDb.GetByRunId(txn, run.id)
	require.NoError(t, err)
	assert.Nil(t, retrievedJob)

	// update the job to have a run
	updatedJob := job.DeepCopy()
	updatedJob.runs = append(updatedJob.runs, run)
	err = jobDb.Upsert(txn, []*SchedulerJob{updatedJob})
	require.NoError(t, err)

	// try to lookup the job by run id- this should now return the job
	retrievedJob, err = jobDb.GetByRunId(txn, run.id)
	require.NoError(t, err)
	assert.Equal(t, updatedJob, retrievedJob)

	// Delete the job
	err = jobDb.BatchDelete(txn, []string{job.id})
	require.NoError(t, err)

	// try to lookup the job by run id- this should  be nil as the job has been deleted
	retrievedJob, err = jobDb.GetByRunId(txn, run.id)
	require.NoError(t, err)
	assert.Nil(t, retrievedJob)
}

func TestGetAll(t *testing.T) {
	jobs := []*SchedulerJob{job1, job2}
	jobDb := createPopulatedJobDb(t, jobs)

	txn := jobDb.ReadTxn()
	retrievedJobs, err := jobDb.GetAll(txn)
	require.NoError(t, err)

	slices.SortFunc(jobs, func(a *SchedulerJob, b *SchedulerJob) bool { return a.id > b.id })
	slices.SortFunc(retrievedJobs, func(a *SchedulerJob, b *SchedulerJob) bool { return a.id > b.id })
	assert.Equal(t, jobs, retrievedJobs)
}

func TestJobQueuePriorityClassIterator(t *testing.T) {
	// jobs in the db at the start of the test
	initialJobs := []*SchedulerJob{
		// Jobs on queue A
		{
			id:        util.NewULID(),
			queue:     "A",
			priority:  0,
			timestamp: 0,
			Queued:    false,
		},
		{
			id:        util.NewULID(),
			queue:     "A",
			priority:  0,
			timestamp: 0,
			Queued:    true,
		},
		{
			id:        util.NewULID(),
			queue:     "A",
			priority:  0,
			timestamp: 1,
			Queued:    true,
		},
		{
			id:        util.NewULID(),
			queue:     "A",
			priority:  1,
			timestamp: 0,
			Queued:    true,
		},

		// Jobs on Queue B
		{
			id:        util.NewULID(),
			queue:     "B",
			priority:  0,
			timestamp: 0,
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

func stringSet(src []string) map[string]bool {
	set := make(map[string]bool, len(src))
	for _, s := range src {
		set[s] = true
	}
	return set
}
