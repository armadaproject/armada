package repository

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api/lookout"
)

func TestGetJobSetInfos_GetNoJobSetsIfQueueDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Empty(t, jobSetInfos)
	})
}

func TestGetJobSetInfos_GetsJobSetWithNoFinishedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set").
			Pending(cluster, k8sId1)

		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set",
			JobsQueued:    1,
			JobsPending:   1,
			JobsRunning:   0,
			JobsSucceeded: 0,
			JobsFailed:    0,
		}, jobSetInfos[0])
	})
}

func TestGetJobSetInfos_GetsJobSetWithOnlyFinishedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set").
			Running(cluster, k8sId1, node).
			Succeeded(cluster, k8sId1, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set").
			Pending(cluster, k8sId2).
			Failed(cluster, k8sId2, node, "some error")

		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set",
			JobsQueued:    0,
			JobsPending:   0,
			JobsRunning:   0,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[0])
	})
}

func TestGetJobSetInfos_JobSetsCounts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "a1").
			UnableToSchedule(cluster, "a1", node).
			Pending(cluster, "a2")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "b1").
			UnableToSchedule(cluster, "b1", node).
			Running(cluster, "b2", node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "c1").
			UnableToSchedule(cluster, "c1", node).
			Running(cluster, "c2", node).
			Succeeded(cluster, "c2", node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "d1").
			UnableToSchedule(cluster, "d1", node).
			Running(cluster, "d2", node).
			Failed(cluster, "d2", node, "something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "e1").
			UnableToSchedule(cluster, "e1", node).
			Running(cluster, "e2", node).
			Cancelled()

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobSetInfos))
		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set-1",
			JobsQueued:    1,
			JobsPending:   1,
			JobsRunning:   1,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[0])
	})
}

func TestGetJobSetInfos_MultipleJobSetsCounts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		// Job set 1
		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "a1").
			UnableToSchedule(cluster, "a1", node).
			Pending(cluster, "a2")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "b1").
			UnableToSchedule(cluster, "b1", node).
			Running(cluster, "b2", node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "c1").
			UnableToSchedule(cluster, "c1", node).
			Running(cluster, "c2", node).
			Succeeded(cluster, "c2", node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-1").
			Pending(cluster, "d1").
			UnableToSchedule(cluster, "d1", node).
			Running(cluster, "d2", node).
			Failed(cluster, "d2", node, "something bad")

		// Job set 2
		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, "e1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, "f1").
			UnableToSchedule(cluster, "f1", node).
			Running(cluster, "f2", node).
			Succeeded(cluster, "f2", node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, "h1").
			UnableToSchedule(cluster, "h1", node).
			Running(cluster, "h2", node).
			Failed(cluster, "h2", node, "something bad")

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobSetInfos))

		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set-1",
			JobsQueued:    2,
			JobsPending:   1,
			JobsRunning:   1,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[0])

		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set-2",
			JobsQueued:    1,
			JobsPending:   1,
			JobsRunning:   0,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[1])
	})
}

func assertJobSetInfosAreEqual(t *testing.T, expected *lookout.JobSetInfo, actual *lookout.JobSetInfo) {
	t.Helper()
	assert.Equal(t, expected.JobSet, actual.JobSet)
	assert.Equal(t, expected.Queue, actual.Queue)
	assert.Equal(t, expected.JobsQueued, actual.JobsQueued)
	assert.Equal(t, expected.JobsPending, actual.JobsPending)
	assert.Equal(t, expected.JobsRunning, actual.JobsRunning)
	assert.Equal(t, expected.JobsSucceeded, actual.JobsSucceeded)
	assert.Equal(t, expected.JobsFailed, actual.JobsFailed)
}
