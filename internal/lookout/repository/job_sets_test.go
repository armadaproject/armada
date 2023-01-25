package repository

import (
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api/lookout"
)

func TestGetJobSetInfos_GetNoJobSetsIfQueueDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		NewJobSimulator(t, jobStore).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Empty(t, jobSetInfos)
	})
}

func TestGetJobSetInfos_GetsJobSetWithNoFinishedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		recentTime := someTime.Add(3 * time.Hour)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set", "user", someTime, map[string]string{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set", "user", recentTime, map[string]string{}).
			Pending(cluster, k8sId1)

		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

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
			Submitted:     &recentTime,
		}, jobSetInfos[0])
	})
}

func TestGetJobSetInfos_GetsJobSetWithOnlyFinishedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		recentTime := someTime.Add(3 * time.Hour)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set", "user", someTime, map[string]string{}).
			Running(cluster, k8sId1, node).
			Succeeded(cluster, k8sId1, node)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set", "user", recentTime, map[string]string{}).
			Pending(cluster, k8sId2).
			Failed(cluster, k8sId2, node, "some error")

		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

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
			Submitted:     &recentTime,
		}, jobSetInfos[0])
	})
}

func TestGetJobSetInfos_JobSetsCounts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "a1").
			UnableToSchedule(cluster, "a1", node).
			Pending(cluster, "a2")

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "b1").
			Running(cluster, "b2", node)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "c1").
			UnableToSchedule(cluster, "c1", node).
			Running(cluster, "c2", node).
			Succeeded(cluster, "c2", node)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "d1").
			Running(cluster, "d2", node).
			Failed(cluster, "d2", node, "something bad")

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "e1").
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
			JobsCancelled: 1,
			Submitted:     &someTime,
		}, jobSetInfos[0])
	})
}

func TestGetJobSetInfos_MultipleJobSetsCounts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		// Job set 1
		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "a1").
			UnableToSchedule(cluster, "a1", node).
			Pending(cluster, "a2")

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "b1").
			UnableToSchedule(cluster, "b1", node).
			Running(cluster, "b2", node)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "c1").
			UnableToSchedule(cluster, "c1", node).
			Running(cluster, "c2", node).
			Succeeded(cluster, "c2", node)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, map[string]string{}).
			Pending(cluster, "d1").
			UnableToSchedule(cluster, "d1", node).
			Running(cluster, "d2", node).
			Failed(cluster, "d2", node, "something bad")

		// Job set 2
		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime, map[string]string{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime, map[string]string{}).
			Pending(cluster, "e1")

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime, map[string]string{}).
			Pending(cluster, "f1").
			UnableToSchedule(cluster, "f1", node).
			Running(cluster, "f2", node).
			Succeeded(cluster, "f2", node)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime, map[string]string{}).
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
			Submitted:     &someTime,
		}, jobSetInfos[0])

		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set-2",
			JobsQueued:    1,
			JobsPending:   1,
			JobsRunning:   0,
			JobsSucceeded: 1,
			JobsFailed:    1,
			Submitted:     &someTime,
		}, jobSetInfos[1])
	})
}

func TestGetJobSetInfos_StatsWithNoRunningOrQueuedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			UnableToSchedule(cluster, k8sId1, node).
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node).
			Succeeded(cluster, k8sId2, node)

		jobSets, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobSets))

		assert.Nil(t, jobSets[0].RunningStats)
		assert.Nil(t, jobSets[0].QueuedStats)
	})
}

func TestGetJobSetInfos_GetRunningStats(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		currentTime := someTime.Add(20 * time.Minute)

		jobRepo := NewSQLJobRepository(db, &util.DummyClock{currentTime})

		for i := 0; i < 11; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			itTime := someTime.Add(time.Duration(i) * time.Minute)
			NewJobSimulator(t, jobStore).
				CreateJob(queue).
				UnableToSchedule(cluster, k8sId, node).
				Pending(cluster, otherK8sId).
				RunningAtTime(cluster, otherK8sId, node, itTime)

			NewJobSimulator(t, jobStore).
				CreateJob(queue)

			NewJobSimulator(t, jobStore).
				CreateJob(queue).
				Running(cluster, util.NewULID(), node).
				Failed(cluster, util.NewULID(), node, "an error")

			NewJobSimulator(t, jobStore).
				CreateJob(queue).
				Cancelled()
		}

		// All the same, except for last
		for i := 0; i < 10; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			NewJobSimulator(t, jobStore).
				CreateJobWithJobSet(queue, "job-set-2").
				UnableToSchedule(cluster, k8sId, node).
				Pending(cluster, otherK8sId).
				RunningAtTime(cluster, otherK8sId, node, someTime)

			NewJobSimulator(t, jobStore).
				CreateJobWithJobSet(queue, "job-set-2")

			NewJobSimulator(t, jobStore).
				CreateJobWithJobSet(queue, "job-set-2").
				Cancelled()
		}

		otherTime := someTime.Add(10 * time.Minute)
		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set-2").
			UnableToSchedule(cluster, k8sId1, node).
			Pending(cluster, k8sId2).
			RunningAtTime(cluster, k8sId2, node, otherTime)

		jobSets, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobSets))

		assertDurationStatsAreEqual(t, &lookout.DurationStats{
			Shortest: types.DurationProto(10 * time.Minute),
			Longest:  types.DurationProto(20 * time.Minute),
			Average:  types.DurationProto(15 * time.Minute),
			Median:   types.DurationProto(15 * time.Minute),
			Q1:       types.DurationProto(12*time.Minute + 30*time.Second),
			Q3:       types.DurationProto(17*time.Minute + 30*time.Second),
		}, jobSets[0].RunningStats)

		assertDurationStatsAreEqual(t, &lookout.DurationStats{
			Shortest: types.DurationProto(10 * time.Minute),
			Longest:  types.DurationProto(20 * time.Minute),
			Average:  types.DurationProto(19*time.Minute + 5*time.Second),
			Median:   types.DurationProto(20 * time.Minute),
			Q1:       types.DurationProto(20 * time.Minute),
			Q3:       types.DurationProto(20 * time.Minute),
		}, jobSets[1].RunningStats)
	})
}

func TestGetJobSetInfos_GetQueuedStats(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		someTime := time.Now()
		currentTime := someTime.Add(30 * time.Minute)

		jobRepo := NewSQLJobRepository(db, &util.DummyClock{currentTime})

		for i := 0; i < 11; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			itTime := someTime.Add(time.Duration(i) * time.Minute)
			NewJobSimulator(t, jobStore).
				CreateJob(queue).
				Pending(cluster, k8sId).
				Pending(cluster, otherK8sId).
				Running(cluster, otherK8sId, node)

			NewJobSimulator(t, jobStore).
				CreateJobAtTime(queue, itTime)

			NewJobSimulator(t, jobStore).
				CreateJob(queue).
				Cancelled()
		}

		for i := 0; i < 10; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			NewJobSimulator(t, jobStore).
				CreateJobWithJobSet(queue, "job-set-2").
				UnableToSchedule(cluster, k8sId, node).
				Pending(cluster, otherK8sId).
				Running(cluster, otherK8sId, node)

			NewJobSimulator(t, jobStore).
				CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime, nil)

			NewJobSimulator(t, jobStore).
				CreateJobWithJobSet(queue, "job-set-2").
				Cancelled()
		}

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime.Add(20*time.Minute), nil)

		jobSets, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobSets))

		assertDurationStatsAreEqual(t, &lookout.DurationStats{
			Shortest: types.DurationProto(20 * time.Minute),
			Longest:  types.DurationProto(30 * time.Minute),
			Average:  types.DurationProto(25 * time.Minute),
			Median:   types.DurationProto(25 * time.Minute),
			Q1:       types.DurationProto(22*time.Minute + 30*time.Second),
			Q3:       types.DurationProto(27*time.Minute + 30*time.Second),
		}, jobSets[0].QueuedStats)

		assertDurationStatsAreEqual(t, &lookout.DurationStats{
			Shortest: types.DurationProto(10 * time.Minute),
			Longest:  types.DurationProto(30 * time.Minute),
			Average:  types.DurationProto(28*time.Minute + 11*time.Second),
			Median:   types.DurationProto(30 * time.Minute),
			Q1:       types.DurationProto(30 * time.Minute),
			Q3:       types.DurationProto(30 * time.Minute),
		}, jobSets[1].QueuedStats)
	})
}

func TestGetJobSetInfos_GetOnlyActiveJobSets(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set-1")

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, k8sId2)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set-3").
			Running(cluster, k8sId2, node)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set-4").
			Cancelled()

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set-5").
			UnableToSchedule(cluster, k8sId1, node).
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node).
			Succeeded(cluster, k8sId2, node)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set-6").
			UnableToSchedule(cluster, k8sId1, node).
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node).
			Failed(cluster, k8sId2, node, "some error")

		jobSets, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{
			Queue:      queue,
			ActiveOnly: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobSets))

		assert.Equal(t, "job-set-1", jobSets[0].JobSet)
		assert.Equal(t, "job-set-2", jobSets[1].JobSet)
		assert.Equal(t, "job-set-3", jobSets[2].JobSet)
	})
}

func TestGetJobSetInfos_GetNewestFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime.Add(1*time.Hour), nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-3", "user", someTime.Add(2*time.Hour), nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-4", "user", someTime.Add(3*time.Hour), nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-5", "user", someTime.Add(4*time.Hour), nil)

		jobSets, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{
			Queue:       queue,
			NewestFirst: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, 5, len(jobSets))

		assert.Equal(t, "job-set-5", jobSets[0].JobSet)
		assert.Equal(t, "job-set-4", jobSets[1].JobSet)
		assert.Equal(t, "job-set-3", jobSets[2].JobSet)
		assert.Equal(t, "job-set-2", jobSets[3].JobSet)
		assert.Equal(t, "job-set-1", jobSets[4].JobSet)
	})
}

func TestGetJobSetInfos_GetOldestFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-1", "user", someTime, nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-2", "user", someTime.Add(1*time.Hour), nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-3", "user", someTime.Add(2*time.Hour), nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-4", "user", someTime.Add(3*time.Hour), nil)

		NewJobSimulator(t, jobStore).
			CreateJobWithOpts(queue, util.NewULID(), "job-set-5", "user", someTime.Add(4*time.Hour), nil)

		jobSets, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{
			Queue:       queue,
			NewestFirst: false,
		})
		assert.NoError(t, err)
		assert.Equal(t, 5, len(jobSets))

		assert.Equal(t, "job-set-1", jobSets[0].JobSet)
		assert.Equal(t, "job-set-2", jobSets[1].JobSet)
		assert.Equal(t, "job-set-3", jobSets[2].JobSet)
		assert.Equal(t, "job-set-4", jobSets[3].JobSet)
		assert.Equal(t, "job-set-5", jobSets[4].JobSet)
	})
}

func assertDurationStatsAreEqual(t *testing.T, expected *lookout.DurationStats, actual *lookout.DurationStats) {
	t.Helper()
	AssertProtoDurationsApproxEqual(t, expected.Longest, actual.Longest)
	AssertProtoDurationsApproxEqual(t, expected.Shortest, actual.Shortest)
	AssertProtoDurationsApproxEqual(t, expected.Average, actual.Average)
	AssertProtoDurationsApproxEqual(t, expected.Median, actual.Median)
	AssertProtoDurationsApproxEqual(t, expected.Q1, actual.Q1)
	AssertProtoDurationsApproxEqual(t, expected.Q3, actual.Q3)
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
	AssertTimesApproxEqual(t, expected.Submitted, actual.Submitted)
}
