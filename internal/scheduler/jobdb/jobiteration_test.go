package jobdb

import (
	"context"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/api"
)

func TestInMemoryJobRepository(t *testing.T) {
	T := time.Now()
	jobs := []*api.Job{
		{
			Queue:    "A",
			Id:       "3",
			Priority: 1,
			Created:  T.Add(3 * time.Second),
		},
		{
			Queue:    "A",
			Id:       "1",
			Priority: 1,
			Created:  T.Add(1 * time.Second),
		},
		{
			Queue:    "A",
			Id:       "2",
			Priority: 1,
			Created:  T.Add(2 * time.Second),
		},
		{
			Queue:    "A",
			Id:       "5",
			Priority: 3,
		},
		{
			Queue:    "A",
			Id:       "0",
			Priority: 0,
		},
		{
			Queue:    "A",
			Id:       "4",
			Priority: 2,
		},
	}
	legacySchedulerJobs := make([]scheduler.LegacySchedulerJob, len(jobs))
	for i, job := range jobs {
		legacySchedulerJobs[i] = job
	}
	repo := NewInMemoryJobRepository(testfixtures.TestPriorityClasses)
	repo.EnqueueMany(legacySchedulerJobs)
	expected := []string{"0", "1", "2", "3", "4", "5"}
	actual, err := repo.GetQueueJobIds("A")
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestMultiJobsIterator_TwoQueues(t *testing.T) {
	repo := scheduler.newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.TestNSmallCpuJob("A", 0, 5) {
		job := scheduler.apiJobFromPodSpec("A", scheduler.podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}
	for _, req := range testfixtures.TestNSmallCpuJob("B", 0, 5) {
		job := scheduler.apiJobFromPodSpec("B", scheduler.podSpecFromPodRequirements(req))
		job.Queue = "B"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	its := make([]JobIterator, 3)
	for i, queue := range []string{"A", "B", "C"} {
		it, err := NewQueuedJobsIterator(ctx, queue, repo)
		if !assert.NoError(t, err) {
			return
		}
		its[i] = it
	}
	it := NewMultiJobsIterator(its...)

	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.GetId())
	}
	assert.Equal(t, expected, actual)
	v, err := it.Next()
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestQueuedJobsIterator_OneQueue(t *testing.T) {
	repo := scheduler.newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.TestNSmallCpuJob("A", 0, 10) {
		job := scheduler.apiJobFromPodSpec("A", scheduler.podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ExceedsBufferSize(t *testing.T) {
	repo := scheduler.newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.TestNSmallCpuJob("A", 0, 17) {
		job := scheduler.apiJobFromPodSpec("A", scheduler.podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ManyJobs(t *testing.T) {
	repo := scheduler.newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.TestNSmallCpuJob("A", 0, 113) {
		job := scheduler.apiJobFromPodSpec("A", scheduler.podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_TwoQueues(t *testing.T) {
	repo := scheduler.newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.TestNSmallCpuJob("A", 0, 10) {
		job := scheduler.apiJobFromPodSpec("A", scheduler.podSpecFromPodRequirements(req))
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	for _, req := range testfixtures.TestNSmallCpuJob("B", 0, 10) {
		job := scheduler.apiJobFromPodSpec("B", scheduler.podSpecFromPodRequirements(req))
		repo.Enqueue(job)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	actual := make([]string, 0)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
		actual = append(actual, job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_RespectsTimeout(t *testing.T) {
	repo := scheduler.newMockJobRepository()
	for _, req := range testfixtures.TestNSmallCpuJob("A", 0, 10) {
		job := scheduler.apiJobFromPodSpec("A", scheduler.podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	defer cancel()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	job, err := it.Next()
	assert.Nil(t, job)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Calling again should produce the same error.
	job, err = it.Next()
	assert.Nil(t, job)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCreateQueuedJobsIterator_NilOnEmpty(t *testing.T) {
	repo := scheduler.newMockJobRepository()
	for _, req := range testfixtures.TestNSmallCpuJob("A", 0, 10) {
		job := scheduler.apiJobFromPodSpec("A", scheduler.podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		if !assert.NoError(t, err) {
			return
		}
	}
	job, err := it.Next()
	assert.Nil(t, job)
	assert.NoError(t, err)
}
