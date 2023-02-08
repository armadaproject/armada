package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiJobsIterator_TwoQueues(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 5) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}
	for _, req := range testNSmallCpuJob(0, 5) {
		job := apiJobFromPodSpec("B", podSpecFromPodRequirements(req))
		job.Queue = "B"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	its := make([]JobIterator, 4)
	for i, queue := range []string{"A", "B", "C"} {
		it, err := NewQueuedJobsIterator(ctx, queue, repo)
		if !assert.NoError(t, err) {
			return
		}
		its[i+1] = it
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

func TestMultiJobsIterator_Nils(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 5) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	ctx := context.Background()
	it, err := NewQueuedJobsIterator(ctx, "A", repo)
	if !assert.NoError(t, err) {
		return
	}
	multiIt := NewMultiJobsIterator([]JobIterator{nil, it}...)

	actual := make([]string, 0)
	for job, err := multiIt.Next(); job != nil; job, err = multiIt.Next() {
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
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
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
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 17) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
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
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 113) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
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
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("B", podSpecFromPodRequirements(req))
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
	repo := newMockJobRepository()
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
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
	repo := newMockJobRepository()
	for _, req := range testNSmallCpuJob(0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
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
