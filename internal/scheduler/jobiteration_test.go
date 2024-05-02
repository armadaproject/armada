package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestInMemoryJobRepository(t *testing.T) {
	jobs := []*jobdb.Job{
		testfixtures.TestJob("A", util.ULID(), "armada-default", nil).WithCreated(3).WithPriority(1),
		testfixtures.TestJob("A", util.ULID(), "armada-default", nil).WithCreated(1).WithPriority(1),
		testfixtures.TestJob("A", util.ULID(), "armada-default", nil).WithCreated(2).WithPriority(1),
		testfixtures.TestJob("A", util.ULID(), "armada-default", nil).WithCreated(0).WithPriority(3),
		testfixtures.TestJob("A", util.ULID(), "armada-default", nil).WithCreated(0).WithPriority(0),
		testfixtures.TestJob("A", util.ULID(), "armada-default", nil).WithCreated(0).WithPriority(2),
	}
	jctxs := make([]*schedulercontext.JobSchedulingContext, len(jobs))
	for i, job := range jobs {
		jctxs[i] = &schedulercontext.JobSchedulingContext{Job: job}
	}
	repo := NewInMemoryJobRepository()
	repo.EnqueueMany(jctxs)
	// expected := []string{"0", "1", "2", "3", "4", "5"}
	expected := []*jobdb.Job{
		jobs[4], jobs[1], jobs[2], jobs[0], jobs[5], jobs[3],
	}
	actual := make([]*jobdb.Job, 0)
	it := repo.GetJobIterator("A")
	for {
		jctx, err := it.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job)
	}
	assert.Equal(t, expected, actual)
}

func TestMultiJobsIterator_TwoQueues(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 5) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
		expected = append(expected, job.Id())
	}
	for _, req := range testfixtures.N1CpuPodReqs("B", 0, 5) {
		job := jobFromPodSpec("B", req)
		repo.Enqueue(job)
		expected = append(expected, job.Id())
	}

	ctx := armadacontext.Background()
	its := make([]JobIterator, 3)
	for i, queue := range []string{"A", "B", "C"} {
		it := NewQueuedJobsIterator(ctx, queue, repo, nil)
		its[i] = it
	}
	it := NewMultiJobsIterator(its...)

	actual := make([]string, 0)
	for {
		jctx, err := it.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.Id())
	}
	assert.Equal(t, expected, actual)
	v, err := it.Next()
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestQueuedJobsIterator_OneQueue(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
		expected = append(expected, job.Id())
	}
	ctx := armadacontext.Background()
	it := NewQueuedJobsIterator(ctx, "A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx, err := it.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.Id())
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ExceedsBufferSize(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 17) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
		expected = append(expected, job.Id())
	}
	ctx := armadacontext.Background()
	it := NewQueuedJobsIterator(ctx, "A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx, err := it.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.Id())
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ManyJobs(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 113) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
		expected = append(expected, job.Id())
	}
	ctx := armadacontext.Background()
	it := NewQueuedJobsIterator(ctx, "A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx, err := it.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.Id())
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_TwoQueues(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
		expected = append(expected, job.Id())
	}

	for _, req := range testfixtures.N1CpuPodReqs("B", 0, 10) {
		job := jobFromPodSpec("B", req)
		repo.Enqueue(job)
	}
	ctx := armadacontext.Background()
	it := NewQueuedJobsIterator(ctx, "A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx, err := it.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.Id())
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_RespectsTimeout(t *testing.T) {
	repo := newMockJobRepository()
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
	}

	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	defer cancel()
	it := NewQueuedJobsIterator(ctx, "A", repo, nil)
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
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
	}
	ctx := armadacontext.Background()
	it := NewQueuedJobsIterator(ctx, "A", repo, nil)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		require.NoError(t, err)
	}
	job, err := it.Next()
	assert.Nil(t, job)
	assert.NoError(t, err)
}

// TODO: Deprecate in favour of InMemoryRepo.
type mockJobRepository struct {
	jobsByQueue map[string][]*jobdb.Job
	jobsById    map[string]*jobdb.Job
	// Ids of all jobs hat were leased to an executor.
	leasedJobs          map[string]bool
	getQueueJobIdsDelay time.Duration
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobsByQueue: make(map[string][]*jobdb.Job),
		jobsById:    make(map[string]*jobdb.Job),
		leasedJobs:  make(map[string]bool),
	}
}

func (repo *mockJobRepository) EnqueueMany(jobs []*jobdb.Job) {
	for _, job := range jobs {
		repo.Enqueue(job)
	}
}

func (repo *mockJobRepository) Enqueue(job *jobdb.Job) {
	repo.jobsByQueue[job.Queue()] = append(repo.jobsByQueue[job.Queue()], job)
	repo.jobsById[job.Id()] = job
}

func (repo *mockJobRepository) GetJobIterator(ctx *armadacontext.Context, queue string) JobIterator {
	return NewQueuedJobsIterator(ctx, queue, repo, nil)
}

func (repo *mockJobRepository) GetQueueJobIds(queue string) []string {
	time.Sleep(repo.getQueueJobIdsDelay)
	if jobs, ok := repo.jobsByQueue[queue]; ok {
		rv := make([]string, 0, len(jobs))
		for _, job := range jobs {
			if !repo.leasedJobs[job.Id()] {
				rv = append(rv, job.Id())
			}
		}
		return rv
	} else {
		return make([]string, 0)
	}
}

func (repo *mockJobRepository) GetExistingJobsByIds(jobIds []string) []*jobdb.Job {
	rv := make([]*jobdb.Job, len(jobIds))
	for i, jobId := range jobIds {
		if job, ok := repo.jobsById[jobId]; ok {
			rv[i] = job
		}
	}
	return rv
}

func jobFromPodSpec(queue string, req *schedulerobjects.PodRequirements) *jobdb.Job {
	return testfixtures.TestJob(queue, util.ULID(), "armada-default", req)
}
