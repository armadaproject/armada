package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestInMemoryJobRepository(t *testing.T) {
	emptyRequirements := &internaltypes.PodRequirements{}
	jobs := []*jobdb.Job{
		testfixtures.TestJob("A", util.ULID(), "armada-default", emptyRequirements).WithCreated(3).WithPriority(1),
		testfixtures.TestJob("A", util.ULID(), "armada-default", emptyRequirements).WithCreated(1).WithPriority(1),
		testfixtures.TestJob("A", util.ULID(), "armada-default", emptyRequirements).WithCreated(2).WithPriority(1),
		testfixtures.TestJob("A", util.ULID(), "armada-default", emptyRequirements).WithCreated(0).WithPriority(3),
		testfixtures.TestJob("A", util.ULID(), "armada-default", emptyRequirements).WithCreated(0).WithPriority(0),
		testfixtures.TestJob("A", util.ULID(), "armada-default", emptyRequirements).WithCreated(0).WithPriority(2),
	}
	jctxs := make([]*schedulercontext.JobSchedulingContext, len(jobs))
	for i, job := range jobs {
		jctxs[i] = &schedulercontext.JobSchedulingContext{Job: job, KubernetesResourceRequirements: job.KubernetesResourceRequirements()}
	}
	repo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.JobPriorityComparer{})
	repo.EnqueueMany(jctxs)
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

	its := make([]JobContextIterator, 3)
	for i, queue := range []string{"A", "B", "C"} {
		it := NewQueuedJobsIterator(queue, testfixtures.TestPool, jobdb.FairShareOrder, repo)
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
	it := NewQueuedJobsIterator("A", testfixtures.TestPool, jobdb.FairShareOrder, repo)
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
	it := NewQueuedJobsIterator("A", testfixtures.TestPool, jobdb.FairShareOrder, repo)
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
	it := NewQueuedJobsIterator("A", testfixtures.TestPool, jobdb.FairShareOrder, repo)
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
	it := NewQueuedJobsIterator("A", testfixtures.TestPool, jobdb.FairShareOrder, repo)
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

func TestCreateQueuedJobsIterator_NilOnEmpty(t *testing.T) {
	repo := newMockJobRepository()
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
	}
	it := NewQueuedJobsIterator("A", testfixtures.TestPool, jobdb.FairShareOrder, repo)
	for job, err := it.Next(); job != nil; job, err = it.Next() {
		require.NoError(t, err)
	}
	job, err := it.Next()
	assert.Nil(t, job)
	assert.NoError(t, err)
}

type mockJobIterator struct {
	jobs []*jobdb.Job
	i    int
}

func (iter *mockJobIterator) Done() bool {
	return iter.i >= len(iter.jobs)
}

func (iter *mockJobIterator) Next() (*jobdb.Job, bool) {
	if iter.Done() {
		return nil, false
	}
	job := iter.jobs[iter.i]
	iter.i++
	return job, true
}

type mockJobRepository struct {
	jobsByQueue  map[string][]*jobdb.Job
	jobsByGangId map[gangKey][]*jobdb.Job
	jobsById     map[string]*jobdb.Job
}

func (repo *mockJobRepository) QueuedJobs(queue string, _ string, _ jobdb.JobSortOrder) jobdb.JobIterator {
	q := repo.jobsByQueue[queue]
	return &mockJobIterator{jobs: q}
}

func (repo *mockJobRepository) GetGangJobsByGangId(queue string, gangId string) ([]*jobdb.Job, error) {
	return repo.jobsByGangId[gangKey{queue: queue, gangId: gangId}], nil
}

func (repo *mockJobRepository) GetById(id string) *jobdb.Job {
	j, _ := repo.jobsById[id]
	return j
}

func (repo *mockJobRepository) NewJob(
	jobId string,
	jobSet string,
	queue string,
	priority uint32,
	schedulingInfo *internaltypes.JobSchedulingInfo,
	queued bool,
	queuedVersion int32,
	cancelRequested bool,
	cancelByJobSetRequested bool,
	cancelled bool,
	created int64,
	validated bool,
	pools []string,
	priceBand int32,
) (*jobdb.Job, error) {
	return &jobdb.Job{}, nil
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobsByQueue:  make(map[string][]*jobdb.Job),
		jobsByGangId: make(map[gangKey][]*jobdb.Job),
		jobsById:     make(map[string]*jobdb.Job),
	}
}

func (repo *mockJobRepository) EnqueueMany(jobs []*jobdb.Job) {
	for _, job := range jobs {
		repo.Enqueue(job)
	}
}

func (repo *mockJobRepository) Enqueue(job *jobdb.Job) {
	repo.jobsByQueue[job.Queue()] = append(repo.jobsByQueue[job.Queue()], job)
	if job.IsInGang() {
		key := gangKey{queue: job.Queue(), gangId: job.GetGangInfo().Id()}
		repo.jobsByGangId[key] = append(repo.jobsByGangId[key], job)
	}
	repo.jobsById[job.Id()] = job
}

func (repo *mockJobRepository) GetJobIterator(queue string) JobContextIterator {
	return NewQueuedJobsIterator(queue, testfixtures.TestPool, jobdb.FairShareOrder, repo)
}

func jobFromPodSpec(queue string, req *internaltypes.PodRequirements) *jobdb.Job {
	return testfixtures.TestJob(queue, util.ULID(), "armada-default", req)
}
