package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
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
			PodSpec:  &v1.PodSpec{},
		},
		{
			Queue:    "A",
			Id:       "1",
			Priority: 1,
			Created:  T.Add(1 * time.Second),
			PodSpec:  &v1.PodSpec{},
		},
		{
			Queue:    "A",
			Id:       "2",
			Priority: 1,
			Created:  T.Add(2 * time.Second),
			PodSpec:  &v1.PodSpec{},
		},
		{
			Queue:    "A",
			Id:       "5",
			Priority: 3,
			PodSpec:  &v1.PodSpec{},
		},
		{
			Queue:    "A",
			Id:       "0",
			Priority: 0,
			PodSpec:  &v1.PodSpec{},
		},
		{
			Queue:    "A",
			Id:       "4",
			Priority: 2,
			PodSpec:  &v1.PodSpec{},
		},
	}
	jctxs := make([]*schedulercontext.JobSchedulingContext, len(jobs))
	for i, job := range jobs {
		jctxs[i] = &schedulercontext.JobSchedulingContext{Job: job}
	}
	repo := NewInMemoryJobRepository()
	repo.EnqueueMany(jctxs)
	expected := []string{"0", "1", "2", "3", "4", "5"}
	actual := make([]string, 0)
	it := repo.GetJobIterator("A")
	for {
		jctx := it.Next()
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestMultiJobsIterator_TwoQueues(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 5) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}
	for _, req := range testfixtures.N1CpuPodReqs("B", 0, 5) {
		job := apiJobFromPodSpec("B", podSpecFromPodRequirements(req))
		job.Queue = "B"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	its := make([]JobIterator, 3)
	for i, queue := range []string{"A", "B", "C"} {
		it := NewQueuedJobsIterator(queue, repo, nil)
		its[i] = it
	}
	it := NewMultiJobsIterator(its...)

	actual := make([]string, 0)
	for {
		jctx := it.Next()
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.GetId())
	}
	assert.Equal(t, expected, actual)
	v := it.Next()
	require.Nil(t, v)
}

func TestQueuedJobsIterator_OneQueue(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	it := NewQueuedJobsIterator("A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx := it.Next()
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ExceedsBufferSize(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 17) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	it := NewQueuedJobsIterator("A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx := it.Next()
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_ManyJobs(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 113) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	it := NewQueuedJobsIterator("A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx := it.Next()
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_TwoQueues(t *testing.T) {
	repo := newMockJobRepository()
	expected := make([]string, 0)
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		repo.Enqueue(job)
		expected = append(expected, job.Id)
	}

	for _, req := range testfixtures.N1CpuPodReqs("B", 0, 10) {
		job := apiJobFromPodSpec("B", podSpecFromPodRequirements(req))
		repo.Enqueue(job)
	}

	it := NewQueuedJobsIterator("A", repo, nil)
	actual := make([]string, 0)
	for {
		jctx := it.Next()
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.GetId())
	}
	assert.Equal(t, expected, actual)
}

func TestCreateQueuedJobsIterator_NilOnEmpty(t *testing.T) {
	repo := newMockJobRepository()
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := apiJobFromPodSpec("A", podSpecFromPodRequirements(req))
		job.Queue = "A"
		repo.Enqueue(job)
	}

	it := NewQueuedJobsIterator("A", repo, nil)
	for job := it.Next(); job != nil; job = it.Next() {
		// Do nothing
	}
	job := it.Next()
	assert.Nil(t, job)
}

// TODO: Deprecate in favour of InMemoryRepo.
type mockJobRepository struct {
	jobsByQueue map[string][]*api.Job
	jobsById    map[string]*api.Job
	// Ids of all jobs hat were leased to an executor.
	leasedJobs          map[string]bool
	getQueueJobIdsDelay time.Duration
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobsByQueue: make(map[string][]*api.Job),
		jobsById:    make(map[string]*api.Job),
		leasedJobs:  make(map[string]bool),
	}
}

func (repo *mockJobRepository) EnqueueMany(jobs []*api.Job) {
	for _, job := range jobs {
		repo.Enqueue(job)
	}
}

func (repo *mockJobRepository) Enqueue(job *api.Job) {
	repo.jobsByQueue[job.Queue] = append(repo.jobsByQueue[job.Queue], job)
	repo.jobsById[job.Id] = job
}

func (repo *mockJobRepository) GetJobIterator(queue string) JobIterator {
	return NewQueuedJobsIterator(queue, repo, nil)
}

func (repo *mockJobRepository) GetQueueJobIds(queue string) []string {
	time.Sleep(repo.getQueueJobIdsDelay)
	if jobs, ok := repo.jobsByQueue[queue]; ok {
		rv := make([]string, 0, len(jobs))
		for _, job := range jobs {
			if !repo.leasedJobs[job.Id] {
				rv = append(rv, job.Id)
			}
		}
		return rv
	} else {
		return make([]string, 0)
	}
}

func (repo *mockJobRepository) GetExistingJobsByIds(jobIds []string) []interfaces.LegacySchedulerJob {
	rv := make([]interfaces.LegacySchedulerJob, len(jobIds))
	for i, jobId := range jobIds {
		if job, ok := repo.jobsById[jobId]; ok {
			rv[i] = job
		}
	}
	return rv
}

func apiJobFromPodSpec(queue string, podSpec *v1.PodSpec) *api.Job {
	return &api.Job{
		Id:      util.NewULID(),
		PodSpec: podSpec,
		Queue:   queue,
	}
}

func podSpecFromPodRequirements(req *schedulerobjects.PodRequirements) *v1.PodSpec {
	return &v1.PodSpec{
		NodeSelector:     req.NodeSelector,
		Affinity:         req.Affinity,
		Tolerations:      req.Tolerations,
		Priority:         &req.Priority,
		PreemptionPolicy: (*v1.PreemptionPolicy)(&req.PreemptionPolicy),
		Containers: []v1.Container{
			{
				Resources: req.ResourceRequirements,
			},
		},
	}
}
