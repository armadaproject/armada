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
	"github.com/armadaproject/armada/pkg/bidstore"
)

func TestInMemoryJobRepository(t *testing.T) {
	jctxs := []*schedulercontext.JobSchedulingContext{
		createJctxCreatedTimeAndPriority("A", 3, 1, true),
		createJctxCreatedTimeAndPriority("A", 1, 1, true),
		createJctxCreatedTimeAndPriority("A", 2, 1, false),
		createJctxCreatedTimeAndPriority("A", 0, 3, false),
		createJctxCreatedTimeAndPriority("A", 0, 0, false),
		createJctxCreatedTimeAndPriority("A", 0, 2, true),
	}
	repo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.JobPriorityComparer{})
	repo.EnqueueMany(jctxs)
	expected := []string{
		jctxs[4].JobId, jctxs[1].JobId, jctxs[2].JobId, jctxs[0].JobId, jctxs[5].JobId, jctxs[3].JobId,
	}
	it := repo.GetJobIterator("A")
	actual := getAllJobIdsFromIterator(t, it)
	assert.Equal(t, expected, actual)
}

func TestInMemoryJobRepository_OnlyYieldEvicted(t *testing.T) {
	jctxs := []*schedulercontext.JobSchedulingContext{
		createJctxCreatedTimeAndPriority("A", 0, 1, true),
		createJctxCreatedTimeAndPriority("A", 1, 1, true),
		createJctxCreatedTimeAndPriority("A", 2, 1, false),
		createJctxCreatedTimeAndPriority("A", 3, 1, false),
		createJctxCreatedTimeAndPriority("A", 4, 1, false),
		createJctxCreatedTimeAndPriority("A", 5, 1, true),
	}

	repo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.JobPriorityComparer{})
	repo.EnqueueMany(jctxs)
	expected := []*jobdb.Job{
		// OnlyYieldEvicted called after 2nd job
		// We should still see the final job, as it is evicted
		jctxs[0].Job, jctxs[1].Job, jctxs[5].Job,
	}
	actual := make([]*jobdb.Job, 0)
	it := repo.GetJobIterator("A")
	for i := 0; ; i++ {
		if i > 1 {
			it.OnlyYieldEvicted()
		}
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

	actual := getAllJobIdsFromIterator(t, it)
	assert.Equal(t, expected, actual)
	v, err := it.Next()
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestMultiJobsIterator_OnlyYieldEvicted(t *testing.T) {
	jctxs := []*schedulercontext.JobSchedulingContext{
		createJctxCreatedTimeAndPriority("B", 0, 1, true),
		createJctxCreatedTimeAndPriority("B", 1, 1, true),
		createJctxCreatedTimeAndPriority("B", 2, 1, false),
		createJctxCreatedTimeAndPriority("B", 3, 1, false),
	}
	inMemoryRepo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.JobPriorityComparer{})
	inMemoryRepo.EnqueueMany(jctxs)

	repo := newMockJobRepository()
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 5) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
	}
	it := NewQueuedJobsIterator("A", testfixtures.TestPool, jobdb.FairShareOrder, repo)

	multiIt := NewMultiJobsIterator(inMemoryRepo.GetJobIterator("B"), it)
	multiIt.OnlyYieldEvicted()

	expected := []string{jctxs[0].JobId, jctxs[1].JobId}
	actual := getAllJobIdsFromIterator(t, multiIt)
	assert.Equal(t, expected, actual)
	v, err := multiIt.Next()
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestMarketDrivenMultiJobsIterator(t *testing.T) {
	newJctxs := []*schedulercontext.JobSchedulingContext{
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_H, false),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_C, false),
	}
	newJctxsRepo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.MarketJobPriorityComparer{})
	newJctxsRepo.EnqueueMany(newJctxs)
	evictedJctxs := []*schedulercontext.JobSchedulingContext{
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_F, true),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_D, true),
	}
	evictedJctxsRepo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.MarketJobPriorityComparer{})
	evictedJctxsRepo.EnqueueMany(evictedJctxs)
	multiIt := NewMarketDrivenMultiJobsIterator(testfixtures.TestPool, newJctxsRepo.GetJobIterator("A"), evictedJctxsRepo.GetJobIterator("A"))

	expected := []string{newJctxs[0].JobId, evictedJctxs[0].JobId, evictedJctxs[1].JobId, newJctxs[1].JobId}
	actual := getAllJobIdsFromIterator(t, multiIt)
	assert.Equal(t, expected, actual)
	v, err := multiIt.Next()
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestMarketDrivenMultiJobsIterator_OnlyYieldEvicted(t *testing.T) {
	newJctxs := []*schedulercontext.JobSchedulingContext{
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_H, false),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_H, false),
	}
	newJctxsRepo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.MarketJobPriorityComparer{})
	newJctxsRepo.EnqueueMany(newJctxs)
	evictedJctxs := []*schedulercontext.JobSchedulingContext{
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_F, true),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_D, true),
	}
	evictedJctxsRepo := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.MarketJobPriorityComparer{})
	evictedJctxsRepo.EnqueueMany(evictedJctxs)
	multiIt := NewMarketDrivenMultiJobsIterator(testfixtures.TestPool, newJctxsRepo.GetJobIterator("A"), evictedJctxsRepo.GetJobIterator("A"))
	multiIt.OnlyYieldEvicted()

	expected := []string{evictedJctxs[0].JobId, evictedJctxs[1].JobId}
	actual := getAllJobIdsFromIterator(t, multiIt)
	assert.Equal(t, expected, actual)
	v, err := multiIt.Next()
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestMarketDrivenMultiJobsIterator_OnlyYieldEvicted_CalledMidIteration(t *testing.T) {
	jctxs1 := []*schedulercontext.JobSchedulingContext{
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_H, false),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_E, false),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_B, true),
	}
	jctxsRepo1 := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.MarketJobPriorityComparer{})
	jctxsRepo1.EnqueueMany(jctxs1)
	jctxs2 := []*schedulercontext.JobSchedulingContext{
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_F, false),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_F, false),
		createJctxWithPrice("A", bidstore.PriceBand_PRICE_BAND_D, true),
	}
	jctxsRepo2 := NewInMemoryJobRepository(testfixtures.TestPool, jobdb.MarketJobPriorityComparer{})
	jctxsRepo2.EnqueueMany(jctxs2)
	multiIt := NewMarketDrivenMultiJobsIterator(testfixtures.TestPool, jctxsRepo1.GetJobIterator("A"), jctxsRepo2.GetJobIterator("A"))

	expected := []string{jctxs1[0].JobId, jctxs2[0].JobId, jctxs2[2].JobId, jctxs1[2].JobId}
	actual := make([]string, 0)
	for i := 0; ; i++ {
		if i > 1 {
			multiIt.OnlyYieldEvicted()
		}
		jctx, err := multiIt.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		actual = append(actual, jctx.Job.Id())
	}
	assert.Equal(t, expected, actual)
	v, err := multiIt.Next()
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
	actual := getAllJobIdsFromIterator(t, it)
	assert.Equal(t, expected, actual)
}

func TestQueuedJobsIterator_OnlyYieldEvicted(t *testing.T) {
	repo := newMockJobRepository()
	for _, req := range testfixtures.N1CpuPodReqs("A", 0, 10) {
		job := jobFromPodSpec("A", req)
		repo.Enqueue(job)
	}
	it := NewQueuedJobsIterator("A", testfixtures.TestPool, jobdb.FairShareOrder, repo)
	it.OnlyYieldEvicted()
	expected := make([]string, 0)
	actual := getAllJobIdsFromIterator(t, it)
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
	actual := getAllJobIdsFromIterator(t, it)
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
	actual := getAllJobIdsFromIterator(t, it)
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
	actual := getAllJobIdsFromIterator(t, it)
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

func getAllJobIdsFromIterator(t *testing.T, it JobContextIterator) []string {
	result := make([]string, 0)
	for {
		jctx, err := it.Next()
		require.NoError(t, err)
		if jctx == nil {
			break
		}
		result = append(result, jctx.Job.Id())
	}
	return result
}

func createJctxWithPrice(queue string, priceBand bidstore.PriceBand, evicted bool) *schedulercontext.JobSchedulingContext {
	emptyRequirements := &internaltypes.PodRequirements{}
	job := testfixtures.TestJob(queue, util.ULID(), "armada-preemptible", emptyRequirements)
	job = job.WithPriceBand(priceBand)
	job = testfixtures.SetPricing(job)
	jctx := schedulercontext.JobSchedulingContextFromJob(job)
	jctx.IsEvicted = evicted

	return jctx
}

func createJctxCreatedTimeAndPriority(queue string, createdTime int64, priority uint32, evicted bool) *schedulercontext.JobSchedulingContext {
	emptyRequirements := &internaltypes.PodRequirements{}
	job := testfixtures.TestJob(queue, util.ULID(), "armada-default", emptyRequirements).WithPriority(priority).WithCreated(createdTime)
	jctx := schedulercontext.JobSchedulingContextFromJob(job)
	jctx.IsEvicted = evicted

	return jctx
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
