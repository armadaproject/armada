package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/cache"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"
)

func TestAggregatedQueueServer_ReturnLeaseCallsRepositoryMethod(t *testing.T) {
	mockJobRepository, _, aggregatedQueueClient := makeAggregatedQueueServerWithTestDoubles(5)

	clusterId := "cluster-1"
	jobId := "job-id-1"
	job := &api.Job{Id: jobId}

	_, addJobsErr := mockJobRepository.AddJobs([]*api.Job{job})
	assert.Nil(t, addJobsErr)

	_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId: clusterId,
		JobId:     jobId,
	})

	assert.Nil(t, err)
	assert.Equal(t, 1, mockJobRepository.returnLeaseCalls)
	assert.Equal(t, clusterId, mockJobRepository.returnLeaseArg1)
	assert.Equal(t, jobId, mockJobRepository.returnLeaseArg2)
}

func TestAggregatedQueueServer_ReturningLeaseMoreThanMaxRetriesDeletesJob(t *testing.T) {
	maxRetries := 5
	mockJobRepository, _, aggregatedQueueClient := makeAggregatedQueueServerWithTestDoubles(uint(maxRetries))

	clusterId := "cluster-1"
	jobId := "job-id-1"
	job := &api.Job{Id: jobId}

	_, addJobsErr := mockJobRepository.AddJobs([]*api.Job{job})
	assert.Nil(t, addJobsErr)

	for i := 0; i < maxRetries; i++ {
		_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
			ClusterId: clusterId,
			JobId:     jobId,
		})
		assert.Nil(t, err)

		assert.Equal(t, i+1, mockJobRepository.returnLeaseCalls)
		assert.Equal(t, clusterId, mockJobRepository.returnLeaseArg1)
		assert.Equal(t, jobId, mockJobRepository.returnLeaseArg2)
	}

	_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId: clusterId,
		JobId:     jobId,
	})
	assert.Nil(t, err)
	assert.Equal(t, maxRetries, mockJobRepository.returnLeaseCalls)

	assert.Equal(t, 1, mockJobRepository.deleteJobsCalls)
	assert.Equal(t, []*api.Job{job}, mockJobRepository.deleteJobsArg)
}

func TestAggregatedQueueServer_ReturningLeaseMoreThanMaxRetriesSendsJobFailedEvent(t *testing.T) {
	maxRetries := 7
	mockJobRepository, fakeEventStore, aggregatedQueueClient := makeAggregatedQueueServerWithTestDoubles(uint(maxRetries))

	clusterId := "cluster-1"
	jobId := "job-id-1"
	jobSetId := "job-set-id-1"
	queue := "queue-1"
	job := &api.Job{
		Id:       jobId,
		JobSetId: jobSetId,
		Queue:    queue,
	}

	_, addJobsErr := mockJobRepository.AddJobs([]*api.Job{job})
	assert.Nil(t, addJobsErr)

	for i := 0; i < maxRetries; i++ {
		_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
			ClusterId: clusterId,
			JobId:     jobId,
		})
		assert.Nil(t, err)
	}

	_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId: clusterId,
		JobId:     jobId,
	})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(fakeEventStore.events))

	failedEvent := fakeEventStore.events[0].GetFailed()
	assert.Equal(t, jobId, failedEvent.JobId)
	assert.Equal(t, jobSetId, failedEvent.JobSetId)
	assert.Equal(t, queue, failedEvent.Queue)
	assert.Equal(t, clusterId, failedEvent.ClusterId)
	assert.Equal(t, fmt.Sprintf("Exceeded maximum number of retries: %d", maxRetries), failedEvent.Reason)
}

func makeAggregatedQueueServerWithTestDoubles(maxRetries uint) (*mockJobRepository, *fakeEventStore, *AggregatedQueueServer) {
	mockJobRepository := newMockJobRepository()
	fakeEventStore := &fakeEventStore{}
	fakeQueueRepository := &fakeQueueRepository{}
	fakeSchedulingInfoRepository := &fakeSchedulingInfoRepository{}
	return mockJobRepository, fakeEventStore, NewAggregatedQueueServer(
		&FakePermissionChecker{},
		configuration.SchedulingConfig{
			MaxRetries: maxRetries,
		},
		mockJobRepository,
		cache.NewQueueCache(fakeQueueRepository, mockJobRepository, fakeSchedulingInfoRepository),
		fakeQueueRepository,
		&fakeUsageRepository{},
		fakeEventStore,
		fakeSchedulingInfoRepository)
}

type mockJobRepository struct {
	jobs       map[string]*api.Job
	jobRetries map[string]int

	returnLeaseCalls int
	deleteJobsCalls  int

	returnLeaseArg1 string
	returnLeaseArg2 string
	deleteJobsArg   []*api.Job
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobs:             make(map[string]*api.Job),
		jobRetries:       make(map[string]int),
		returnLeaseCalls: 0,
		deleteJobsCalls:  0,
		returnLeaseArg1:  "",
		returnLeaseArg2:  "",
		deleteJobsArg:    nil,
	}
}

func (repo *mockJobRepository) GetQueueJobIds(queueName string) ([]string, error) {
	return []string{}, nil
}

func (repo *mockJobRepository) CreateJobs(request *api.JobSubmitRequest, owner string, ownershipGroups []string) ([]*api.Job, error) {
	return []*api.Job{}, nil
}

func (repo *mockJobRepository) AddJobs(job []*api.Job) ([]*repository.SubmitJobResult, error) {
	for _, j := range job {
		repo.jobs[j.Id] = j
	}
	return []*repository.SubmitJobResult{}, nil
}

func (repo *mockJobRepository) GetExistingJobsByIds(ids []string) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0)
	for _, id := range ids {
		if job, ok := repo.jobs[id]; ok {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (repo *mockJobRepository) FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error) {
	return []*api.Queue{}, nil
}

func (repo *mockJobRepository) GetQueueSizes(queues []*api.Queue) (sizes []int64, e error) {
	return []int64{}, nil
}

func (repo *mockJobRepository) RenewLease(clusterId string, jobIds []string) (renewed []string, e error) {
	return []string{}, nil
}

func (repo *mockJobRepository) ExpireLeases(queue string, deadline time.Time) (expired []*api.Job, e error) {
	return []*api.Job{}, nil
}

func (repo *mockJobRepository) ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error) {
	repo.returnLeaseCalls++
	repo.returnLeaseArg1 = clusterId
	repo.returnLeaseArg2 = jobId
	return nil, nil
}

func (repo *mockJobRepository) DeleteJobs(jobs []*api.Job) map[*api.Job]error {
	repo.deleteJobsCalls++
	repo.deleteJobsArg = jobs

	for _, job := range jobs {
		delete(repo.jobs, job.Id)
	}

	return map[*api.Job]error{}
}

func (repo *mockJobRepository) GetActiveJobIds(queue string, jobSetId string) ([]string, error) {
	return []string{}, nil
}

func (repo *mockJobRepository) GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error) {
	return []*api.JobSetInfo{}, nil
}

func (repo *mockJobRepository) AddRetryAttempt(jobId string) error {
	_, ok := repo.jobs[jobId]
	if !ok {
		return fmt.Errorf("No job with id %q found", jobId)
	}
	repo.jobRetries[jobId]++
	return nil
}

func (repo *mockJobRepository) GetNumberOfRetryAttempts(jobId string) (int, error) {
	_, ok := repo.jobs[jobId]
	if !ok {
		return 0, fmt.Errorf("No job with id %q found", jobId)
	}
	return repo.jobRetries[jobId], nil
}

func (repo *mockJobRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	return []*api.Job{}, nil
}

func (repo *mockJobRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	return []*api.Job{}, nil
}

func (repo *mockJobRepository) IterateQueueJobs(queueName string, action func(*api.Job)) error {
	return nil
}

func (repo *mockJobRepository) GetLeasedJobIds(queue string) ([]string, error) {
	return []string{}, nil
}

func (repo *mockJobRepository) UpdateStartTime(jobStartInfos []*repository.JobStartInfo) ([]error, error) {
	return nil, nil
}

func (repo *mockJobRepository) UpdateJobs(ids []string, mutator func([]*api.Job)) []repository.UpdateJobResult {
	return []repository.UpdateJobResult{}
}

func (repo *mockJobRepository) GetJobRunInfos(jobIds []string) (map[string]*repository.RunInfo, error) {
	return map[string]*repository.RunInfo{}, nil
}

type fakeQueueRepository struct{}

func (repo *fakeQueueRepository) GetAllQueues() ([]*api.Queue, error) {
	return []*api.Queue{}, nil
}

func (repo *fakeQueueRepository) GetQueue(name string) (*api.Queue, error) {
	return &api.Queue{}, nil
}

func (repo *fakeQueueRepository) CreateQueue(queue *api.Queue) error {
	return nil
}

func (repo *fakeQueueRepository) UpdateQueue(queue *api.Queue) error {
	return nil
}

func (repo *fakeQueueRepository) DeleteQueue(name string) error {
	return nil
}

type fakeUsageRepository struct{}

func (repo *fakeUsageRepository) GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error) {
	return map[string]*api.ClusterUsageReport{}, nil
}

func (repo *fakeUsageRepository) GetClusterPriority(clusterId string) (map[string]float64, error) {
	return map[string]float64{}, nil
}

func (repo *fakeUsageRepository) GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error) {
	return map[string]map[string]float64{}, nil
}

func (repo *fakeUsageRepository) GetClusterLeasedReports() (map[string]*api.ClusterLeasedReport, error) {
	return map[string]*api.ClusterLeasedReport{}, nil
}

func (repo *fakeUsageRepository) UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error {
	return nil
}

func (repo *fakeUsageRepository) UpdateClusterLeased(report *api.ClusterLeasedReport) error {
	return nil
}

type fakeEventStore struct {
	events []*api.EventMessage
}

func (es *fakeEventStore) ReportEvents(message []*api.EventMessage) error {
	es.events = append(es.events, message...)
	return nil
}

type fakeSchedulingInfoRepository struct{}

func (repo *fakeSchedulingInfoRepository) GetClusterSchedulingInfo() (map[string]*api.ClusterSchedulingInfoReport, error) {
	return map[string]*api.ClusterSchedulingInfoReport{}, nil
}

func (repo *fakeSchedulingInfoRepository) UpdateClusterSchedulingInfo(report *api.ClusterSchedulingInfoReport) error {
	return nil
}
