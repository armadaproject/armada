package server

import (
	"context"
	"fmt"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/armada/authorization"
)

func TestAggregatedQueueServer_ReturnLeaseCallsTheRepositoryMethod(t *testing.T) {
	mockJobRepository, aggregatedQueueClient := makeAggregatedQueueServerWithTestDoubles()

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

func TestAggregatedQueueServer_ReturnLeaseCalledMoreThanFiveTimesDeletesTheJob(t *testing.T) {
	mockJobRepository, aggregatedQueueClient := makeAggregatedQueueServerWithTestDoubles()

	clusterId := "cluster-1"
	jobId := "job-id-1"
	job := &api.Job{Id: jobId}

	_, addJobsErr := mockJobRepository.AddJobs([]*api.Job{job})
	assert.Nil(t, addJobsErr)

	for i := 0; i < 5; i++ {
		_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
			ClusterId: clusterId,
			JobId:     jobId,
		})
		assert.Nil(t, err)

		assert.Equal(t, i + 1, mockJobRepository.returnLeaseCalls)
		assert.Equal(t, clusterId, mockJobRepository.returnLeaseArg1)
		assert.Equal(t, jobId, mockJobRepository.returnLeaseArg2)
	}

	_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId: clusterId,
		JobId:     jobId,
	})
	assert.Nil(t, err)
	assert.Equal(t, 5, mockJobRepository.returnLeaseCalls)

	assert.Equal(t, 1, mockJobRepository.deleteJobsCalls)
	assert.Equal(t, []*api.Job{job}, mockJobRepository.deleteJobsArg)
}

func makeAggregatedQueueServerWithTestDoubles() (*mockJobRepository, *AggregatedQueueServer) {
	mockJobRepository := newMockJobRepository()
	return mockJobRepository, NewAggregatedQueueServer(
		&FakePermissionChecker{},
		configuration.SchedulingConfig{},
		mockJobRepository,
		&fakeQueueRepository{},
		&fakeUsageRepository{},
		&fakeEventStore{},
		&fakeSchedulingInfoRepository{})
}

type mockJobRepository struct{
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

func (repo *mockJobRepository) CreateJobs(request *api.JobSubmitRequest, principal authorization.Principal) ([]*api.Job, error) {
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

type fakeEventStore struct{}

func (es *fakeEventStore) ReportEvents(message []*api.EventMessage) error {
	return nil
}

type fakeSchedulingInfoRepository struct{}

func (repo *fakeSchedulingInfoRepository) GetClusterSchedulingInfo() (map[string]*api.ClusterSchedulingInfoReport, error) {
	return map[string]*api.ClusterSchedulingInfoReport{}, nil
}

func (repo *fakeSchedulingInfoRepository) UpdateClusterSchedulingInfo(report *api.ClusterSchedulingInfoReport) error {
	return nil
}
