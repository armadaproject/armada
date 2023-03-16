package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
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

func TestAggregatedQueueServer_ReturnLeaseCallsSendsJobLeaseReturnedEvent(t *testing.T) {
	mockJobRepository, fakeEventStore, aggregatedQueueClient := makeAggregatedQueueServerWithTestDoubles(5)

	clusterId := "cluster-1"
	jobId := "job-id-1"
	jobSetId := "job-set-id-1"
	kubernetesId := "kubernetes-id"
	queueName := "queue-1"
	reason := "bad lease"
	job := &api.Job{
		Id:       jobId,
		JobSetId: jobSetId,
		Queue:    queueName,
	}

	_, addJobsErr := mockJobRepository.AddJobs([]*api.Job{job})
	assert.Nil(t, addJobsErr)

	_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId:    clusterId,
		JobId:        jobId,
		Reason:       reason,
		KubernetesId: kubernetesId,
	})

	assert.Nil(t, err)
	assert.Equal(t, 1, len(fakeEventStore.events))
	leaseReturnedEvent := fakeEventStore.events[0].GetLeaseReturned()
	assert.Equal(t, jobId, leaseReturnedEvent.JobId)
	assert.Equal(t, jobSetId, leaseReturnedEvent.JobSetId)
	assert.Equal(t, queueName, leaseReturnedEvent.Queue)
	assert.Equal(t, clusterId, leaseReturnedEvent.ClusterId)
	assert.Equal(t, kubernetesId, leaseReturnedEvent.KubernetesId)
	assert.Equal(t, reason, leaseReturnedEvent.Reason)
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
			ClusterId:       clusterId,
			JobId:           jobId,
			JobRunAttempted: true,
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
	queueName := "queue-1"
	job := &api.Job{
		Id:       jobId,
		JobSetId: jobSetId,
		Queue:    queueName,
	}

	_, addJobsErr := mockJobRepository.AddJobs([]*api.Job{job})
	assert.Nil(t, addJobsErr)

	for i := 0; i < maxRetries; i++ {
		_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
			ClusterId:       clusterId,
			JobId:           jobId,
			JobRunAttempted: true,
		})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(fakeEventStore.events))
		// Reset received events for each lease call
		fakeEventStore.events = []*api.EventMessage{}
	}

	_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId: clusterId,
		JobId:     jobId,
	})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(fakeEventStore.events))

	assert.NotNil(t, fakeEventStore.events[0].GetLeaseReturned())
	failedEvent := fakeEventStore.events[1].GetFailed()
	assert.Equal(t, jobId, failedEvent.JobId)
	assert.Equal(t, jobSetId, failedEvent.JobSetId)
	assert.Equal(t, queueName, failedEvent.Queue)
	assert.Equal(t, clusterId, failedEvent.ClusterId)
	assert.Equal(t, fmt.Sprintf("Exceeded maximum number of retries: %d", maxRetries), failedEvent.Reason)
}

func TestAggregatedQueueServer_ReturningLease_IncrementsRetries(t *testing.T) {
	mockJobRepository, _, aggregatedQueueClient := makeAggregatedQueueServerWithTestDoubles(uint(5))

	clusterId := "cluster-1"
	jobId := "job-id-1"
	jobSetId := "job-set-id-1"
	queueName := "queue-1"
	job := &api.Job{
		Id:       jobId,
		JobSetId: jobSetId,
		Queue:    queueName,
	}

	_, addJobsErr := mockJobRepository.AddJobs([]*api.Job{job})
	assert.Nil(t, addJobsErr)

	// Does not count towards retries if JobRunAttempted is false
	_, err := aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId:       clusterId,
		JobId:           jobId,
		JobRunAttempted: false,
	})
	assert.Nil(t, err)
	numberOfRetries, err := mockJobRepository.GetNumberOfRetryAttempts(jobId)
	assert.NoError(t, err)
	assert.Equal(t, 0, numberOfRetries)

	// Does count towards reties if JobRunAttempted is true
	_, err = aggregatedQueueClient.ReturnLease(context.TODO(), &api.ReturnLeaseRequest{
		ClusterId:       clusterId,
		JobId:           jobId,
		JobRunAttempted: true,
	})
	assert.NoError(t, err)
	numberOfRetries, err = mockJobRepository.GetNumberOfRetryAttempts(jobId)
	assert.NoError(t, err)
	assert.Equal(t, 1, numberOfRetries)
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
		fakeQueueRepository,
		&fakeUsageRepository{},
		fakeEventStore,
		fakeSchedulingInfoRepository,
		nil,
		0,
		fakeExecutorRepository{},
	)
}

type mockJobRepository struct {
	jobs       map[string]*api.Job
	jobRetries map[string]int

	returnLeaseCalls int
	deleteJobsCalls  int

	returnLeaseArg1 string
	returnLeaseArg2 string
	deleteJobsArg   []*api.Job

	jobStartTimeInfos       map[string]*repository.JobStartInfo
	updateJobStartTimeError error
	redisError              error
}

func (repo *mockJobRepository) StorePulsarSchedulerJobDetails(jobDetails []*schedulerobjects.PulsarSchedulerJobDetails) error {
	return nil
}

func (repo *mockJobRepository) GetPulsarSchedulerJobDetails(jobIds string) (*schedulerobjects.PulsarSchedulerJobDetails, error) {
	return nil, nil
}

func (repo *mockJobRepository) DeletePulsarSchedulerJobDetails(jobId []string) error {
	return nil
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobs:              make(map[string]*api.Job),
		jobRetries:        make(map[string]int),
		returnLeaseCalls:  0,
		deleteJobsCalls:   0,
		returnLeaseArg1:   "",
		returnLeaseArg2:   "",
		deleteJobsArg:     nil,
		jobStartTimeInfos: map[string]*repository.JobStartInfo{},
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

func (repo *mockJobRepository) GetJobsByIds(ids []string) ([]*repository.JobResult, error) {
	jobResults := make([]*repository.JobResult, 0)
	for _, id := range ids {
		if job, ok := repo.jobs[id]; ok {
			jobResults = append(jobResults, &repository.JobResult{JobId: job.Id, Job: job})
		}
	}
	return jobResults, nil
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

func (repo *mockJobRepository) ExpireLeasesById(jobIds []string, deadline time.Time) (expired []*api.Job, e error) {
	return []*api.Job{}, nil
}

func (repo *mockJobRepository) ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error) {
	repo.returnLeaseCalls++
	repo.returnLeaseArg1 = clusterId
	repo.returnLeaseArg2 = jobId
	return nil, nil
}

func (repo *mockJobRepository) DeleteJobs(jobs []*api.Job) (map[*api.Job]error, error) {
	repo.deleteJobsCalls++
	repo.deleteJobsArg = jobs

	for _, job := range jobs {
		delete(repo.jobs, job.Id)
	}

	return map[*api.Job]error{}, nil
}

func (repo *mockJobRepository) GetActiveJobIds(queue string, jobSetId string) ([]string, error) {
	return []string{}, nil
}

func (repo *mockJobRepository) GetJobSetJobIds(queue string, jobSetId string, filter *repository.JobSetFilter) ([]string, error) {
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

func (repo *mockJobRepository) TryLeaseJobs(clusterId string, jobIdsByQueue map[string][]string) (map[string][]string, error) {
	return make(map[string][]string), nil
}

func (repo *mockJobRepository) GetLeasedJobIds(queue string) ([]string, error) {
	return []string{}, nil
}

func (repo *mockJobRepository) UpdateStartTime(jobStartInfos []*repository.JobStartInfo) ([]error, error) {
	if repo.redisError != nil {
		return nil, repo.redisError
	}
	if repo.updateJobStartTimeError != nil {
		errors := make([]error, 0, len(jobStartInfos))
		errors = append(errors, repo.updateJobStartTimeError)
		return errors, nil
	}
	errors := make([]error, 0, len(jobStartInfos))
	for _, startInfo := range jobStartInfos {
		repo.jobStartTimeInfos[startInfo.JobId] = startInfo
		errors = append(errors, nil)
	}
	return errors, nil
}

func (repo *mockJobRepository) UpdateJobs(ids []string, mutator func([]*api.Job)) ([]repository.UpdateJobResult, error) {
	return []repository.UpdateJobResult{}, nil
}

func (repo *mockJobRepository) GetJobRunInfos(jobIds []string) (map[string]*repository.RunInfo, error) {
	return map[string]*repository.RunInfo{}, nil
}

type fakeQueueRepository struct{}

func (repo *fakeQueueRepository) GetAllQueues() ([]queue.Queue, error) {
	return []queue.Queue{}, nil
}

func (repo *fakeQueueRepository) GetQueue(name string) (queue.Queue, error) {
	return queue.Queue{}, nil
}

func (repo *fakeQueueRepository) CreateQueue(queue queue.Queue) error {
	return nil
}

func (repo *fakeQueueRepository) UpdateQueue(queue queue.Queue) error {
	return nil
}

func (repo *fakeQueueRepository) DeleteQueue(name string) error {
	return nil
}

type fakeUsageRepository struct{}

func (repo *fakeUsageRepository) GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error) {
	return map[string]*api.ClusterUsageReport{}, nil
}

func (repo *fakeUsageRepository) GetClusterQueueResourceUsage() (map[string]*schedulerobjects.ClusterResourceUsageReport, error) {
	return nil, nil
}

func (repo *fakeUsageRepository) UpdateClusterQueueResourceUsage(cluster string, resourceUsage *schedulerobjects.ClusterResourceUsageReport) error {
	return nil
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

func (es *fakeEventStore) ReportEvents(_ context.Context, message []*api.EventMessage) error {
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

type fakeExecutorRepository struct{}

func (f fakeExecutorRepository) GetExecutors(ctx context.Context) ([]*schedulerobjects.Executor, error) {
	return nil, nil
}

func (f fakeExecutorRepository) GetLastUpdateTimes(ctx context.Context) (map[string]time.Time, error) {
	return nil, nil
}

func (f fakeExecutorRepository) StoreExecutor(ctx context.Context, executor *schedulerobjects.Executor) error {
	return nil
}
