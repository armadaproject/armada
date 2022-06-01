package processor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/eventstream"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func TestHandleMessage_JobRunningEvent(t *testing.T) {
	acked := false
	runningEventMessage := createJobRunningEventStreamMessage(
		util.NewULID(), "queue", "jobset", "clusterId",
		func() error {
			acked = true
			return nil
		})

	mockBatcher := &mockEventBatcher{}
	processor := NewEventJobStatusProcessor("test", &repository.RedisJobRepository{}, &eventstream.JetstreamEventStream{}, mockBatcher)
	err := processor.handleMessage(runningEventMessage)

	assert.NoError(t, err)
	assert.False(t, acked)
	assert.Len(t, mockBatcher.events, 1)
	assert.Equal(t, mockBatcher.events[0], runningEventMessage)
}

func TestHandleMessage_NonJobRunningEvent(t *testing.T) {
	acked := false
	leasedEventMessage := createJobLeasedEventStreamMessage(
		func() error {
			acked = true
			return nil
		})

	mockBatcher := &mockEventBatcher{}
	processor := NewEventJobStatusProcessor("test", &repository.RedisJobRepository{}, &eventstream.JetstreamEventStream{}, mockBatcher)
	err := processor.handleMessage(leasedEventMessage)

	assert.NoError(t, err)
	assert.True(t, acked)
	assert.Len(t, mockBatcher.events, 0)
}

func TestHandleBatch_NonJobRunningEvent(t *testing.T) {
	withEventStatusProcess(newMockJobRepository(), func(processor *EventJobStatusProcessor) {
		acked := false
		leasedEventMessage := createJobLeasedEventStreamMessage(
			func() error {
				acked = true
				return nil
			})

		err := processor.handleBatch([]*eventstream.Message{leasedEventMessage})
		assert.NoError(t, err)
		assert.True(t, acked)
	})
}

func TestHandleBatch_OnJobRunningEvent_UpdatesJobStartTime(t *testing.T) {
	jobRepo := newMockJobRepository()
	withEventStatusProcess(jobRepo, func(processor *EventJobStatusProcessor) {
		acked := false
		runningEventMessage := createJobRunningEventStreamMessage(
			"jobId", "queue", "jobSetId", "clusterId",
			func() error {
				acked = true
				return nil
			})

		err := processor.handleBatch([]*eventstream.Message{runningEventMessage})
		assert.NoError(t, err)
		assert.True(t, acked)

		jobRunInfo, exists := jobRepo.jobStartTimeInfos["jobId"]
		assert.True(t, exists)
		assert.Equal(t, runningEventMessage.EventMessage.GetRunning().Created.UTC(), jobRunInfo.StartTime.UTC())
	})
}

func TestHandleBatch_OnJobRunningEvent_NonExistentJob(t *testing.T) {
	jobRepo := newMockJobRepository()
	jobRepo.updateJobStartTimeError = &repository.ErrJobNotFound{JobId: "jobId", ClusterId: "clusterId"}
	withEventStatusProcess(jobRepo, func(processor *EventJobStatusProcessor) {
		acked := false
		runningEventMessage := createJobRunningEventStreamMessage(
			"jobId", "queue", "jobset", "clusterId",
			func() error {
				acked = true
				return nil
			})

		err := processor.handleBatch([]*eventstream.Message{runningEventMessage})
		assert.NoError(t, err)
		assert.True(t, acked)
	})
}

func TestHandleBatch_OnJobRunningEvent_JobError(t *testing.T) {
	jobRepo := newMockJobRepository()
	jobRepo.updateJobStartTimeError = fmt.Errorf("Job update error")
	withEventStatusProcess(jobRepo, func(processor *EventJobStatusProcessor) {
		acked := false
		runningEventMessage := createJobRunningEventStreamMessage(
			util.NewULID(), "queue", "jobset", "clusterId",
			func() error {
				acked = true
				return nil
			})

		err := processor.handleBatch([]*eventstream.Message{runningEventMessage})
		assert.NoError(t, err)
		assert.False(t, acked)
	})
}

func TestHandleBatch_OnJobRunningEvent_RedisError(t *testing.T) {
	jobRepo := newMockJobRepository()
	jobRepo.redisError = fmt.Errorf("Redis error")
	withEventStatusProcess(jobRepo, func(processor *EventJobStatusProcessor) {
		acked := false
		runningEventMessage := createJobRunningEventStreamMessage(
			util.NewULID(), "queue", "jobset", "clusterId",
			func() error {
				acked = true
				return nil
			})

		err := processor.handleBatch([]*eventstream.Message{runningEventMessage})
		assert.Error(t, err)
		assert.False(t, acked)
	})
}

func createJobLeasedEventStreamMessage(ackFunction eventstream.AckFn) *eventstream.Message {
	eventMessage := &api.EventMessage{
		Events: &api.EventMessage_Leased{
			Leased: &api.JobLeasedEvent{
				JobId:    util.NewULID(),
				JobSetId: "jobset",
				Queue:    "test",
			},
		},
	}
	return &eventstream.Message{
		EventMessage: eventMessage,
		Ack:          ackFunction,
	}
}

func createJobRunningEventStreamMessage(jobId string, queue string, jobSetId string, clusterId string, ackFunction eventstream.AckFn) *eventstream.Message {
	eventMessage := &api.EventMessage{
		Events: &api.EventMessage_Running{
			Running: &api.JobRunningEvent{
				JobId:     jobId,
				JobSetId:  jobSetId,
				Queue:     queue,
				ClusterId: clusterId,
				Created:   time.Now(),
			},
		},
	}

	return &eventstream.Message{
		EventMessage: eventMessage,
		Ack:          ackFunction,
	}
}

func withEventStatusProcess(jobRepository repository.JobRepository, action func(processor *EventJobStatusProcessor)) {
	processor := NewEventJobStatusProcessor("test", jobRepository, &eventstream.JetstreamEventStream{}, &eventstream.TimedEventBatcher{})
	action(processor)
}

type mockEventBatcher struct {
	events []*eventstream.Message
}

func (b *mockEventBatcher) Register(callback eventstream.EventBatchCallback) {
}

func (b *mockEventBatcher) Report(event *eventstream.Message) error {
	b.events = append(b.events, event)
	return nil
}

func (b *mockEventBatcher) Stop() error {
	return nil
}

type mockJobRepository struct {
	jobStartTimeInfos       map[string]*repository.JobStartInfo
	updateJobStartTimeError error
	redisError              error
}

func newMockJobRepository() *mockJobRepository {
	return &mockJobRepository{
		jobStartTimeInfos: make(map[string]*repository.JobStartInfo),
	}
}

func (repo *mockJobRepository) GetQueueJobIds(queueName string) ([]string, error) {
	return []string{}, nil
}

func (repo *mockJobRepository) CreateJobs(request *api.JobSubmitRequest, owner string, ownershipGroups []string) ([]*api.Job, error) {
	return []*api.Job{}, nil
}

func (repo *mockJobRepository) AddJobs(job []*api.Job) ([]*repository.SubmitJobResult, error) {
	return []*repository.SubmitJobResult{}, nil
}

func (repo *mockJobRepository) GetExistingJobsByIds(ids []string) ([]*api.Job, error) {
	return []*api.Job{}, nil
}

func (repo *mockJobRepository) GetJobsByIds(ids []string) ([]*repository.JobResult, error) {
	return []*repository.JobResult{}, nil
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
	return nil, nil
}

func (repo *mockJobRepository) DeleteJobs(jobs []*api.Job) (map[*api.Job]error, error) {
	return map[*api.Job]error{}, nil
}

func (repo *mockJobRepository) GetActiveJobIds(queue string, jobSetId string) ([]string, error) {
	return []string{}, nil
}

func (repo *mockJobRepository) GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error) {
	return []*api.JobSetInfo{}, nil
}

func (repo *mockJobRepository) AddRetryAttempt(jobId string) error {
	return nil
}

func (repo *mockJobRepository) GetNumberOfRetryAttempts(jobId string) (int, error) {
	return 0, nil
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
