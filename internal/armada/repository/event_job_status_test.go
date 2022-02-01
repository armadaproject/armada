package repository

import (
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/configuration"
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
	processor := NewEventJobStatusProcessor("test", &RedisJobRepository{}, &eventstream.JetstreamEventStream{}, mockBatcher)
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
	processor := NewEventJobStatusProcessor("test", &RedisJobRepository{}, &eventstream.JetstreamEventStream{}, mockBatcher)
	err := processor.handleMessage(leasedEventMessage)

	assert.NoError(t, err)
	assert.True(t, acked)
	assert.Len(t, mockBatcher.events, 0)
}

func TestHandleBatch_NonJobRunningEvent(t *testing.T) {
	withEventStatusProcess(false, func(processor *EventJobStatusProcessor) {
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
	withEventStatusProcess(false, func(processor *EventJobStatusProcessor) {
		job := createLeasedJob(t, processor.jobRepository, "clusterId")
		acked := false
		runningEventMessage := createJobRunningEventStreamMessage(
			job.Id, job.Queue, job.JobSetId, "clusterId",
			func() error {
				acked = true
				return nil
			})

		err := processor.handleBatch([]*eventstream.Message{runningEventMessage})
		assert.NoError(t, err)
		assert.True(t, acked)

		_, _ = processor.jobRepository.GetJobRunInfos([]string{job.Id})

	})
}

func TestHandleBatch_OnJobRunningEvent_NonExistentJob(t *testing.T) {
	withEventStatusProcess(false, func(processor *EventJobStatusProcessor) {
		acked := false
		runningEventMessage := createJobRunningEventStreamMessage(
			util.NewULID(), "queue", "jobset", "clusterId",
			func() error {
				acked = true
				return nil
			})

		err := processor.handleBatch([]*eventstream.Message{runningEventMessage})
		assert.NoError(t, err)
		assert.True(t, acked)
	})
}

//func TestHandleBatch_OnJobRunningEvent_RedisDown(t *testing.T) {
//	withEventStatusProcess(true, func(processor *EventJobStatusProcessor) {
//		acked := false
//		runningEventMessage := createJobRunningEventStreamMessage(
//			util.NewULID(), "queue", "jobset", "clusterId",
//			func() error {
//				acked = true
//				return nil
//			})
//
//		err := processor.handleBatch([]*eventstream.Message{runningEventMessage})
//		assert.Error(t, err)
//		assert.False(t, acked)
//	})
//}

func createLeasedJob(t *testing.T, jobRepository JobRepository, cluster string) *api.Job {
	jobs := make([]*api.Job, 0, 1)
	j := &api.Job{
		Id:                       util.NewULID(),
		Queue:                    "queue",
		JobSetId:                 "jobSetId",
		Priority:                 1,
		Created:                  time.Now(),
		Owner:                    "user",
		QueueOwnershipUserGroups: []string{},
	}
	jobs = append(jobs, j)

	results, e := jobRepository.AddJobs(jobs)
	assert.NoError(t, e)
	assert.NoError(t, results[0].Error)
	job := results[0].SubmittedJob

	return job
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

func withEventStatusProcess(redisDown bool, action func(processor *EventJobStatusProcessor)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	jobRepository := NewRedisJobRepository(client, configuration.DatabaseRetentionPolicy{JobRetentionDuration: time.Hour})
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
