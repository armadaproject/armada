package repository

import (
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/armada/repository/sequence"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString = "123e4567-e89b-12d3-a456-426614174000"
)

var (
	jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)
	runIdProto    = armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))
	baseTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
)

const (
	jobSetName = "testJobset"
	testQueue  = "test-queue"
	executorId = "testCluster"
	nodeName   = "testNode"
	podName    = "test-pod"
)

const (
	namespace = "test-ns"
	podNumber = 6
)

// Assigned
var assigned = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_JobRunAssigned{
		JobRunAssigned: &armadaevents.JobRunAssigned{
			RunId: runIdProto,
			JobId: jobIdProto,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					ObjectMeta: &armadaevents.ObjectMeta{
						KubernetesId: runIdString,
						Name:         podName,
						Namespace:    namespace,
						ExecutorId:   executorId,
					},
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							PodNumber: podNumber,
						},
					},
				},
			},
		},
	},
}

// Running
var running = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_JobRunRunning{
		JobRunRunning: &armadaevents.JobRunRunning{
			RunId: runIdProto,
			JobId: jobIdProto,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							NodeName:  nodeName,
							PodNumber: podNumber,
						},
					},
				},
			},
		},
	},
}

var runSucceeded = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
		JobRunSucceeded: &armadaevents.JobRunSucceeded{
			RunId: runIdProto,
			JobId: jobIdProto,
			ResourceInfos: []*armadaevents.KubernetesResourceInfo{
				{
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							NodeName:  nodeName,
							PodNumber: podNumber,
						},
					},
				},
			},
		},
	},
}

var expectedPending = api.EventMessage{
	Events: &api.EventMessage_Pending{
		Pending: &api.JobPendingEvent{
			JobId:        jobIdString,
			JobSetId:     jobSetName,
			Queue:        testQueue,
			Created:      baseTime,
			ClusterId:    executorId,
			KubernetesId: runIdString,
			PodNumber:    podNumber,
			PodName:      podName,
			PodNamespace: namespace,
		},
	},
}

var expectedRunning = api.EventMessage{
	Events: &api.EventMessage_Running{
		Running: &api.JobRunningEvent{
			JobId:        jobIdString,
			JobSetId:     jobSetName,
			Queue:        testQueue,
			Created:      baseTime,
			ClusterId:    executorId,
			KubernetesId: runIdString,
			NodeName:     nodeName,
			PodNumber:    podNumber,
			PodName:      podName,
			PodNamespace: namespace,
		},
	},
}

func TestRead(t *testing.T) {
	withRedisEventRepository(func(r *RedisEventRepository) {
		err := storeEvents(r, assigned, running)
		assert.NoError(t, err)

		// Fetch from beginning
		events, lastMessageId, err := r.ReadEvents(testQueue, jobSetName, "", 500, 1*time.Second)
		assert.NoError(t, err)
		assertExpected(t, events, lastMessageId, &expectedPending, &expectedRunning)

		// Fetch from offset in the middle
		offset := events[0].Id
		events, lastMessageId, err = r.ReadEvents(testQueue, jobSetName, offset, 500, 1*time.Second)
		assert.NoError(t, err)
		assertExpected(t, events, lastMessageId, &expectedRunning)

		// Fetch from offset after
		offset = events[0].Id
		events, lastMessageId, err = r.ReadEvents(testQueue, jobSetName, offset, 500, 1*time.Second)
		assert.NoError(t, err)
		assert.Nil(t, lastMessageId)
		assert.Equal(t, 0, len(events))

		// Fetch for events that won't produce api events
		// JobRunSucceeded doesn't result in an api event, so expect:
		// - No events
		// - Last message Id to be non-nil
		err = storeEvents(r, runSucceeded)
		assert.NoError(t, err)
		offSetId, err := sequence.Parse(offset)
		assert.NoError(t, err)
		events, lastMessageId, err = r.ReadEvents(testQueue, jobSetName, offSetId.String(), 500, 1*time.Second)
		assert.NoError(t, err)
		assert.NotNil(t, lastMessageId)
		assert.True(t, lastMessageId.IsAfter(offSetId))
		assert.Equal(t, 0, len(events))
	})

}

func TestGetLastId(t *testing.T) {
	withRedisEventRepository(func(r *RedisEventRepository) {
		// Event doesn't exist- should be "0"
		retrievedLastId, err := r.GetLastMessageId(testQueue, jobSetName)
		assert.NoError(t, err)
		assert.Equal(t, "0", retrievedLastId)

		// Now create the stream and fetch the events to manually determine the last id
		err = storeEvents(r, assigned, running)
		assert.NoError(t, err)
		events, _, err := r.ReadEvents(testQueue, jobSetName, "", 500, 1*time.Second)
		assert.NoError(t, err)
		actualLastId := events[1].Id

		// Assert that the test id matches
		retrievedLastId, err = r.GetLastMessageId(testQueue, jobSetName)
		assert.NoError(t, err)
		assert.Equal(t, actualLastId, retrievedLastId)
	})
}

func TestStreamExists(t *testing.T) {
	withRedisEventRepository(func(r *RedisEventRepository) {
		exists, err := r.CheckStreamExists(testQueue, jobSetName)
		assert.NoError(t, err)
		assert.False(t, exists)

		err = storeEvents(r, assigned, running)
		assert.NoError(t, err)

		exists, err = r.CheckStreamExists(testQueue, jobSetName)
		assert.NoError(t, err)
		assert.True(t, exists)
	})
}

func withRedisEventRepository(action func(r *RedisEventRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := NewEventRepository(client)
	action(repo)
}

func assertExpected(t *testing.T, actual []*api.EventStreamMessage, lastMessageId *sequence.ExternalSeqNo, expected ...*api.EventMessage) {
	assert.Equal(t, len(actual), len(expected))

	for i, streamMessage := range expected {
		assert.Equal(t, expected[i].Events, streamMessage.Events)
	}
	assert.Equal(t, actual[len(actual)-1].Id, lastMessageId.String())
}

func storeEvents(r *RedisEventRepository, events ...*armadaevents.EventSequence_Event) error {
	// create an eventSequence
	es := &armadaevents.EventSequence{Events: events}

	bytes, err := proto.Marshal(es)
	if err != nil {
		return err
	}
	compressor, err := compress.NewZlibCompressor(0)
	if err != nil {
		return err
	}
	compressed, err := compressor.Compress(bytes)
	if err != nil {
		return err
	}

	r.db.XAdd(&redis.XAddArgs{
		Stream: eventStreamPrefix + testQueue + ":" + jobSetName,
		Values: map[string]interface{}{
			dataKey: compressed,
		},
	})

	return nil
}
