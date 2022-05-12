package serving

import (
	"bytes"
	"compress/zlib"
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/eventapi"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type testEventContext struct {
	grpc.ServerStream
	eventChannel        chan []*model.EventRow
	activeSubscriptions int
	messagesSent        []*api.EventStreamMessage
	SequenceManager     *DefaultSequenceManager
}

func (ec *testEventContext) Subscribe(jobset int64, fromOffset int64) *model.EventSubscription {
	ec.activeSubscriptions++
	return &model.EventSubscription{
		SubscriptionId: 1,
		Channel:        ec.eventChannel,
	}
}

func (ec *testEventContext) Unsubscribe(subscriptionId int64) {
	ec.activeSubscriptions--
}

func (ec *testEventContext) Send(m *api.EventStreamMessage) error {
	ec.messagesSent = append(ec.messagesSent, m)
	return nil
}

func (ec *testEventContext) Context() context.Context {
	return context.Background()
}

const jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"
const queue = "test-queue"
const jobset = "test-jobset"
const user = "test-user"

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
var jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)
var cancelledEvent = &armadaevents.CancelledJob{JobId: jobIdProto}
var expectedApiEvent = &api.EventMessage_Cancelled{
	Cancelled: &api.JobCancelledEvent{
		JobId:     jobIdString,
		JobSetId:  jobset,
		Queue:     queue,
		Created:   baseTime,
		Requestor: user,
	},
}
var jobsetMapper = &eventapi.StaticJobsetMapper{JobsetIds: map[string]int64{"test-queue:test-jobset": 1}}

// Test that if you ask for events and there are no events available then you return immediately
func TestGetEvents_WithNoEventAvailable(t *testing.T) {
	withEventApi(func(eventApi *EventApi, context *testEventContext) {
		request := &api.JobSetRequest{Id: jobset, Watch: false, Queue: queue}
		err := eventApi.GetJobSetEvents(request, context)
		assert.NoError(t, err)
		assert.Equal(t, []*api.EventStreamMessage(nil), context.messagesSent)
		assert.Equal(t, 0, context.activeSubscriptions)
	})
}

// Test that if you ask for events and the FromMessageId is after the last available event, we return immediately
func TestGetEvents_WithLatestEventAfterBeginSeq(t *testing.T) {
	withEventApi(func(eventApi *EventApi, context *testEventContext) {
		context.SequenceManager.Update(map[int64]int64{1: 1})
		request := &api.JobSetRequest{Id: jobset, Watch: false, Queue: queue, FromMessageId: "2:0"}
		err := eventApi.GetJobSetEvents(request, context)
		assert.NoError(t, err)
		assert.Equal(t, []*api.EventStreamMessage(nil), context.messagesSent)
		assert.Equal(t, 0, context.activeSubscriptions)
	})
}

// Test that if you ask for events and there is a single message available, that message is returned
func TestGetEvents_WithFromMessageId(t *testing.T) {
	withEventApi(func(eventApi *EventApi, context *testEventContext) {
		context.SequenceManager.Update(map[int64]int64{1: 1})
		request := &api.JobSetRequest{Id: jobset, Watch: false, Queue: queue}
		sequence := &armadaevents.EventSequence{
			UserId: user,
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: &baseTime,
					Event: &armadaevents.EventSequence_Event_CancelledJob{
						CancelledJob: cancelledEvent,
					},
				},
			},
		}
		marshalledBytes, _ := CreateDbMessage(sequence)
		context.eventChannel <- []*model.EventRow{{JobSetId: 1, SeqNo: 1, Event: marshalledBytes}}
		err := eventApi.GetJobSetEvents(request, context)
		assert.NoError(t, err)
		expectedMessage := &api.EventStreamMessage{
			Id: "1:0",
			Message: &api.EventMessage{
				Events: expectedApiEvent,
			},
		}
		assert.Equal(t, expectedMessage, context.messagesSent[0])
		assert.Equal(t, 0, context.activeSubscriptions)
	})
}

// Test that if you ask for events, you don't get events before you asked for
func TestGetEvents_WithEventAvailable(t *testing.T) {
	withEventApi(func(eventApi *EventApi, context *testEventContext) {
		context.SequenceManager.Update(map[int64]int64{1: 2})
		request := &api.JobSetRequest{Id: jobset, Watch: false, Queue: queue, FromMessageId: "1:0"}
		sequence := &armadaevents.EventSequence{
			UserId: user,
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: &baseTime,
					Event: &armadaevents.EventSequence_Event_CancelledJob{
						CancelledJob: cancelledEvent,
					},
				},
			},
		}
		marshalledBytes, _ := CreateDbMessage(sequence)
		context.eventChannel <- []*model.EventRow{
			{JobSetId: 1, SeqNo: 1, Event: marshalledBytes},
			{JobSetId: 1, SeqNo: 2, Event: marshalledBytes},
		}
		err := eventApi.GetJobSetEvents(request, context)
		assert.NoError(t, err)
		expectedMessage := []*api.EventStreamMessage{
			{
				Id: "2:0",
				Message: &api.EventMessage{
					Events: expectedApiEvent,
				},
			},
		}
		assert.Equal(t, expectedMessage, context.messagesSent)
		assert.Equal(t, 0, context.activeSubscriptions)
	})
}

func CreateDbMessage(sequence *armadaevents.EventSequence) ([]byte, error) {
	dbEvent := &armadaevents.DatabaseEvent{
		EventSequence: sequence,
	}
	payload, err := proto.Marshal(dbEvent)
	if err != nil {
		return nil, err
	}
	return compressBytes(payload)
}

func compressBytes(s []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(s)
	w.Close()
	return b.Bytes(), err
}
func withEventApi(action func(eventApi *EventApi, context *testEventContext)) {
	context := &testEventContext{
		eventChannel:        make(chan []*model.EventRow, 10),
		activeSubscriptions: 0,
		SequenceManager:     &DefaultSequenceManager{sequences: make(map[int64]int64)},
	}
	eventApi := NewEventApi(jobsetMapper, context, context.SequenceManager)
	action(eventApi, context)
}
