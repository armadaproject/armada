package convert

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/compress"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"

	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

const (
	jobset      = "testJobset"
	queue       = "testQueue"
	jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString = "123e4567-e89b-12d3-a456-426614174000"
)

var (
	jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)
	runIdProto    = armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))
)

var baseTime, _ = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")

// Succeeded
var jobRunSucceeded = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
		JobRunSucceeded: &armadaevents.JobRunSucceeded{
			RunId: runIdProto,
			JobId: jobIdProto,
		},
	},
}

// Cancelled
var cancelled = &armadaevents.EventSequence_Event{
	Created: &baseTime,
	Event: &armadaevents.EventSequence_Event_CancelledJob{
		CancelledJob: &armadaevents.CancelledJob{
			JobId: jobIdProto,
		},
	},
}

func TestSingle(t *testing.T) {
	msg := NewMsg(baseTime, jobRunSucceeded)
	compressor, err := compress.NewZlibCompressor(0)
	assert.NoError(t, err)
	converter := MessageRowConverter{Compressor: compressor, MaxMessageBatchSize: 1024}
	batchUpdate := converter.ConvertBatch(context.Background(), []*pulsarutils.ConsumerMessage{msg})
	expectedSequence := armadaevents.EventSequence{
		Events: []*armadaevents.EventSequence_Event{jobRunSucceeded},
	}
	assert.Equal(t, []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}}, batchUpdate.MessageIds)
	assert.Equal(t, 1, len(batchUpdate.Events))
	event := batchUpdate.Events[0]
	assert.Equal(t, queue, event.Queue)
	assert.Equal(t, jobset, event.Jobset)
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	assert.Equal(t, expectedSequence.Events, es.Events)
}

func TestSingleWithMissingCreated(t *testing.T) {
	// Succeeded
	suceededMissingCreated := &armadaevents.EventSequence_Event{
		// No created time
		Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
			JobRunSucceeded: &armadaevents.JobRunSucceeded{
				RunId: runIdProto,
				JobId: jobIdProto,
			},
		},
	}

	msg := NewMsg(baseTime, suceededMissingCreated)
	compressor, err := compress.NewZlibCompressor(0)
	assert.NoError(t, err)
	converter := MessageRowConverter{Compressor: compressor, MaxMessageBatchSize: 1024}
	batchUpdate := converter.ConvertBatch(context.Background(), []*pulsarutils.ConsumerMessage{msg})
	expectedSequence := armadaevents.EventSequence{
		Events: []*armadaevents.EventSequence_Event{jobRunSucceeded},
	}
	assert.Equal(t, []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}}, batchUpdate.MessageIds)
	event := batchUpdate.Events[0]
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	assert.Equal(t, expectedSequence.Events, es.Events)
}

func TestMultiple(t *testing.T) {
	msg := NewMsg(baseTime, cancelled, jobRunSucceeded)
	compressor, err := compress.NewZlibCompressor(0)
	assert.NoError(t, err)
	converter := MessageRowConverter{Compressor: compressor, MaxMessageBatchSize: 1024}
	batchUpdate := converter.ConvertBatch(context.Background(), []*pulsarutils.ConsumerMessage{msg})
	expectedSequence := armadaevents.EventSequence{
		Events: []*armadaevents.EventSequence_Event{cancelled, jobRunSucceeded},
	}
	assert.Equal(t, []*pulsarutils.ConsumerMessageId{{msg.Message.ID(), 0, msg.ConsumerId}}, batchUpdate.MessageIds)
	assert.Equal(t, 1, len(batchUpdate.Events))
	event := batchUpdate.Events[0]
	assert.Equal(t, queue, event.Queue)
	assert.Equal(t, jobset, event.Jobset)
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	assert.Equal(t, expectedSequence.Events, es.Events)
}

func TestMultipleMessages(t *testing.T) {
	msg1 := NewMsg(baseTime, cancelled)
	msg2 := NewMsg(baseTime, jobRunSucceeded)
	compressor, err := compress.NewZlibCompressor(0)
	assert.NoError(t, err)
	converter := MessageRowConverter{Compressor: compressor, MaxMessageBatchSize: 1024}
	batchUpdate := converter.ConvertBatch(context.Background(), []*pulsarutils.ConsumerMessage{msg1, msg2})
	expectedSequence := armadaevents.EventSequence{
		Events: []*armadaevents.EventSequence_Event{cancelled, jobRunSucceeded},
	}
	assert.Equal(t, []*pulsarutils.ConsumerMessageId{
		{msg1.Message.ID(), 0, msg1.ConsumerId},
		{msg2.Message.ID(), 0, msg2.ConsumerId},
	}, batchUpdate.MessageIds)
	assert.Equal(t, 1, len(batchUpdate.Events))
	event := batchUpdate.Events[0]
	assert.Equal(t, queue, event.Queue)
	assert.Equal(t, jobset, event.Jobset)
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	assert.Equal(t, expectedSequence.Events, es.Events)
}

func NewMsg(publishTime time.Time, event ...*armadaevents.EventSequence_Event) *pulsarutils.ConsumerMessage {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobset,
		Events:     event,
	}
	payload, _ := proto.Marshal(seq)
	messageSeq := rand.Int()
	return &pulsarutils.ConsumerMessage{Message: pulsarutils.NewPulsarMessage(messageSeq, publishTime, payload), ConsumerId: messageSeq}
}

func extractEventSeq(b []byte) (*armadaevents.EventSequence, error) {
	decompressor, err := compress.NewZlibDecompressor()
	if err != nil {
		return nil, err
	}
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return nil, err
	}
	es := &armadaevents.EventSequence{}
	err = proto.Unmarshal(decompressed, es)
	if err != nil {
		return nil, err
	}
	return es, nil
}
