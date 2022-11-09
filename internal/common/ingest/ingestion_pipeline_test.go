package ingest

import (
	"context"
	"github.com/gogo/protobuf/proto"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/G-Research/armada/pkg/armadaevents"

	"github.com/apache/pulsar-client-go/pulsar"
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

type mockPulsarConsumer struct {
	messages   []pulsar.Message
	messageIdx int
	acked      []pulsar.MessageID
	pulsar.Consumer
}

func (p *mockPulsarConsumer) Receive(ctx context.Context) (pulsar.Message, error) {
	if p.messageIdx < len(p.messages) {
		msg := p.messages[p.messageIdx]
		p.messageIdx++
		return msg, nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil, context.DeadlineExceeded
		}
	}
}

func (p *mockPulsarConsumer) AckID(pulsar.MessageID) {
	// do nothing
}

func (p *mockPulsarConsumer) Close() {
	// do nothing
}

func TestRun_HappyPath(t *testing.T) {
	es := &armadaevents.EventSequence{
		Queue:      "test",
		JobSetName: "test",
		UserId:     "chrisma",
		Events: []*armadaevents.EventSequence_Event{
			{
				Created: &baseTime,
				Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
					JobRunSucceeded: &armadaevents.JobRunSucceeded{
						RunId: runIdProto,
						JobId: jobIdProto,
					},
				},
			},
		},
	}

	proto.Marshal(es)

	var mockConsumer pulsar.Consumer = mockPulsarConsumer{}

}
