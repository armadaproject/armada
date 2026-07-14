package publisher

import (
	"github.com/google/uuid"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Dummy publisher just drops all messages, can be useful for testing the scheduler
type DummyPublisher struct{}

func NewDummyPublisher() *DummyPublisher {
	return &DummyPublisher{}
}

func (p *DummyPublisher) PublishMessages(ctx *armadacontext.Context, events []*armadaevents.EventSequence, shouldPublish func() bool) error {
	return nil
}

func (p *DummyPublisher) PublishMarkers(ctx *armadacontext.Context, groupId uuid.UUID) (uint32, error) {
	return 0, nil
}
