package scheduler

import (
	"context"

	"github.com/google/uuid"

	"github.com/G-Research/armada/pkg/armadaevents"
)

type Publisher interface {
	PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, token LeaderToken) error
	PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error)
}
