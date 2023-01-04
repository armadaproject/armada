package scheduler

import (
	"context"

	"github.com/google/uuid"

	"github.com/G-Research/armada/pkg/armadaevents"
)

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher interface {
	// PublishMessages will publish the supplied messages. A LeaderToken is provided and the
	// implementor may decide whether or not to publish based on the status of this token
	PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, token LeaderToken) error

	// PublishMarkers publishes a single marker message for each Pulsar partition.  Each marker
	// massage contains the supplied group id, which allows nall marker messages for a given call
	// to be identified.  The uint32 returned is the number of messages published
	PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error)
}
