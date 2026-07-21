package publisher

import (
	"github.com/google/uuid"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher interface {
	// PublishMessages will publish the supplied messages. A LeaderToken is provided and the
	// implementor may decide whether to publish based on the status of this token
	PublishMessages(ctx *armadacontext.Context, events []*armadaevents.EventSequence, shouldPublish func() bool) error

	// PublishMarkers publishes a single marker message for each Pulsar partition.  Each marker
	// massage contains the supplied group id, which allows all marker messages for a given call
	// to be identified.  The uint32 returned is the number of messages published
	PublishMarkers(ctx *armadacontext.Context, groupId uuid.UUID) (uint32, error)
}
