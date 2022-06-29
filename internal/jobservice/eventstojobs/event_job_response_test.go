package eventstojobs

import (
	"testing"

	"github.com/G-Research/armada/pkg/api"
)

func TestEventsToJobResponseSubmitted(t *testing.T) {
	submitted := api.EventMessage{&api.EventMessage_Submitted{}}
	EventsToJobResponse(submitted)
}
