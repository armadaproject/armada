package armadaevents

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEventNameJobRunTerminated(t *testing.T) {
	event := &EventSequence_Event{
		Event: &EventSequence_Event_JobRunTerminated{
			JobRunTerminated: &JobRunTerminated{},
		},
	}

	assert.Equal(t, "JobRunTerminated", event.GetEventName())
}
