package armadaevents

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobIdFromEvent(t *testing.T) {
	tests := []struct {
		name  string
		event *EventSequence_Event
	}{
		{
			name:  "a stop request gives its job id",
			event: &EventSequence_Event{Event: &EventSequence_Event_JobRunStopRequested{JobRunStopRequested: &JobRunStopRequested{JobId: "job-1", RunId: "run-1"}}},
		},
		{
			name:  "a detected stop gives its job id",
			event: &EventSequence_Event{Event: &EventSequence_Event_JobRunStopped{JobRunStopped: &JobRunStopped{JobId: "job-1", RunId: "run-1"}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jobId, err := JobIdFromEvent(tc.event)
			require.NoError(t, err)
			assert.Equal(t, "job-1", jobId)
		})
	}
}
