package eventwatcher

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api"
)

func TestAssertEvent_PodName(t *testing.T) {
	pending := func(podName string) *api.EventMessage {
		return &api.EventMessage{Events: &api.EventMessage_Pending{Pending: &api.JobPendingEvent{JobId: "job-1", RunId: "run-1", PodName: podName}}}
	}
	running := func(podName string) *api.EventMessage {
		return &api.EventMessage{Events: &api.EventMessage_Running{Running: &api.JobRunningEvent{JobId: "job-1", RunId: "run-1", PodName: podName}}}
	}
	failed := func(podName string) *api.EventMessage {
		return &api.EventMessage{Events: &api.EventMessage_Failed{Failed: &api.JobFailedEvent{JobId: "job-1", RunId: "run-1", PodName: podName}}}
	}
	tests := map[string]struct {
		expected *api.EventMessage
		actual   *api.EventMessage
		wantErr  string
	}{
		"an empty template does not check the pod name": {
			expected: pending(""),
			actual:   pending("any-name"),
		},
		"the job ID fills the job-scoped name": {
			expected: pending("armada-{JobId}-0"),
			actual:   pending("armada-job-1-0"),
		},
		"the run ID fills the run-scoped name": {
			expected: running("armada-{RunId}"),
			actual:   running("armada-run-1"),
		},
		"a pod name of the other format fails": {
			expected: pending("armada-{RunId}"),
			actual:   pending("armada-job-1-0"),
			wantErr:  `expected pod name "armada-run-1" but got "armada-job-1-0"`,
		},
		"a failed event checks the pod name": {
			expected: failed("armada-{RunId}"),
			actual:   failed(""),
			wantErr:  `expected pod name "armada-run-1" but got ""`,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := assertEvent(tc.expected, tc.actual)
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tc.wantErr)
		})
	}
}
