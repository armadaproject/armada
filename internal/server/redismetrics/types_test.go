package redismetrics

import (
	"testing"

	"github.com/armadaproject/armada/internal/common/constants"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStreamKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		wantQueue  string
		wantJobSet string
		wantErr    bool
	}{
		{
			name:       "standard queue and jobset",
			key:        constants.EventStreamPrefix + "myqueue:myjobset",
			wantQueue:  "myqueue",
			wantJobSet: "myjobset",
			wantErr:    false,
		},
		{
			name:       "queue with colon separator",
			key:        constants.EventStreamPrefix + "team:gpu:myjobset",
			wantQueue:  "team",
			wantJobSet: "gpu:myjobset",
			wantErr:    false,
		},
		{
			name:       "empty queue",
			key:        constants.EventStreamPrefix + ":myjobset",
			wantQueue:  "",
			wantJobSet: "myjobset",
			wantErr:    false,
		},
		{
			name:       "invalid prefix",
			key:        "Other:q:js",
			wantQueue:  "",
			wantJobSet: "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue, jobSet, err := parseStreamKey(tt.key)
			if tt.wantErr {
				require.Error(t, err, "expected error but got none")
			} else {
				require.NoError(t, err, "unexpected error")
				assert.Equal(t, tt.wantQueue, queue, "queue mismatch")
				assert.Equal(t, tt.wantJobSet, jobSet, "jobSet mismatch")
			}
		})
	}
}
