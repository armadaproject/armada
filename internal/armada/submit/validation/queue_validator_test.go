package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestQueueValidator(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequest
		expectSuccess bool
	}{
		"good queue": {
			req: &api.JobSubmitRequest{
				Queue: "my-queue",
			},
			expectSuccess: true,
		},
		"missing namespace": {
			req:           &api.JobSubmitRequest{},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := queueValidator{}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
