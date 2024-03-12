package validation

import (
	"testing"
)

func TestHasNamespaceValidator(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"good namespace": {
			req: &api.JobSubmitRequestItem{
				Namespace: "my-namespace",
			},
			expectSuccess: true,
		},
		"missing namespace": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := hasNamespaceValidator{}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
