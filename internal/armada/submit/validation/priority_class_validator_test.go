package validation

import (
	"testing"

	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
)

func TestPriorityClassValidator(t *testing.T) {
	defaultAllowedPriorityClasses := map[string]types.PriorityClass{
		"pc1": {},
	}

	tests := map[string]struct {
		req             *api.JobSubmitRequestItem
		priorityClasses map[string]types.PriorityClass
		expectSuccess   bool
	}{
		"empty priority class": {
			req:             &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{}},
			priorityClasses: defaultAllowedPriorityClasses,
			expectSuccess:   true,
		},
		"valid priority class": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				PriorityClassName: "pc1",
			}},
			priorityClasses: defaultAllowedPriorityClasses,
			expectSuccess:   true,
		},
		"invalid priority class": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				PriorityClassName: "notValid",
			}},
			priorityClasses: defaultAllowedPriorityClasses,
			expectSuccess:   false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := priorityClassValidator{
				allowedPriorityClasses: tc.priorityClasses,
			}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
