package validation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/pkg/api"
)

func TestValidateSubmitRequest(t *testing.T) {
	defaultSchedulingConfig := configuration.SchedulingConfig{
		Preemption: configuration.PreemptionConfig{
			PriorityClasses: map[string]types.PriorityClass{
				"pc1": {},
			},
		},
		MinJobResources:           map[v1.ResourceName]resource.Quantity{},
		MaxPodSpecSizeBytes:       100,
		MinTerminationGracePeriod: 30 * time.Second,
		MaxTerminationGracePeriod: 300 * time.Second,
	}

	tests := map[string]struct {
		req              *api.JobSubmitRequest
		schedulingConfig configuration.SchedulingConfig
		expectSuccess    bool
	}{
		"valid request": {
			schedulingConfig: defaultSchedulingConfig,
			expectSuccess:    true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateSubmitRequest(tc.req, tc.schedulingConfig)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
