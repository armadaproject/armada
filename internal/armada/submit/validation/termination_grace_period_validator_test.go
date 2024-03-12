package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/pkg/api"
)

func TestTerminationGracePeriodValidator(t *testing.T) {
	var defaultMinPeriod int64 = 30
	var defaultMaxPeriod int64 = 300

	tests := map[string]struct {
		req            *api.JobSubmitRequestItem
		minGracePeriod int64
		maxGracePeriod int64
		expectSuccess  bool
	}{
		"no period specified": {
			req:            &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  true,
		},
		"valid TerminationGracePeriod": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(60),
			}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  true,
		},
		"TerminationGracePeriod too low": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(10),
			}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  false,
		},
		"TerminationGracePeriod too high": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				TerminationGracePeriodSeconds: pointer.Int64(700),
			}},
			minGracePeriod: defaultMinPeriod,
			maxGracePeriod: defaultMaxPeriod,
			expectSuccess:  false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := terminationGracePeriodValidator{
				minGracePeriod: tc.minGracePeriod,
				maxGracePeriod: tc.maxGracePeriod,
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
