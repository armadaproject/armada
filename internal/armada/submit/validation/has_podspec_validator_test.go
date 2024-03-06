package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestHasPodSpecValidator(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"single podspec in podspec field": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{},
			},
			expectSuccess: true,
		},
		"single podspec in podspecs field": {
			req: &api.JobSubmitRequestItem{
				PodSpecs: []*v1.PodSpec{{}},
			},
			expectSuccess: true,
		},
		"multiple podspecs in podspecs field": {
			req: &api.JobSubmitRequestItem{
				PodSpecs: []*v1.PodSpec{{}, {}},
			},
			expectSuccess: false,
		},
		"podspecs and podspec": {
			req: &api.JobSubmitRequestItem{
				PodSpec:  &v1.PodSpec{},
				PodSpecs: []*v1.PodSpec{{}},
			},
			expectSuccess: false,
		},
		"no podspec": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := hasPodSpecValidator{}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
