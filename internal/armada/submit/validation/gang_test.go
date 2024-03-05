package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGangValidator(t *testing.T) {
	tests := map[string]struct {
		jobRequests                            []*api.JobSubmitRequestItem
		expectSuccess                          bool
		ExpectedGangMinimumCardinalityByGangId map[string]int
	}{
		"no gang jobs": {
			jobRequests:   []*api.JobSubmitRequestItem{{}, {}},
			expectSuccess: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := gangValidator{}
			err := v.Validate(&api.JobSubmitRequest{JobRequestItems: tc.jobRequests})
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
			for id, e := range gangDetailsById {
				assert.Equal(t, tc.ExpectedGangMinimumCardinalityByGangId[id], e.MinimumCardinality)
			}
		})
	}
}
