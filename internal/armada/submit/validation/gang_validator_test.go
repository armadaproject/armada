package validation

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"strconv"
	"testing"
)

func TestGangValidator(t *testing.T) {
	tests := map[string]struct {
		jobRequests   []*api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"no gang jobs": {
			jobRequests:   []*api.JobSubmitRequestItem{{}, {}},
			expectSuccess: true,
		},
		"complete gang job of cardinality 1 with no minimum cardinality provided": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			expectSuccess: true,
		},
		"complete gang job of cardinality 2 with minimum cardinality of 1": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:                 "foo",
						configuration.GangCardinalityAnnotation:        strconv.Itoa(2),
						configuration.GangMinimumCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			expectSuccess: true,
		},
		"empty gangId": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "",
						configuration.GangCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			expectSuccess: false,
		},
		"complete gang job of cardinality 3": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
			},
			expectSuccess: true,
		},
		"two complete gangs": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
			},
			expectSuccess: true,
		},
		"one complete and one incomplete gang are passed through": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
			},
			expectSuccess: true,
		},
		"missing cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation: "bar",
					},
				},
			},
			expectSuccess: false,
		},
		"invalid cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "not an int",
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation: "not an int",
					},
				},
			},
			expectSuccess: false,
		},
		"zero cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "0",
					},
				},
			},
			expectSuccess: false,
		},
		"negative cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "-1",
					},
				},
			},
			expectSuccess: false,
		},
		"inconsistent cardinality": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(3),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
				},
			},
			expectSuccess: false,
		},
		"inconsistent PriorityClassName": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
					PodSpec: &v1.PodSpec{
						PriorityClassName: "baz",
					},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: strconv.Itoa(2),
					},
					PodSpec: &v1.PodSpec{
						PriorityClassName: "zab",
					},
				},
			},
			expectSuccess: false,
		},
		"inconsistent NodeUniformityLabel": {
			jobRequests: []*api.JobSubmitRequestItem{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:                  "bar",
						configuration.GangCardinalityAnnotation:         strconv.Itoa(2),
						configuration.GangNodeUniformityLabelAnnotation: "foo",
					},
					PodSpec: &v1.PodSpec{},
				},
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:                  "bar",
						configuration.GangCardinalityAnnotation:         strconv.Itoa(2),
						configuration.GangNodeUniformityLabelAnnotation: "bar",
					},
					PodSpec: &v1.PodSpec{},
				},
			},
			expectSuccess: false,
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
		})
	}
}
