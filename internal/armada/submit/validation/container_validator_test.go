package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestContainerValidator(t *testing.T) {

	oneCpu := v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("1"),
	}

	twoCpu := v1.ResourceList{
		v1.ResourceCPU: resource.MustParse("2"),
	}

	tests := map[string]struct {
		req             *api.JobSubmitRequestItem
		minJobResources v1.ResourceList
		expectSuccess   bool
	}{
		"No pod spec": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: true,
		},
		"No containers": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{},
			},
			expectSuccess: false,
		},
		"Requests Missing": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Limits: oneCpu,
				},
			}),
			expectSuccess: false,
		},
		"Limits Missing": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Requests: oneCpu,
				},
			}),
			expectSuccess: false,
		},
		"Requests and limits different": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Requests: oneCpu,
					Limits:   twoCpu,
				},
			}),
			expectSuccess: false,
		},
		"Request and limits the same": {
			req: reqFromContainer(v1.Container{
				Resources: v1.ResourceRequirements{
					Requests: oneCpu,
					Limits:   oneCpu,
				},
			}),
			expectSuccess: true,
		},
		"Request and limits the same with two containers": {
			req: reqFromContainers([]v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: oneCpu,
						Limits:   oneCpu,
					},
				},
				{
					Resources: v1.ResourceRequirements{
						Requests: twoCpu,
						Limits:   twoCpu,
					},
				},
			}),
			expectSuccess: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := containerValidator{}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func reqFromContainer(container v1.Container) *api.JobSubmitRequestItem {
	return reqFromContainers([]v1.Container{container})
}

func reqFromContainers(containers []v1.Container) *api.JobSubmitRequestItem {
	return &api.JobSubmitRequestItem{
		PodSpec: &v1.PodSpec{Containers: containers},
	}
}
