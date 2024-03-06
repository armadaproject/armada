package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestContainerValidator(t *testing.T) {
	tests := map[string]struct {
		req        *api.JobSubmitRequestItem
		shouldPass bool
	}{
		"No pod spec": {
			req:        &api.JobSubmitRequestItem{},
			shouldPass: true,
		},
		"No containers": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{},
			},
			shouldPass: false,
		},
		"Requests Missing": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			shouldPass: false,
		},
		"Limits Missing": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			shouldPass: false,
		},
		"Requests and limits different": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			shouldPass: false,
		},
		"One valid container": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			shouldPass: true,
		},
		"Two valid containers": {
			req: &api.JobSubmitRequestItem{
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("2"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			shouldPass: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := containerValidator{}
			err := v.Validate(tc.req)
			if tc.shouldPass {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
