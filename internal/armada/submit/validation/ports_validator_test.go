package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
)

func TestPortsValidator(t *testing.T) {

	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"no ports": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{},
				},
			}},
			expectSuccess: true,
		},
		"single port": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
				},
			}},
			expectSuccess: true,
		},
		"multiple ports": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
							{ContainerPort: 8080},
						},
					},
				},
			}},
			expectSuccess: true,
		},
		"multiple containers": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 8080},
						},
					},
				},
			}},
			expectSuccess: true,
		},
		"duplicate port": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
							{ContainerPort: 80},
						},
					},
				},
			}},
			expectSuccess: false,
		},
		"duplicate port over multiple containers": {
			req: &api.JobSubmitRequestItem{PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
					{
						Ports: []v1.ContainerPort{
							{ContainerPort: 80},
						},
					},
				},
			}},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := portsValidator{}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
