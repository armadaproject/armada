package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api"
)

func TestIngressValidator(t *testing.T) {
	tests := map[string]struct {
		req           *api.JobSubmitRequestItem
		expectSuccess bool
	}{
		"no ingress": {
			req:           &api.JobSubmitRequestItem{},
			expectSuccess: true,
		},
		"valid ingress": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
				},
			},
			expectSuccess: true,
		},
		"multiple ingress": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							6,
						},
					},
				},
			},
			expectSuccess: true,
		},
		"multiple ports": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5, 6,
						},
					},
				},
			},
			expectSuccess: true,
		},
		"no ports": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type:  api.IngressType_Ingress,
						Ports: []uint32{},
					},
				},
			},
			expectSuccess: false,
		},
		"duplicate ports": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5, 6, 5,
						},
					},
				},
			},
			expectSuccess: false,
		},
		"duplicate ports on different ingresses": {
			req: &api.JobSubmitRequestItem{
				Ingress: []*api.IngressConfig{
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
					{
						Type: api.IngressType_Ingress,
						Ports: []uint32{
							5,
						},
					},
				},
			},
			expectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			v := ingressValidator{}
			err := v.Validate(tc.req)
			if tc.expectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
