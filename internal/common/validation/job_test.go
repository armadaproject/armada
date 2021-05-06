package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api"
)

func Test_ValidateJobSubmitRequestItem(t *testing.T) {
	validIngressConfig := &api.JobSubmitRequestItem{
		Ingress: []*api.IngressConfig{
			{
				Type: api.IngressType_Http,
				Ports: []uint32{
					5,
				},
			},
		},
	}
	assert.NoError(t, ValidateJobSubmitRequestItem(validIngressConfig))
}

func Test_ValidateJobSubmitRequestItem_WithPortRepeatedInSingleConfig(t *testing.T) {
	validIngressConfig := &api.JobSubmitRequestItem{
		Ingress: []*api.IngressConfig{
			{
				Type: api.IngressType_Http,
				Ports: []uint32{
					5,
					5,
				},
			},
		},
	}
	assert.Error(t, ValidateJobSubmitRequestItem(validIngressConfig))
}

func Test_ValidateJobSubmitRequestItem_WithPortRepeatedInSeperateConfig(t *testing.T) {
	validIngressConfig := &api.JobSubmitRequestItem{
		Ingress: []*api.IngressConfig{
			{
				Type: api.IngressType_Http,
				Ports: []uint32{
					5,
				},
			},
			{
				Type: api.IngressType_Http,
				Ports: []uint32{
					5,
				},
			},
		},
	}
	assert.Error(t, ValidateJobSubmitRequestItem(validIngressConfig))
}
