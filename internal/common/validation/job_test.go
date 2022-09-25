package validation

import (
	"testing"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/armadaerrors"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api"
)

func Test_ValidateJobSubmitRequestItem(t *testing.T) {
	validIngressConfig := &api.JobSubmitRequestItem{
		Ingress: []*api.IngressConfig{
			{
				Type: api.IngressType_Ingress,
				Ports: []uint32{
					5,
				},
			},
		},
	}
	assert.NoError(t, ValidateJobSubmitRequestItem(validIngressConfig))
}

func Test_ValidateApiJobPodSpecs(t *testing.T) {
	noPodSpec := &api.Job{}
	err := ValidateApiJobPodSpecs(noPodSpec)
	assert.Error(
		t,
		err,
		"validation should fail when job does not contain at least one PodSpec",
	)
	validateInvalidArgumentErrorMessage(t, err, "Job does not contain at least one PodSpec")

	multiplePods := &api.Job{
		PodSpecs: []*v1.PodSpec{{}, {}},
	}
	err = ValidateApiJobPodSpecs(multiplePods)
	assert.Error(
		t,
		err,
		"validation should fail when a job contains multiple pods",
	)
	validateInvalidArgumentErrorMessage(t, err, "Jobs with multiple pods are not supported")

	multiplePodSpecs := &api.Job{
		PodSpec:  &v1.PodSpec{},
		PodSpecs: []*v1.PodSpec{{}},
	}
	err = ValidateApiJobPodSpecs(multiplePodSpecs)
	assert.Error(
		t,
		err,
		"validation should fail when both PodSpec and PodSpecs fields are specified",
	)
	validateInvalidArgumentErrorMessage(t, err, "Jobs with multiple pods are not supported")
}

func validateInvalidArgumentErrorMessage(t *testing.T, err error, msg string) {
	t.Helper()

	var invalidArgumentErr *armadaerrors.ErrInvalidArgument
	ok := errors.As(err, &invalidArgumentErr)
	assert.True(t, ok, "error should be of type *armadaerrors.ErrInvalidArgument")
	assert.Equal(t, invalidArgumentErr.Message, msg)
}

func Test_ValidateJobSubmitRequestItem_WithPortRepeatedInSingleConfig(t *testing.T) {
	validIngressConfig := &api.JobSubmitRequestItem{
		Ingress: []*api.IngressConfig{
			{
				Type: api.IngressType_Ingress,
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
	}
	assert.Error(t, ValidateJobSubmitRequestItem(validIngressConfig))
}
