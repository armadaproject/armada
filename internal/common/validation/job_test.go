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

func Test_ValidateJobRequestItemPodSpec(t *testing.T) {
	noPodSpec := &api.JobSubmitRequestItem{}
	err := ValidateJobRequestItemPodSpec(noPodSpec)
	assert.Error(
		t,
		err,
		"validation should fail when job does not contain at least one PodSpec",
	)
	validateInvalidArgumentErrorMessage(t, err, "Job does not contain at least one PodSpec")

	multiplePods := &api.JobSubmitRequestItem{
		PodSpecs: []*v1.PodSpec{{}, {}},
	}
	err = ValidateJobRequestItemPodSpec(multiplePods)
	assert.Error(
		t,
		err,
		"validation should fail when a job contains multiple pods",
	)
	validateInvalidArgumentErrorMessage(t, err, "Jobs with multiple pods are not supported")

	multiplePodSpecs := &api.JobSubmitRequestItem{
		PodSpec:  &v1.PodSpec{},
		PodSpecs: []*v1.PodSpec{{}},
	}
	err = ValidateJobRequestItemPodSpec(multiplePodSpecs)
	assert.Error(
		t,
		err,
		"validation should fail when both PodSpec and PodSpecs fields are specified",
	)
	validateInvalidArgumentErrorMessage(t, err, "Jobs with multiple pods are not supported")
}

func Test_ValidateJobRequestItemPriorityClass(t *testing.T) {
	validPriorityClass := &api.JobSubmitRequestItem{
		PodSpec: &v1.PodSpec{PriorityClassName: "some-priority-class"},
	}
	allowedPriorityClasses := map[string]int32{"some-priority-class": 10}
	assert.NoError(
		t,
		ValidateJobRequestItemPriorityClass(validPriorityClass, true, allowedPriorityClasses),
		"validation should pass when specified priority class is configured to be allowed and preemption is enabled",
	)

	err := ValidateJobRequestItemPriorityClass(validPriorityClass, false, allowedPriorityClasses)
	assert.Error(
		t,
		err,
		"validation should fail if priority class is specified and disabled",
	)
	validateInvalidArgumentErrorMessage(t, err, "Preemption is disabled in Server config")

	invalidPriorityClass := &api.JobSubmitRequestItem{
		PodSpec: &v1.PodSpec{PriorityClassName: "some-other-priority-class"},
	}
	err = ValidateJobRequestItemPriorityClass(invalidPriorityClass, true, allowedPriorityClasses)
	assert.Error(
		t,
		err,
		"validation should fail if specified priority class is not configured to be allowed",
	)
	validateInvalidArgumentErrorMessage(t, err, "Specified Priority Class is not supported in Server config")
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
