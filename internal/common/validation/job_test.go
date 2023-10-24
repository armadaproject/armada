package validation

import (
	"strconv"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/pkg/api"
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

func TestValidateGangs(t *testing.T) {
	tests := map[string]struct {
		Jobs                                   []*api.Job
		ExpectSuccess                          bool
		ExpectedGangMinimumCardinalityByGangId map[string]int
	}{
		"no gang jobs": {
			Jobs:          []*api.Job{{}, {}},
			ExpectSuccess: true,
		},
		"complete gang job of cardinality 1 with no minimum cardinality provided": {
			Jobs: []*api.Job{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "foo",
						configuration.GangCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			ExpectSuccess:                          true,
			ExpectedGangMinimumCardinalityByGangId: map[string]int{"foo": 1},
		},
		"complete gang job of cardinality 2 with minimum cardinality of 1": {
			Jobs: []*api.Job{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:                 "foo",
						configuration.GangCardinalityAnnotation:        strconv.Itoa(2),
						configuration.GangMinimumCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			ExpectSuccess:                          true,
			ExpectedGangMinimumCardinalityByGangId: map[string]int{"foo": 1},
		},
		"empty gangId": {
			Jobs: []*api.Job{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "",
						configuration.GangCardinalityAnnotation: strconv.Itoa(1),
					},
				},
			},
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
		"complete gang job of cardinality 3": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          true,
			ExpectedGangMinimumCardinalityByGangId: map[string]int{"foo": 3},
		},
		"two complete gangs": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          true,
			ExpectedGangMinimumCardinalityByGangId: map[string]int{"foo": 3, "bar": 2},
		},
		"one complete and one incomplete gang are passed through": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          true,
			ExpectedGangMinimumCardinalityByGangId: map[string]int{"foo": 3, "bar": 2},
		},
		"missing cardinality": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
		"invalid cardinality": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
		"zero cardinality": {
			Jobs: []*api.Job{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "0",
					},
				},
			},
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
		"negative cardinality": {
			Jobs: []*api.Job{
				{
					Annotations: map[string]string{
						configuration.GangIdAnnotation:          "bar",
						configuration.GangCardinalityAnnotation: "-1",
					},
				},
			},
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
		"inconsistent cardinality": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
		"inconsistent PriorityClassName": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
		"inconsistent NodeUniformityLabel": {
			Jobs: []*api.Job{
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
			ExpectSuccess:                          false,
			ExpectedGangMinimumCardinalityByGangId: nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gangDetailsById, err := validateGangs(tc.Jobs)
			if tc.ExpectSuccess {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}

			for id, e := range gangDetailsById {
				assert.Equal(t, tc.ExpectedGangMinimumCardinalityByGangId[id], e.expectedMinimumCardinality)
			}
		})
	}
}
