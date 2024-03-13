package validation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/pointer"

	"github.com/armadaproject/armada/internal/armada/configuration"
)

func Test_ValidatePodSpec_checkForMissingValues(t *testing.T) {
	config := &configuration.SubmissionConfig{
		MinJobResources:     v1.ResourceList{},
		MaxPodSpecSizeBytes: 65535,
	}

	assert.Error(t, ValidatePodSpec(nil, config))
	assert.Error(t, ValidatePodSpec(&v1.PodSpec{}, config))
	assert.Error(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{}},
	}, config))
}

func Test_ValidatePodSpec_checkForResources(t *testing.T) {
	cpu := resource.MustParse("1")
	cpu2 := resource.MustParse("2")
	memory := resource.MustParse("512Mi")

	resources1 := v1.ResourceList{"cpu": cpu, "memory": memory}
	resources2 := v1.ResourceList{"cpu": cpu2, "memory": memory}

	config := &configuration.SubmissionConfig{
		MinJobResources:     v1.ResourceList{},
		MaxPodSpecSizeBytes: 65535,
	}

	assert.Error(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Limits:   resources1,
				Requests: resources2,
			},
		}},
	}, config))

	assert.NoError(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Limits:   resources1,
				Requests: resources1,
			},
		}},
	}, config))
}

func Test_ValidatePodSpec_terminationGracePeriod(t *testing.T) {
	config := &configuration.SubmissionConfig{
		DefaultPriorityClassName:  "high",
		AllowedPriorityClassNames: map[string]bool{"high": true},
		MinTerminationGracePeriod: time.Duration(30 * time.Second),
		MaxTerminationGracePeriod: time.Duration(300 * time.Second),
	}

	podspecWithinRange := &v1.PodSpec{
		TerminationGracePeriodSeconds: pointer.Int64(60),
		PriorityClassName:             "high",
	}
	podspecOutsideRange := &v1.PodSpec{
		TerminationGracePeriodSeconds: pointer.Int64(29),
		PriorityClassName:             "high",
	}
	podspecNoSetting := &v1.PodSpec{
		PriorityClassName: "high",
	}

	assert.Error(t, validateTerminationGracePeriod(podspecOutsideRange, config))
	assert.NoError(t, validateTerminationGracePeriod(podspecWithinRange, config))
	assert.NoError(t, validateTerminationGracePeriod(podspecNoSetting, config))
}

func Test_ValidatePodSpec_checkForPortConfiguration(t *testing.T) {
	config := &configuration.SubmissionConfig{
		MinJobResources:     v1.ResourceList{},
		MaxPodSpecSizeBytes: 65535,
	}

	portsUniqueToContainer := &v1.PodSpec{
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
	}

	portExposeOverMultipleContainers := &v1.PodSpec{
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
	}
	assert.Error(t, ValidatePodSpec(portsUniqueToContainer, config))
	assert.Error(t, ValidatePodSpec(portExposeOverMultipleContainers, config))
}

func Test_ValidatePodSpec_WhenPreferredAffinitySet_Fails(t *testing.T) {
	config := &configuration.SubmissionConfig{
		MinJobResources:     v1.ResourceList{},
		MaxPodSpecSizeBytes: 65535,
	}
	preference := v1.NodeSelectorTerm{
		MatchExpressions: []v1.NodeSelectorRequirement{
			{
				Key:      "a",
				Values:   []string{"b"},
				Operator: v1.NodeSelectorOpIn,
			},
		},
	}

	terms := []v1.PreferredSchedulingTerm{
		{
			Weight:     5,
			Preference: preference,
		},
	}
	podSpec := minimalValidPodSpec()
	podSpec.Affinity = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: terms,
		},
	}

	assert.Error(t, ValidatePodSpec(podSpec, config))
}

func Test_ValidatePodSpec_WhenValidRequiredAffinitySet_Succeeds(t *testing.T) {
	config := &configuration.SubmissionConfig{
		MinJobResources:     v1.ResourceList{},
		MaxPodSpecSizeBytes: 65535,
	}

	nodeSelector := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{
			{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{
						Key:      "a",
						Values:   []string{"b"},
						Operator: v1.NodeSelectorOpIn,
					},
				},
			},
		},
	}

	podSpec := minimalValidPodSpec()
	podSpec.Affinity = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: nodeSelector,
		},
	}

	assert.Nil(t, ValidatePodSpec(podSpec, config))
}

func Test_ValidatePodSpec_WhenInvalidRequiredAffinitySet_Fails(t *testing.T) {
	config := &configuration.SubmissionConfig{
		MinJobResources:     v1.ResourceList{},
		MaxPodSpecSizeBytes: 65535,
	}

	invalidNodeSelector := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{
			{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{
						Key:      "a",
						Values:   []string{"b"},
						Operator: "wrong",
					},
				},
			},
		},
	}

	podSpec := minimalValidPodSpec()
	podSpec.Affinity = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: invalidNodeSelector,
		},
	}

	assert.Error(t, ValidatePodSpec(podSpec, config))
}

func Test_ValidatePodSpec_WhenExceedsMaxSize_Fails(t *testing.T) {
	spec := minimalValidPodSpec()
	specSize := uint(spec.Size())

	config := &configuration.SubmissionConfig{
		MinJobResources:     v1.ResourceList{},
		MaxPodSpecSizeBytes: specSize,
	}
	assert.NoError(t, ValidatePodSpec(spec, config))

	config.MaxPodSpecSizeBytes -= 1
	assert.Error(t, ValidatePodSpec(spec, config))
}

func Test_ValidatePodSpec_WhenResourcesAboveMinimum_Succeeds(t *testing.T) {
	spec := minimalValidPodSpec()

	config := &configuration.SubmissionConfig{
		MaxPodSpecSizeBytes: uint(spec.Size()),
		MinJobResources: v1.ResourceList{
			"memory": resource.MustParse("100Mi"),
		},
	}

	assert.NoError(t, ValidatePodSpec(spec, config))
}

func Test_ValidatePodSpec_WhenResourcesBelowMinimum_Fails(t *testing.T) {
	res := v1.ResourceList{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("1"),
	}
	spec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					Requests: res,
					Limits:   res,
				},
			},
		},
	}

	config := &configuration.SubmissionConfig{
		MaxPodSpecSizeBytes: uint(spec.Size()),
		MinJobResources: v1.ResourceList{
			"memory": resource.MustParse("100Mi"),
		},
	}

	assert.Error(t, ValidatePodSpec(spec, config))
}

func minimalValidPodSpec() *v1.PodSpec {
	res := v1.ResourceList{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("1Gi"),
	}
	return &v1.PodSpec{
		Containers: []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					Requests: res,
					Limits:   res,
				},
			},
		},
	}
}

func Test_ValidatePodSpecPriorityClass(t *testing.T) {
	validPriorityClass := &v1.PodSpec{PriorityClassName: "some-priority-class"}
	allowedPriorityClasses := map[string]bool{"some-priority-class": true}
	assert.NoError(
		t,
		validatePodSpecPriorityClass(validPriorityClass, allowedPriorityClasses),
		"validation should pass when specified priority class is configured to be allowed and preemption is enabled",
	)

	invalidPriorityClass := &v1.PodSpec{PriorityClassName: "some-other-priority-class"}
	err := validatePodSpecPriorityClass(invalidPriorityClass, allowedPriorityClasses)
	assert.Error(
		t,
		err,
		"validation should fail if specified priority class is not configured to be allowed",
	)
	validateInvalidArgumentErrorMessage(t, err, "priorityClassName not in map[some-priority-class:true]")
}
