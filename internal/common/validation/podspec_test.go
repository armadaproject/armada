package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func Test_ValidatePodSpec_checkForMissingValues(t *testing.T) {
	minResources := v1.ResourceList{}

	assert.Error(t, ValidatePodSpec(nil, 65535, minResources))
	assert.Error(t, ValidatePodSpec(&v1.PodSpec{}, 65535, minResources))
	assert.Error(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{}},
	}, 65535, minResources))
}

func Test_ValidatePodSpec_checkForResources(t *testing.T) {

	cpu := resource.MustParse("1")
	cpu2 := resource.MustParse("2")
	memory := resource.MustParse("512Mi")

	resources1 := v1.ResourceList{"cpu": cpu, "memory": memory}
	resources2 := v1.ResourceList{"cpu": cpu2, "memory": memory}

	minResources := v1.ResourceList{}

	assert.Error(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Limits:   resources1,
				Requests: resources2,
			},
		}},
	}, 65535, minResources))

	assert.NoError(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Limits:   resources1,
				Requests: resources1,
			},
		}},
	}, 65535, minResources))
}

func Test_ValidatePodSpec_checkForPortConfiguration(t *testing.T) {
	minResources := v1.ResourceList{}
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
	assert.Error(t, ValidatePodSpec(portsUniqueToContainer, 65535, minResources))
	assert.Error(t, ValidatePodSpec(portExposeOverMultipleContainers, 65535, minResources))
}

func Test_ValidatePodSpec_WhenPreferredAffinitySet_Fails(t *testing.T) {
	minResources := v1.ResourceList{}
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

	assert.Error(t, ValidatePodSpec(podSpec, 65535, minResources))
}

func Test_ValidatePodSpec_WhenValidRequiredAffinitySet_Succeeds(t *testing.T) {

	minResources := v1.ResourceList{}
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

	assert.Nil(t, ValidatePodSpec(podSpec, 65535, minResources))
}

func Test_ValidatePodSpec_WhenInvalidRequiredAffinitySet_Fails(t *testing.T) {

	minResources := v1.ResourceList{}
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

	assert.Error(t, ValidatePodSpec(podSpec, 65535, minResources))
}

func Test_ValidatePodSpec_WhenExceedsMaxSize_Fails(t *testing.T) {
	minResources := v1.ResourceList{}
	spec := minimalValidPodSpec()
	specSize := uint(spec.Size())
	assert.NoError(t, ValidatePodSpec(spec, specSize, minResources))
	assert.Error(t, ValidatePodSpec(spec, specSize-1, minResources))
}

func Test_ValidatePodSpec_WhenResourcesAboveMinimum_Succeedes(t *testing.T) {
	minResources := v1.ResourceList{
		"memory": resource.MustParse("100Mi"),
	}
	spec := minimalValidPodSpec()

	assert.NoError(t, ValidatePodSpec(spec, uint(spec.Size()), minResources))
}

func Test_ValidatePodSpec_WhenResourcesBelowMinimum_Fails(t *testing.T) {
	minResources := v1.ResourceList{
		"memory": resource.MustParse("100Mi"),
	}
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

	assert.Error(t, ValidatePodSpec(spec, uint(spec.Size()), minResources))
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
