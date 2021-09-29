package validation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func Test_ValidatePodSpec_checkForMissingValues(t *testing.T) {
	assert.Error(t, ValidatePodSpec(nil))
	assert.Error(t, ValidatePodSpec(&v1.PodSpec{}))
	assert.Error(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{}},
	}))
}

func Test_ValidatePodSpec_checkForResources(t *testing.T) {

	cpu := resource.MustParse("1")
	cpu2 := resource.MustParse("2")
	memory := resource.MustParse("512Mi")

	resources1 := v1.ResourceList{"cpu": cpu, "memory": memory}
	resources2 := v1.ResourceList{"cpu": cpu2, "memory": memory}

	assert.Error(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Limits:   resources1,
				Requests: resources2,
			},
		}},
	}))

	assert.NoError(t, ValidatePodSpec(&v1.PodSpec{
		Containers: []v1.Container{{
			Resources: v1.ResourceRequirements{
				Limits:   resources1,
				Requests: resources1,
			},
		}},
	}))
}

func Test_ValidatePodSpec_checkForPortConfiguration(t *testing.T) {
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
	assert.Error(t, ValidatePodSpec(portsUniqueToContainer))
	assert.Error(t, ValidatePodSpec(portExposeOverMultipleContainers))
}

func Test_ValidatePodSpec_WhenPreferredAffinitySet_Fails(t *testing.T) {
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

	assert.Error(t, ValidatePodSpec(podSpec))
}

func Test_ValidatePodSpec_WhenValidRequiredAffinitySet_Succeeds(t *testing.T) {

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

	assert.Nil(t, ValidatePodSpec(podSpec))
}

func Test_ValidatePodSpec_WhenInValidRequiredAffinitySet_Fails(t *testing.T) {

	inValidNodeSelector := &v1.NodeSelector{
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
			RequiredDuringSchedulingIgnoredDuringExecution: inValidNodeSelector,
		},
	}

	assert.Error(t, ValidatePodSpec(podSpec))
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
