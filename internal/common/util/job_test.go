package util

import (
	"testing"

	"github.com/G-Research/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestGetResourceRequestKey(t *testing.T) {
	resourceRequirement := createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("700Gi"), "nvidia.com/gpu": resource.MustParse("1")})
	job := api.Job{
		Priority: 50,
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: resourceRequirement,
				},
			},
			NodeSelector: map[string]string{
				"armada/important": "true",
			},
			Tolerations: []v1.Toleration{
				{
					Key:      "armada/important",
					Value:    "true",
					Operator: v1.TolerationOpEqual,
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "NodeName",
										Operator: v1.NodeSelectorOpExists,
										Values: []string{
											"val1",
											"val2",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	result := GetRequirementKey(&job)

	assert.Equal(t, result, "50++cpu=1--memory=751619276800--nvidia.com/gpu=1||armada/important=true||armada/important+true+NoSchedule+Equal||\n\"\n \n\u001E\n\bNodeName\u0012\u0006Exists\u001A\u0004val1\u001A\u0004val2||1")
}

func TestGenerateJobRequirementsFromKey(t *testing.T) {
	resourceRequirement := createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("751619276800"), "nvidia.com/gpu": resource.MustParse("1")})
	expected := &api.Job{
		Priority: 50,
		PodSpecs: []*v1.PodSpec{
			{
				Containers: []v1.Container{
					{
						Resources: resourceRequirement,
					},
				},
				NodeSelector: map[string]string{
					"armada/important": "true",
				},
				Tolerations: []v1.Toleration{
					{
						Key:      "armada/important",
						Value:    "true",
						Operator: v1.TolerationOpEqual,
						Effect:   v1.TaintEffectNoSchedule,
					},
				},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "NodeName",
											Operator: v1.NodeSelectorOpExists,
											Values: []string{
												"val1",
												"val2",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	result, err := GenerateJobRequirementsFromKey("50++cpu=1--memory=751619276800--nvidia.com/gpu=1||armada/important=true||armada/important+true+NoSchedule+Equal||\n\"\n \n\u001E\n\bNodeName\u0012\u0006Exists\u001A\u0004val1\u001A\u0004val2||1")
	assert.NoError(t, err)
	assert.Equal(t, result, expected)
}

func TestGetRequirementsKey(t *testing.T) {
	resourceRequirement := createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi"), "nvidia.com/gpu": resource.MustParse("1")})
	spec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Resources: resourceRequirement,
			},
		},
	}

	key := getRequirementsKey(spec)
	assert.Equal(t, key, "cpu=1--memory=1073741824--nvidia.com/gpu=1")

	resourceRequirement = createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("1000m"), "memory": resource.MustParse("1Gi"), "nvidia.com/gpu": resource.MustParse("1")})
	spec.Containers[0].Resources = resourceRequirement

	key = getRequirementsKey(spec)
	assert.Equal(t, key, "cpu=1--memory=1073741824--nvidia.com/gpu=1")

	resourceRequirement = createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("100m"), "memory": resource.MustParse("1Gi"), "nvidia.com/gpu": resource.MustParse("1")})
	spec.Containers[0].Resources = resourceRequirement

	key = getRequirementsKey(spec)
	assert.Equal(t, key, "cpu=0.1--memory=1073741824--nvidia.com/gpu=1")

	resourceRequirement = createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("100m"), "memory": resource.MustParse("1G"), "nvidia.com/gpu": resource.MustParse("1")})
	spec.Containers[0].Resources = resourceRequirement

	key = getRequirementsKey(spec)
	assert.Equal(t, key, "cpu=0.1--memory=1000000000--nvidia.com/gpu=1")
}

func TestGetRequirementsKey_WhenPodHasMultipleContainers(t *testing.T) {
	resourceRequirement := createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi"), "nvidia.com/gpu": resource.MustParse("1")})
	spec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Resources: resourceRequirement,
			},
			{
				Resources: resourceRequirement,
			},
		},
		InitContainers: []v1.Container{
			{
				Resources: resourceRequirement,
			},
			{
				Resources: resourceRequirement,
			},
		},
	}

	result := getRequirementsKey(spec)
	assert.Equal(t, result, "cpu=2--memory=2147483648--nvidia.com/gpu=2")
}

func TestGetRequirementFromKey(t *testing.T) {
	resourceRequirement := createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1073741824"), "nvidia.com/gpu": resource.MustParse("1")})
	result, err := getRequirementsFromKey("cpu=1--memory=1073741824--nvidia.com/gpu=1")
	assert.NoError(t, err)
	assert.Equal(t, resourceRequirement, result)

	resourceRequirement = createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("0.1"), "memory": resource.MustParse("1073741824"), "nvidia.com/gpu": resource.MustParse("1")})
	result, err = getRequirementsFromKey("cpu=0.1--memory=1073741824--nvidia.com/gpu=1")
	assert.NoError(t, err)
	assert.Equal(t, resourceRequirement, result)

	resourceRequirement = createResourceRequirements(v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1000000000")})
	result, err = getRequirementsFromKey("cpu=1--memory=1000000000")
	assert.NoError(t, err)
	assert.Equal(t, resourceRequirement, result)

	_, err = getRequirementsFromKey("INVALID_KEY")
	assert.Error(t, err)
}

func TestGetTolerationsKey(t *testing.T) {
	tolerations := []v1.Toleration{
		{
			Key:      "armada/special",
			Operator: v1.TolerationOpEqual,
			Value:    "true",
			Effect:   v1.TaintEffectNoSchedule,
		},
		{
			Key:      "armada/important",
			Operator: v1.TolerationOpEqual,
			Value:    "true",
			Effect:   v1.TaintEffectNoSchedule,
		},
	}

	result := getTolerationsKey(tolerations)
	assert.Equal(t, "armada/important+true+NoSchedule+Equal--armada/special+true+NoSchedule+Equal", result)

	wildcardToleration := []v1.Toleration{
		{
			Key:   "armada/special",
			Value: "true",
		},
	}

	result = getTolerationsKey(wildcardToleration)
	assert.Equal(t, "armada/special+true++", result)

	result = getTolerationsKey([]v1.Toleration{})
	assert.Equal(t, noValueString, result)
}

func TestGetTolerationsFromKey(t *testing.T) {
	tolerations := []v1.Toleration{
		{
			Key:      "armada/important",
			Operator: v1.TolerationOpEqual,
			Value:    "true",
			Effect:   v1.TaintEffectNoSchedule,
		},
		{
			Key:      "armada/special",
			Operator: v1.TolerationOpEqual,
			Value:    "true",
			Effect:   v1.TaintEffectNoSchedule,
		},
	}

	result, err := getTolerationsFromKey("armada/important+true+NoSchedule+Equal--armada/special+true+NoSchedule+Equal")
	assert.NoError(t, err)
	assert.Equal(t, result, tolerations)

	wildcardToleration := []v1.Toleration{
		{
			Key:   "armada/special",
			Value: "true",
		},
	}

	result, err = getTolerationsFromKey("armada/special+true++")
	assert.NoError(t, err)
	assert.Equal(t, result, wildcardToleration)

	result, err = getTolerationsFromKey(noValueString)
	assert.NoError(t, err)
	assert.Equal(t, []v1.Toleration{}, result)

	result, err = getTolerationsFromKey("INVALID_KEY")
	assert.Error(t, err)
}

func TestGetNodeSelectorKey(t *testing.T) {
	spec := &v1.PodSpec{
		NodeSelector: map[string]string{
			"armada/special":   "true",
			"armada/important": "true",
		},
	}

	result := getNodeSelectorKey(spec)
	assert.Equal(t, result, "armada/important=true--armada/special=true")

	spec = &v1.PodSpec{}
	result = getNodeSelectorKey(spec)
	assert.Equal(t, result, noValueString)
}

func TestGetNodeSelectorFromKey(t *testing.T) {
	expected := map[string]string{
		"armada/special":   "true",
		"armada/important": "true",
	}

	result, err := getNodeSelectorFromKey("armada/important=true--armada/special=true")
	assert.NoError(t, err)
	assert.Equal(t, result, expected)

	result, err = getNodeSelectorFromKey(noValueString)
	assert.NoError(t, err)
	assert.Equal(t, result, map[string]string{})

	result, err = getNodeSelectorFromKey("UNKNOWN_FORMAT")
	assert.Error(t, err)
}

func TestGetNodeAffinityKey(t *testing.T) {
	spec := &v1.PodSpec{}
	result := getAffinityKey(spec)
	assert.Equal(t, result, noValueString)

	spec.Affinity = &v1.Affinity{
		NodeAffinity: &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "NodeName",
								Operator: v1.NodeSelectorOpExists,
								Values: []string{
									"val1",
									"val2",
								},
							},
						},
					},
				},
			},
		},
	}

	result = getAffinityKey(spec)
	assert.Equal(t, result, "\n\"\n \n\u001E\n\bNodeName\u0012\u0006Exists\u001A\u0004val1\u001A\u0004val2")
}

func TestGetNodeAffinityFromKey(t *testing.T) {
	result, err := getAffinityFromKey(noValueString)
	assert.NoError(t, err)
	assert.Nil(t, result)

	expected := &v1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: []v1.NodeSelectorRequirement{
						{
							Key:      "NodeName",
							Operator: v1.NodeSelectorOpExists,
							Values: []string{
								"val1",
								"val2",
							},
						},
					},
				},
			},
		},
	}
	result, err = getAffinityFromKey("\n\"\n \n\u001E\n\bNodeName\u0012\u0006Exists\u001A\u0004val1\u001A\u0004val2")
	assert.NoError(t, err)
	assert.Equal(t, expected, result)

	result, err = getAffinityFromKey("UNKNOWN_FORMAT")
	assert.Error(t, err)
}

func createResourceRequirements(resource v1.ResourceList) v1.ResourceRequirements {
	return v1.ResourceRequirements{
		Limits:   resource,
		Requests: resource,
	}
}
