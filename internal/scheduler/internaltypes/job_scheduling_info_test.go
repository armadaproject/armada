package internaltypes

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestFromSchedulerObjectsJobSchedulingInfo(t *testing.T) {

	baseTime := time.Now()

	input := &schedulerobjects.JobSchedulingInfo{
		PriorityClassName: "high-priority",
		SubmitTime:        baseTime,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector: map[string]string{"zone": "us-central1-a"},
						Affinity: &v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
									NodeSelectorTerms: []v1.NodeSelectorTerm{
										{
											MatchExpressions: []v1.NodeSelectorRequirement{
												{
													Key:      "diskType",
													Operator: v1.NodeSelectorOpIn,
													Values:   []string{"ssd"},
												},
											},
										},
									},
								},
							},
						},
						Tolerations: []v1.Toleration{
							{
								Key:      "key1",
								Operator: v1.TolerationOpEqual,
								Value:    "value1",
								Effect:   v1.TaintEffectNoSchedule,
							},
						},
						Annotations: map[string]string{"annotation1": "value1"},
						ResourceRequirements: v1.ResourceRequirements{
							Limits:   v1.ResourceList{"cpu": resource.MustParse("1000m")},
							Requests: v1.ResourceList{"cpu": resource.MustParse("500m")},
						},
					},
				},
			},
		},
		Version: 1,
	}

	expected := &JobSchedulingInfo{
		PriorityClassName: "high-priority",
		SubmitTime:        baseTime,
		PodRequirements: &PodRequirements{
			NodeSelector: map[string]string{"zone": "us-central1-a"},
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "diskType",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"ssd"},
									},
								},
							},
						},
					},
				},
			},
			Tolerations: []v1.Toleration{
				{
					Key:      "key1",
					Operator: v1.TolerationOpEqual,
					Value:    "value1",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			Annotations: map[string]string{"annotation1": "value1"},
			ResourceRequirements: v1.ResourceRequirements{
				Limits:   v1.ResourceList{"cpu": resource.MustParse("1000m")},
				Requests: v1.ResourceList{"cpu": resource.MustParse("500m")},
			},
		},
		Version: 1,
	}

	actual, err := FromSchedulerObjectsJobSchedulingInfo(input)

	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestToSchedulerObjectsJobSchedulingInfo(t *testing.T) {

	baseTime := time.Now()

	input := &JobSchedulingInfo{
		PriorityClassName: "high-priority",
		SubmitTime:        baseTime,
		PodRequirements: &PodRequirements{
			NodeSelector: map[string]string{"zone": "us-central1-a"},
			Affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "diskType",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"ssd"},
									},
								},
							},
						},
					},
				},
			},
			Tolerations: []v1.Toleration{
				{
					Key:      "key1",
					Operator: v1.TolerationOpEqual,
					Value:    "value1",
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			Annotations: map[string]string{"annotation1": "value1"},
			ResourceRequirements: v1.ResourceRequirements{
				Limits:   v1.ResourceList{"cpu": resource.MustParse("1000m")},
				Requests: v1.ResourceList{"cpu": resource.MustParse("500m")},
			},
		},
		Version: 1,
	}

	actual := ToSchedulerObjectsJobSchedulingInfo(input)

	// Create the expected result
	expected := &schedulerobjects.JobSchedulingInfo{
		PriorityClassName: input.PriorityClassName,
		SubmitTime:        input.SubmitTime,
		ObjectRequirements: []*schedulerobjects.ObjectRequirements{
			{
				Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
					PodRequirements: &schedulerobjects.PodRequirements{
						NodeSelector:         input.PodRequirements.NodeSelector,
						Affinity:             input.PodRequirements.Affinity,
						Tolerations:          input.PodRequirements.Tolerations,
						Annotations:          input.PodRequirements.Annotations,
						ResourceRequirements: input.PodRequirements.ResourceRequirements,
					},
				},
			},
		},
		Version: input.Version,
	}

	// Ensure that the conversion produces the correct result
	assert.Equal(t, expected, actual)
}

func TestPodRequirementsDeepCopy(t *testing.T) {
	originalRequirements := samplePodRequirements()
	copiedRequirements := originalRequirements.DeepCopy()

	// Ensure that the deep copiedRequirements is equal to the originalRequirements.
	require.Equal(t, copiedRequirements, originalRequirements)

	// NodeSelector.
	copiedRequirements.NodeSelector["zone"] = "us-central1-b"
	require.NotEqual(t, originalRequirements.NodeSelector, copiedRequirements.NodeSelector)

	// Affinity
	copiedRequirements.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.
		NodeSelectorTerms[0].MatchExpressions[0].Values[0] = "hdd"
	require.NotEqual(t, originalRequirements.Affinity, copiedRequirements.Affinity)

	// Tolerations.
	copiedRequirements.Tolerations[0].Value = "value2"
	require.NotEqual(t, originalRequirements.Tolerations, copiedRequirements.Tolerations)

	// Annotations.
	copiedRequirements.Annotations["annotation1"] = "value2"
	require.NotEqual(t, originalRequirements.Annotations, copiedRequirements.Annotations)

	// ResourceRequirements.
	copiedRequirements.ResourceRequirements.Limits["cpu"] = resource.MustParse("2000m")
	require.NotEqual(t, originalRequirements.ResourceRequirements, copiedRequirements.ResourceRequirements)
}

func TestJobSchedulingInfoDeepCopy(t *testing.T) {
	originalRequirements := &JobSchedulingInfo{
		PriorityClassName: "high-priority",
		SubmitTime:        time.Now(),
		PodRequirements:   samplePodRequirements(),
		Version:           1,
	}
	copiedRequirements := originalRequirements.DeepCopy()

	// Ensure that the deep copiedRequirements is equal to the originalRequirements.
	require.Equal(t, copiedRequirements, originalRequirements)

	// PriorityClassName
	copiedRequirements.PriorityClassName = "low-priority"
	require.NotEqual(t, originalRequirements.PriorityClassName, copiedRequirements.PriorityClassName)

	copiedRequirements.Version = 2
	require.NotEqual(t, originalRequirements.Version, copiedRequirements.Version)

	copiedRequirements.SubmitTime = copiedRequirements.SubmitTime.Add(2 * time.Hour)
	require.NotEqual(t, originalRequirements.SubmitTime, copiedRequirements.SubmitTime)

	// Modify the nested PodRequirements.
	copiedRequirements.PodRequirements.NodeSelector["zone"] = "us-east1-c"
	require.NotEqual(t, originalRequirements.PodRequirements, copiedRequirements.PodRequirements)
}

func TestGetAffinityNodeSelector(t *testing.T) {
	tests := map[string]struct {
		affinity *v1.Affinity
		expected *v1.NodeSelector
	}{
		"nil affinity": {
			affinity: nil,
			expected: nil,
		},
		"nil NodeAffinity": {
			affinity: &v1.Affinity{
				NodeAffinity: nil,
			},
			expected: nil,
		},
		"nil RequiredDuringSchedulingIgnoredDuringExecution": {
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: nil,
				},
			},
			expected: nil,
		},
		"valid NodeSelector": {
			affinity: &v1.Affinity{
				NodeAffinity: &v1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
						NodeSelectorTerms: []v1.NodeSelectorTerm{
							{
								MatchExpressions: []v1.NodeSelectorRequirement{
									{
										Key:      "key1",
										Operator: v1.NodeSelectorOpIn,
										Values:   []string{"value1"},
									},
								},
							},
						},
					},
				},
			},
			expected: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "key1",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{"value1"},
							},
						},
					},
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {

			podReq := &PodRequirements{
				Affinity: tc.affinity,
			}

			nodeSelector := podReq.GetAffinityNodeSelector()

			require.Equal(t, tc.expected, nodeSelector)
		})
	}
}

// helper function to create a sample PodRequirements instance.
func samplePodRequirements() *PodRequirements {
	return &PodRequirements{
		NodeSelector: map[string]string{"zone": "us-central1-a"},
		Affinity: &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "diskType",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"ssd"},
								},
							},
						},
					},
				},
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:      "key1",
				Operator: v1.TolerationOpEqual,
				Value:    "value1",
				Effect:   v1.TaintEffectNoSchedule,
			},
		},
		Annotations: map[string]string{"annotation1": "value1"},
		ResourceRequirements: v1.ResourceRequirements{
			Limits:   v1.ResourceList{"cpu": resource.MustParse("1000m")},
			Requests: v1.ResourceList{"cpu": resource.MustParse("500m")},
		},
	}
}
