package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func TestSchedulingKey(t *testing.T) {
	defaultN := 10 // Runt the check several times to check for consistency.
	tests := map[string]struct {
		a     *PodRequirements
		b     *PodRequirements
		equal bool
		n     int
	}{
		"annontations does not affect key": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Annotations: map[string]string{
					"foo":  "bar",
					"fish": "chips",
					"salt": "pepper",
				},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Annotations: map[string]string{
					"foo":  "bar",
					"fish": "chips",
				},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			equal: true,
		},
		"preemptionPolicy does not affect key": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abcdef",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("2"),
					},
				},
			},
			equal: true,
		},
		"limits does not affect key": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abc",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("3"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority:         1,
				PreemptionPolicy: "abcdef",
				ResourceRequirements: v1.ResourceRequirements{
					Limits: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"gpu":    resource.MustParse("4"),
					},
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			equal: true,
		},
		"priority": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 2,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"zero request does not affect key": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
						"foo":    resource.MustParse("0"),
					},
				},
			},
			equal: true,
		},
		"nodeSelector key": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
					"property2": "value2",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"nodeSelector value": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1-2",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration key": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a-2",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration operator": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b-2",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration value": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b-2",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration effect": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d-2",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
		},
		"toleration tolerationSeconds": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(2),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			equal: true,
		},
		"key ordering": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 2,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
						"cpu":    resource.MustParse("4"),
					},
				},
			},
		},
		"affinity MatchLabels": {
			a: &PodRequirements{
				NodeSelector: map[string]string{
					"property1": "value1",
					"property3": "value3",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
					PodAffinity: &v1.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label1": "labelval1",
										"label2": "labelval2",
										"label3": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2", "v3"},
										},
									},
								},
								Namespaces:  []string{"n1, n2, n3"},
								TopologyKey: "topkey1",
								NamespaceSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label10": "labelval1",
										"label20": "labelval2",
										"label30": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k10",
											Operator: "o10",
											Values:   []string{"v10", "v20", "v30"},
										},
									},
								},
							},
						},
					},
					PodAntiAffinity: nil,
				},
			},
			b: &PodRequirements{
				NodeSelector: map[string]string{
					"property3": "value3",
					"property1": "value1",
				},
				Tolerations: []v1.Toleration{{
					Key:               "a",
					Operator:          "b",
					Value:             "b",
					Effect:            "d",
					TolerationSeconds: pointer.Int64(1),
				}},
				Priority: 1,
				ResourceRequirements: v1.ResourceRequirements{
					Requests: map[v1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"gpu":    resource.MustParse("6"),
					},
				},
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2"},
										},
									},
									MatchFields: []v1.NodeSelectorRequirement{
										{
											Key:      "k2",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
					PodAffinity: &v1.PodAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
							{
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label1": "labelval1-2",
										"label2": "labelval2",
										"label3": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v2", "v3"},
										},
									},
								},
								Namespaces:  []string{"n1, n2, n3"},
								TopologyKey: "topkey1",
								NamespaceSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"label10": "labelval1",
										"label20": "labelval2",
										"label30": "labelval3",
									},
									MatchExpressions: []metav1.LabelSelectorRequirement{
										{
											Key:      "k10",
											Operator: "o10",
											Values:   []string{"v10", "v20", "v30"},
										},
									},
								},
							},
						},
					},
					PodAntiAffinity: nil,
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			schedulingKeyA := tc.a.SchedulingKey()
			schedulingKeyB := tc.b.SchedulingKey()
			var prevSchedulingKeyA [SchedulingKeySize]byte
			var prevSchedulingKeyB [SchedulingKeySize]byte
			n := defaultN
			if tc.n != 0 {
				n = tc.n
			}
			for i := 0; i < n; i++ {
				tc.a.ClearCachedSchedulingKey()
				tc.b.ClearCachedSchedulingKey()
				if tc.equal {
					assert.Equal(t, schedulingKeyA, schedulingKeyB)
				} else {
					assert.NotEqual(t, schedulingKeyA, schedulingKeyB)
				}
				if i != 0 {
					assert.Equal(t, prevSchedulingKeyA, schedulingKeyA)
					assert.Equal(t, prevSchedulingKeyB, schedulingKeyB)
				}
				prevSchedulingKeyA = schedulingKeyA
				prevSchedulingKeyB = schedulingKeyB
			}
		})
	}
}

func BenchmarkSchedulingKey(b *testing.B) {
	jobSchedulingInfo := &JobSchedulingInfo{
		Lifetime:          1,
		AtMostOnce:        true,
		Preemptible:       true,
		ConcurrencySafe:   true,
		PriorityClassName: "armada-default",
		Priority:          10,
		ObjectRequirements: []*ObjectRequirements{
			{
				Requirements: &ObjectRequirements_PodRequirements{
					PodRequirements: &PodRequirements{
						NodeSelector: map[string]string{
							"property1": "value1",
							"property3": "value3",
						},
						Tolerations: []v1.Toleration{{
							Key:               "a",
							Operator:          "b",
							Value:             "b",
							Effect:            "d",
							TolerationSeconds: pointer.Int64(1),
						}},
						Annotations: map[string]string{
							"foo":  "bar",
							"fish": "chips",
							"salt": "pepper",
						},
						Priority:         1,
						PreemptionPolicy: "abc",
						ResourceRequirements: v1.ResourceRequirements{
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("3"),
							},
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("2"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("2"),
							},
						},
					},
				},
			},
		},
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		jobSchedulingInfo.ClearCachedSchedulingKey()
		jobSchedulingInfo.SchedulingKey()
	}
}

func BenchmarkSchedulingKey_Affinity(b *testing.B) {
	jobSchedulingInfo := &JobSchedulingInfo{
		Lifetime:          1,
		AtMostOnce:        true,
		Preemptible:       true,
		ConcurrencySafe:   true,
		PriorityClassName: "armada-default",
		Priority:          10,
		ObjectRequirements: []*ObjectRequirements{
			{
				Requirements: &ObjectRequirements_PodRequirements{
					PodRequirements: &PodRequirements{
						NodeSelector: map[string]string{
							"property1": "value1",
							"property3": "value3",
						},
						Affinity: &v1.Affinity{
							NodeAffinity: &v1.NodeAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
									NodeSelectorTerms: []v1.NodeSelectorTerm{
										{
											MatchExpressions: []v1.NodeSelectorRequirement{
												{
													Key:      "k1",
													Operator: "o1",
													Values:   []string{"v1", "v2"},
												},
											},
											MatchFields: []v1.NodeSelectorRequirement{
												{
													Key:      "k2",
													Operator: "o2",
													Values:   []string{"v10", "v20"},
												},
											},
										},
									},
								},
							},
							PodAffinity: &v1.PodAffinity{
								RequiredDuringSchedulingIgnoredDuringExecution: []v1.PodAffinityTerm{
									{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"label1": "labelval1",
												"label2": "labelval2",
												"label3": "labelval3",
											},
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "k1",
													Operator: "o1",
													Values:   []string{"v1", "v2", "v3"},
												},
											},
										},
										Namespaces:  []string{"n1, n2, n3"},
										TopologyKey: "topkey1",
										NamespaceSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{
												"label10": "labelval1",
												"label20": "labelval2",
												"label30": "labelval3",
											},
											MatchExpressions: []metav1.LabelSelectorRequirement{
												{
													Key:      "k10",
													Operator: "o10",
													Values:   []string{"v10", "v20", "v30"},
												},
											},
										},
									},
								},
							},
							PodAntiAffinity: nil,
						},
						Tolerations: []v1.Toleration{{
							Key:               "a",
							Operator:          "b",
							Value:             "b",
							Effect:            "d",
							TolerationSeconds: pointer.Int64(1),
						}},
						Annotations: map[string]string{
							"foo":  "bar",
							"fish": "chips",
							"salt": "pepper",
						},
						Priority:         1,
						PreemptionPolicy: "abc",
						ResourceRequirements: v1.ResourceRequirements{
							Limits: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("3"),
							},
							Requests: map[v1.ResourceName]resource.Quantity{
								"cpu":    resource.MustParse("2"),
								"memory": resource.MustParse("2"),
								"gpu":    resource.MustParse("2"),
							},
						},
					},
				},
			},
		},
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		jobSchedulingInfo.ClearCachedSchedulingKey()
		jobSchedulingInfo.SchedulingKey()
	}
}
