package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

// TestPodRequirementsSerialiser_AffinityDocsUnchanged fails if the docs for the parts of v1.Affinity
// we use for scheduling change, so that we are warned if its definition changes and need to update PodRequirementsSerialiser.
func TestPodRequirementsSerialiser_AffinityDocsUnchanged(t *testing.T) {
	affinity := &v1.Affinity{
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
	}
	assert.Equal(
		t,
		map[string]string{
			"":                "Affinity is a group of affinity scheduling rules.",
			"nodeAffinity":    "Describes node affinity scheduling rules for the pod.",
			"podAffinity":     "Describes pod affinity scheduling rules (e.g. co-locate this pod in the same node, zone, etc. as some other pod(s)).",
			"podAntiAffinity": "Describes pod anti-affinity scheduling rules (e.g. avoid putting this pod in the same node, zone, etc. as some other pod(s)).",
		},
		affinity.SwaggerDoc(),
	)
	assert.Equal(
		t,
		map[string]string{
			"": "Node affinity is a group of node affinity scheduling rules.",
			"requiredDuringSchedulingIgnoredDuringExecution":  "If the affinity requirements specified by this field are not met at scheduling time, the pod will not be scheduled onto the node. If the affinity requirements specified by this field cease to be met at some point during pod execution (e.g. due to an update), the system may or may not try to eventually evict the pod from its node.",
			"preferredDuringSchedulingIgnoredDuringExecution": "The scheduler will prefer to schedule pods to nodes that satisfy the affinity expressions specified by this field, but it may choose a node that violates one or more of the expressions. The node that is most preferred is the one with the greatest sum of weights, i.e. for each node that meets all of the scheduling requirements (resource request, requiredDuringScheduling affinity expressions, etc.), compute a sum by iterating through the elements of this field and adding \"weight\" to the sum if the node matches the corresponding matchExpressions; the node(s) with the highest sum are the most preferred.",
		},
		affinity.NodeAffinity.SwaggerDoc(),
	)
	assert.Equal(
		t,
		map[string]string{
			"":                  "A node selector represents the union of the results of one or more label queries over a set of nodes; that is, it represents the OR of the selectors represented by the node selector terms.",
			"nodeSelectorTerms": "Required. A list of node selector terms. The terms are ORed.",
		},
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.SwaggerDoc(),
	)
	assert.Equal(
		t,
		map[string]string{
			"":                 "A null or empty node selector term matches no objects. The requirements of them are ANDed. The TopologySelectorTerm type implements a subset of the NodeSelectorTerm.",
			"matchExpressions": "A list of node selector requirements by node's labels.",
			"matchFields":      "A list of node selector requirements by node's fields.",
		},
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].SwaggerDoc(),
	)
	assert.Equal(
		t,
		map[string]string{
			"":         "A node selector requirement is a selector that contains values, a key, and an operator that relates the key and values.",
			"key":      "The label key that the selector applies to.",
			"operator": "Represents a key's relationship to a set of values. Valid operators are In, NotIn, Exists, DoesNotExist. Gt, and Lt.",
			"values":   "An array of string values. If the operator is In or NotIn, the values array must be non-empty. If the operator is Exists or DoesNotExist, the values array must be empty. If the operator is Gt or Lt, the values array must have a single element, which will be interpreted as an integer. This array is replaced during a strategic merge patch.",
		},
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].SwaggerDoc(),
	)
	assert.Equal(
		t,
		map[string]string{
			"":         "A node selector requirement is a selector that contains values, a key, and an operator that relates the key and values.",
			"key":      "The label key that the selector applies to.",
			"operator": "Represents a key's relationship to a set of values. Valid operators are In, NotIn, Exists, DoesNotExist. Gt, and Lt.",
			"values":   "An array of string values. If the operator is In or NotIn, the values array must be non-empty. If the operator is Exists or DoesNotExist, the values array must be empty. If the operator is Gt or Lt, the values array must have a single element, which will be interpreted as an integer. This array is replaced during a strategic merge patch.",
		},
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].SwaggerDoc(),
	)
}

func TestSchedulingKey(t *testing.T) {
	defaultN := 10 // Run the check several times to check for consistency.
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
		"affinity PodAffinity ignored": {
			a: &PodRequirements{
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
			equal: true,
		},
		"affinity NodeAffinity MatchExpressions": {
			a: &PodRequirements{
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
				},
			},
			b: &PodRequirements{
				Affinity: &v1.Affinity{
					NodeAffinity: &v1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
							NodeSelectorTerms: []v1.NodeSelectorTerm{
								{
									MatchExpressions: []v1.NodeSelectorRequirement{
										{
											Key:      "k1",
											Operator: "o1",
											Values:   []string{"v1", "v3"},
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
				},
			},
			equal: false,
		},
		"affinity NodeAffinity MatchFields": {
			a: &PodRequirements{
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
				},
			},
			b: &PodRequirements{
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
											Values:   []string{"v10", "v21"},
										},
									},
								},
							},
						},
					},
				},
			},
			equal: false,
		},
		"affinity NodeAffinity multiple MatchFields": {
			a: &PodRequirements{
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
				},
			},
			b: &PodRequirements{
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
										{
											Key:      "k3",
											Operator: "o2",
											Values:   []string{"v10", "v20"},
										},
									},
								},
							},
						},
					},
				},
			},
			equal: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			skg := NewSchedulingKeyGenerator()
			schedulingKeyA := skg.Key(
				tc.a.NodeSelector,
				tc.a.Affinity,
				tc.a.Tolerations,
				tc.a.ResourceRequirements.Requests,
				tc.a.Priority,
			)
			schedulingKeyB := skg.Key(
				tc.b.NodeSelector,
				tc.b.Affinity,
				tc.b.Tolerations,
				tc.b.ResourceRequirements.Requests,
				tc.b.Priority,
			)

			var prevSchedulingKeyA SchedulingKey
			var prevSchedulingKeyB SchedulingKey
			n := defaultN
			if tc.n != 0 {
				n = tc.n
			}
			for i := 0; i < n; i++ {
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

func benchmarPodRequirementsSerialiser(b *testing.B, jobSchedulingInfo *JobSchedulingInfo) {
	skg := NewPodRequirementsSerialiser()
	req := (jobSchedulingInfo.ObjectRequirements[0]).GetPodRequirements()
	out := make([]byte, 0, 1024)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		out = out[0:0]
		out = skg.AppendRequirements(
			out,
			req.NodeSelector,
			req.Affinity,
			req.Tolerations,
			req.ResourceRequirements.Requests,
			req.Priority,
		)
	}
}

func BenchmarkPodRequirementsSerialiser(b *testing.B) {
	benchmarPodRequirementsSerialiser(b, getBenchmarkJobSchedulingSchedulingInfo())
}

func BenchmarkPodRequirementsSerialiser_Affinity(b *testing.B) {
	benchmarPodRequirementsSerialiser(b, getBenchmarkJobSchedulingSchedulingInfoWithAffinity())
}

func benchmarkSchedulingKey(b *testing.B, jobSchedulingInfo *JobSchedulingInfo) {
	skg := NewSchedulingKeyGenerator()
	req := (jobSchedulingInfo.ObjectRequirements[0]).GetPodRequirements()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		skg.Key(
			req.NodeSelector,
			req.Affinity,
			req.Tolerations,
			req.ResourceRequirements.Requests,
			req.Priority,
		)
	}
}

func BenchmarkSchedulingKey(b *testing.B) {
	benchmarkSchedulingKey(b, getBenchmarkJobSchedulingSchedulingInfo())
}

func BenchmarkSchedulingKey_Affinity(b *testing.B) {
	benchmarkSchedulingKey(b, getBenchmarkJobSchedulingSchedulingInfoWithAffinity())
}

func getBenchmarkJobSchedulingSchedulingInfo() *JobSchedulingInfo {
	return &JobSchedulingInfo{
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
}

func getBenchmarkJobSchedulingSchedulingInfoWithAffinity() *JobSchedulingInfo {
	return &JobSchedulingInfo{
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
}
