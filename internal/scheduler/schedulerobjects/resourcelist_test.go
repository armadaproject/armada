package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestQuantityByTAndResourceTypeAdd(t *testing.T) {
	tests := map[string]struct {
		a        QuantityByTAndResourceType[int32]
		b        QuantityByTAndResourceType[int32]
		expected QuantityByTAndResourceType[int32]
	}{
		"nil and nil": {
			a:        nil,
			b:        nil,
			expected: nil,
		},
		"empty and nil": {
			a:        QuantityByTAndResourceType[int32]{},
			b:        nil,
			expected: QuantityByTAndResourceType[int32]{},
		},
		"nil and empty": {
			a:        nil,
			b:        QuantityByTAndResourceType[int32]{},
			expected: nil,
		},
		"matching": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("1")}},
			},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("4")}},
			},
		},
		"mismatched resources": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"bar": resource.MustParse("1")}},
			},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"foo": resource.MustParse("3"),
						"bar": resource.MustParse("1"),
					},
				},
			},
		},
		"mismatched priorities": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
			},
			b: QuantityByTAndResourceType[int32]{
				1: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("1")}},
			},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
				1: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("1")}},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.a.Add(tc.b)
			assert.True(t, tc.a.Equal(tc.expected))
		})
	}
}

func TestQuantityByTAndResourceTypeSub(t *testing.T) {
	tests := map[string]struct {
		a        QuantityByTAndResourceType[int32]
		b        QuantityByTAndResourceType[int32]
		expected QuantityByTAndResourceType[int32]
	}{
		"nil and nil": {
			a:        nil,
			b:        nil,
			expected: nil,
		},
		"empty and nil": {
			a:        QuantityByTAndResourceType[int32]{},
			b:        nil,
			expected: QuantityByTAndResourceType[int32]{},
		},
		"nil and empty": {
			a:        nil,
			b:        QuantityByTAndResourceType[int32]{},
			expected: nil,
		},
		"matching": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("1")}},
			},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("2")}},
			},
		},
		"mismatched resources": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"bar": resource.MustParse("1")}},
			},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"foo": resource.MustParse("3"),
						"bar": resource.MustParse("-1"),
					},
				},
			},
		},
		"mismatched priorities": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
			},
			b: QuantityByTAndResourceType[int32]{
				1: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("1")}},
			},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("3")}},
				1: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("-1")}},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.a.Sub(tc.b)
			assert.True(t, tc.a.Equal(tc.expected))
		})
	}
}

func TestQuantityByTAndResourceTypeEqual(t *testing.T) {
	tests := map[string]struct {
		a        QuantityByTAndResourceType[int32]
		b        QuantityByTAndResourceType[int32]
		expected bool
	}{
		"both empty": {
			a:        QuantityByTAndResourceType[int32]{},
			b:        QuantityByTAndResourceType[int32]{},
			expected: true,
		},
		"both with an empty map": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{},
			},
			expected: true,
		},
		"one empty map": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{},
			},
			b:        QuantityByTAndResourceType[int32]{},
			expected: true,
		},
		"zero equals empty": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"foo": resource.MustParse("0"),
					},
				},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{},
			},
			expected: true,
		},
		"zero equals missing": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{},
			},
			b:        QuantityByTAndResourceType[int32]{},
			expected: true,
		},
		"zero equals missing with empty ResourceList": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"foo": resource.MustParse("0"),
					},
				},
			},
			b:        QuantityByTAndResourceType[int32]{},
			expected: true,
		},
		"simple equal": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("3"),
					},
				},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("3"),
					},
				},
			},
			expected: true,
		},
		"equal with two priorities": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("3"),
					},
				},
				1: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"foo":    resource.MustParse("6"),
					},
				},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("3"),
					},
				},
				1: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("4"),
						"memory": resource.MustParse("5"),
						"foo":    resource.MustParse("6"),
					},
				},
			},
			expected: true,
		},
		"simple unequal": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("3"),
					},
				},
			},
			b: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("4"),
					},
				},
			},
			expected: false,
		},
		"unequal differing priority": {
			a: QuantityByTAndResourceType[int32]{
				0: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("3"),
					},
				},
			},
			b: QuantityByTAndResourceType[int32]{
				1: ResourceList{
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("2"),
						"foo":    resource.MustParse("3"),
					},
				},
			},
			expected: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.a.Equal(tc.b))
			assert.Equal(t, tc.expected, tc.b.Equal(tc.a))
		})
	}
}

func TestQuantityByTAndResourceTypeIsStrictlyNonNegative(t *testing.T) {
	tests := map[string]struct {
		m        QuantityByTAndResourceType[int32]
		expected bool
	}{
		"nil": {
			m:        nil,
			expected: true,
		},
		"empty": {
			m:        QuantityByTAndResourceType[int32]{},
			expected: true,
		},
		"simple zero": {
			m: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("0")}},
			},
			expected: true,
		},
		"simple positive": {
			m: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("1")}},
			},
			expected: true,
		},
		"simple positive and negative": {
			m: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"foo": resource.MustParse("1")}},
				1: ResourceList{Resources: map[string]resource.Quantity{"bar": resource.MustParse("-1")}},
			},
			expected: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.m.IsStrictlyNonNegative())
		})
	}
}

func TestQuantityByTAndResourceTypeMaxAggregatedByResource(t *testing.T) {
	tests := map[string]struct {
		q        QuantityByTAndResourceType[int32]
		p        int32
		rl       ResourceList
		expected QuantityByTAndResourceType[int32]
	}{
		"no change": {
			q: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
			p:  1,
			rl: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
		},
		"empty": {
			q:  QuantityByTAndResourceType[int32]{},
			p:  0,
			rl: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
		},
		"add same resource at same priority": {
			q: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
			p:  0,
			rl: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2")}},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2")}},
			},
		},
		"add different resource at same priority": {
			q: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
			p:  0,
			rl: ResourceList{Resources: map[string]resource.Quantity{"memory": resource.MustParse("1Gi")}},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
			},
		},
		"add same resource at different priority": {
			q: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
			p:  1,
			rl: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("2")}},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
				1: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
		},
		"add different resource at different priority": {
			q: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
			p:  1,
			rl: ResourceList{Resources: map[string]resource.Quantity{"memory": resource.MustParse("1Gi")}},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
				1: ResourceList{Resources: map[string]resource.Quantity{"memory": resource.MustParse("1Gi")}},
			},
		},
		"multiple resources": {
			q: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("100m"), "memory": resource.MustParse("50Mi")}},
			},
			p:  1,
			rl: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("10"), "memory": resource.MustParse("4000Mi")}},
			expected: QuantityByTAndResourceType[int32]{
				0: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("100m"), "memory": resource.MustParse("50Mi")}},
				1: ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("9900m"), "memory": resource.MustParse("3950Mi")}},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.q.MaxAggregatedByResource(tc.p, tc.rl)
			assert.True(t, tc.expected.Equal(tc.q), "expected %s, but got %s", tc.expected.String(), tc.q.String())
		})
	}
}

func TestAllocatableByPriorityAndResourceType(t *testing.T) {
	tests := map[string]struct {
		Priorities     []int32
		UsedAtPriority int32
		Resources      ResourceList
	}{
		"lowest priority": {
			Priorities:     []int32{1, 5, 10},
			UsedAtPriority: 1,
			Resources: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
					"gpu": resource.MustParse("2"),
				},
			},
		},
		"mid priority": {
			Priorities:     []int32{1, 5, 10},
			UsedAtPriority: 5,
			Resources: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
					"gpu": resource.MustParse("2"),
				},
			},
		},
		"highest priority": {
			Priorities:     []int32{1, 5, 10},
			UsedAtPriority: 10,
			Resources: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
					"gpu": resource.MustParse("2"),
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := NewAllocatableByPriorityAndResourceType(tc.Priorities, tc.Resources)
			assert.Equal(t, len(tc.Priorities), len(m))

			m.MarkAllocated(tc.UsedAtPriority, tc.Resources)
			for resourceType, quantity := range tc.Resources.Resources {
				for _, p := range tc.Priorities {
					actual := m.Get(p, resourceType)
					if p > tc.UsedAtPriority {
						assert.Equal(t, 0, quantity.Cmp(actual))
					} else {
						expected := resource.MustParse("0")
						assert.Equal(t, 0, expected.Cmp(actual))
					}
				}
			}

			m.MarkAllocatable(tc.UsedAtPriority, tc.Resources)
			for resourceType, quantity := range tc.Resources.Resources {
				for _, p := range tc.Priorities {
					actual := m.Get(p, resourceType)
					assert.Equal(t, 0, quantity.Cmp(actual))
				}
			}
		})
	}
}

func TestAllocatedByPriorityAndResourceType(t *testing.T) {
	tests := map[string]struct {
		Priorities     []int32
		UsedAtPriority int32
		Resources      map[string]resource.Quantity
	}{
		"lowest priority": {
			Priorities:     []int32{1, 5, 10},
			UsedAtPriority: 1,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"mid priority": {
			Priorities:     []int32{1, 5, 10},
			UsedAtPriority: 5,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"highest priority": {
			Priorities:     []int32{1, 5, 10},
			UsedAtPriority: 10,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := NewAllocatedByPriorityAndResourceType(tc.Priorities)
			assert.Equal(t, len(tc.Priorities), len(m))

			m.MarkAllocated(tc.UsedAtPriority, ResourceList{Resources: tc.Resources})
			for resourceType, quantity := range tc.Resources {
				for _, p := range tc.Priorities {
					actual := m.Get(p, resourceType)
					if p <= tc.UsedAtPriority {
						assert.Equal(t, 0, quantity.Cmp(actual))
					} else {
						expected := resource.MustParse("0")
						assert.Equal(t, 0, expected.Cmp(actual))
					}
				}
			}

			m.MarkAllocatable(tc.UsedAtPriority, ResourceList{Resources: tc.Resources})
			for resourceType := range tc.Resources {
				for _, p := range tc.Priorities {
					actual := m.Get(p, resourceType)
					expected := resource.MustParse("0")
					assert.Equal(t, 0, expected.Cmp(actual))
				}
			}
		})
	}
}

func TestResourceListDeepCopy(t *testing.T) {
	rl := ResourceList{
		Resources: map[string]resource.Quantity{
			"foo": resource.MustParse("1"),
		},
	}
	rlCopy := rl.DeepCopy()
	rlCopy.Resources["bar"] = resource.MustParse("2")
	q := rlCopy.Resources["foo"]
	q.Add(resource.MustParse("10"))
	assert.True(
		t,
		rl.Equal(ResourceList{
			Resources: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
			},
		}),
	)
}

func TestResourceListEqual(t *testing.T) {
	tests := map[string]struct {
		a        ResourceList
		b        ResourceList
		expected bool
	}{
		"both empty": {
			a:        ResourceList{},
			b:        ResourceList{},
			expected: true,
		},
		"both empty maps": {
			a: ResourceList{
				Resources: make(map[string]resource.Quantity),
			},
			b: ResourceList{
				Resources: make(map[string]resource.Quantity),
			},
			expected: true,
		},
		"one empty map": {
			a: ResourceList{
				Resources: make(map[string]resource.Quantity),
			},
			b:        ResourceList{},
			expected: true,
		},
		"zero equals empty": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0"),
				},
			},
			b:        ResourceList{},
			expected: true,
		},
		"simple equal": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("2"),
					"foo":    resource.MustParse("3"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("2"),
					"foo":    resource.MustParse("3"),
				},
			},
			expected: true,
		},
		"simple unequal": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("3"),
				},
			},
			expected: false,
		},
		"zero and missing is equal": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("0"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
				},
			},
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.a.Equal(tc.b))
			assert.Equal(t, tc.expected, tc.b.Equal(tc.a))
		})
	}
}

func TestResourceListIsStrictlyNonNegative(t *testing.T) {
	tests := map[string]struct {
		rl       ResourceList
		expected bool
	}{
		"empty": {
			rl:       ResourceList{},
			expected: true,
		},
		"empty maps": {
			rl: ResourceList{
				Resources: make(map[string]resource.Quantity),
			},
			expected: true,
		},
		"zero-values resource": {
			rl: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0"),
				},
			},
			expected: true,
		},
		"simple non-negative": {
			rl: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("2"),
					"foo":    resource.MustParse("3"),
				},
			},
			expected: true,
		},
		"zero and positive": {
			rl: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("0"),
				},
			},
			expected: true,
		},
		"simple negative": {
			rl: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("-1"),
					"bar": resource.MustParse("0"),
				},
			},
			expected: false,
		},
		"negative zero": {
			rl: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("-0"),
				},
			},
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.rl.IsStrictlyNonNegative())
		})
	}
}

func TestResourceListIsStrictlyLessOrEqual(t *testing.T) {
	tests := map[string]struct {
		a        ResourceList
		b        ResourceList
		expected bool
	}{
		"both empty": {
			a:        ResourceList{},
			b:        ResourceList{},
			expected: true,
		},
		"both empty maps": {
			a: ResourceList{
				Resources: make(map[string]resource.Quantity),
			},
			b: ResourceList{
				Resources: make(map[string]resource.Quantity),
			},
			expected: true,
		},
		"one empty map": {
			a: ResourceList{
				Resources: make(map[string]resource.Quantity),
			},
			b:        ResourceList{},
			expected: true,
		},
		"zero equals empty": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("0"),
				},
			},
			b:        ResourceList{},
			expected: true,
		},
		"zero and missing is equal": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("0"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
				},
			},
			expected: true,
		},
		"simple equal": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("2"),
					"foo":    resource.MustParse("3"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("2"),
					"foo":    resource.MustParse("3"),
				},
			},
			expected: true,
		},
		"simple true": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("3"),
				},
			},
			expected: true,
		},
		"simple false": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("3"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
				},
			},
			expected: false,
		},
		"present in a missing in b true": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
				},
			},
			expected: true,
		},
		"missing in a present in b true": {
			a: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
				},
			},
			b: ResourceList{
				Resources: map[string]resource.Quantity{
					"foo": resource.MustParse("1"),
					"bar": resource.MustParse("2"),
				},
			},
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.a.IsStrictlyLessOrEqual(tc.b))
		})
	}
}

func TestResourceListZero(t *testing.T) {
	rl := ResourceList{
		Resources: map[string]resource.Quantity{
			"foo": resource.MustParse("1"),
			"bar": resource.MustParse("10Gi"),
			"baz": resource.MustParse("0"),
		},
	}
	rl.Zero()
	assert.True(t, rl.Equal(ResourceList{}))
}

func BenchmarkResourceListZero(b *testing.B) {
	rl := ResourceList{
		Resources: map[string]resource.Quantity{
			"foo": resource.MustParse("1"),
			"bar": resource.MustParse("10Gi"),
			"baz": resource.MustParse("0"),
		},
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		rl.Zero()
	}
}

func TestV1ResourceListConversion(t *testing.T) {
	rl := ResourceList{
		Resources: map[string]resource.Quantity{
			"foo": resource.MustParse("1"),
		},
	}
	rlCopy := rl.DeepCopy()
	v1rl := V1ResourceListFromResourceList(rlCopy)
	rlCopy.Resources["bar"] = resource.MustParse("2")
	q := rlCopy.Resources["foo"]
	q.Add(resource.MustParse("10"))

	rl = ResourceListFromV1ResourceList(v1rl)
	assert.True(
		t,
		rl.Equal(ResourceList{
			Resources: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
			},
		}),
	)

	v1rlCopy := V1ResourceListFromResourceList(rl)
	assert.True(t, maps.Equal(v1rlCopy, v1rl))
}

func BenchmarkResourceListZeroAdd(b *testing.B) {
	rla := NewResourceList(3)
	rlb := NewResourceList(3)
	rlb.AddQuantity("cpu", resource.MustParse("2"))
	rlb.AddQuantity("memory", resource.MustParse("10Gi"))
	rlb.AddQuantity("nvidia.com/gpu", resource.MustParse("1"))
	for n := 0; n < b.N; n++ {
		rla.Zero()
		rla.Add(rlb)
	}
}

func BenchmarkQuantityByTAndResourceTypeAdd(b *testing.B) {
	dst := make(QuantityByTAndResourceType[string], 3)
	src := QuantityByTAndResourceType[string]{
		"1": ResourceList{
			Resources: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("2"),
				"baz": resource.MustParse("3"),
			},
		},
		"2": ResourceList{
			Resources: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("2"),
				"baz": resource.MustParse("3"),
			},
		},
		"3": ResourceList{
			Resources: map[string]resource.Quantity{
				"foo": resource.MustParse("1"),
				"bar": resource.MustParse("2"),
				"baz": resource.MustParse("3"),
			},
		},
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dst.Add(src)
	}
}
