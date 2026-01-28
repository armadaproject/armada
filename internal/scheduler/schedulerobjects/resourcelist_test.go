package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/pointer"
)

func TestResourceListDeepCopy(t *testing.T) {
	rl := ResourceList{
		Resources: map[string]*resource.Quantity{
			"foo": pointer.MustParseResource("1"),
		},
	}
	rlCopy := rl.DeepCopy()
	rlCopy.Resources["bar"] = pointer.MustParseResource("2")
	q := rlCopy.Resources["foo"]
	q.Add(resource.MustParse("10"))
	assert.True(
		t,
		rl.Equal(&ResourceList{
			Resources: map[string]*resource.Quantity{
				"foo": pointer.MustParseResource("1"),
			},
		}),
	)
}

func TestResourceListEqual(t *testing.T) {
	tests := map[string]struct {
		a        *ResourceList
		b        *ResourceList
		expected bool
	}{
		"both empty": {
			a:        &ResourceList{},
			b:        &ResourceList{},
			expected: true,
		},
		"both empty maps": {
			a: &ResourceList{
				Resources: make(map[string]*resource.Quantity),
			},
			b: &ResourceList{
				Resources: make(map[string]*resource.Quantity),
			},
			expected: true,
		},
		"one empty map": {
			a: &ResourceList{
				Resources: make(map[string]*resource.Quantity),
			},
			b:        &ResourceList{},
			expected: true,
		},
		"zero equals empty": {
			a: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"foo": pointer.MustParseResource("0"),
				},
			},
			b:        &ResourceList{},
			expected: true,
		},
		"simple equal": {
			a: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":    pointer.MustParseResource("1"),
					"memory": pointer.MustParseResource("2"),
					"foo":    pointer.MustParseResource("3"),
				},
			},
			b: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":    pointer.MustParseResource("1"),
					"memory": pointer.MustParseResource("2"),
					"foo":    pointer.MustParseResource("3"),
				},
			},
			expected: true,
		},
		"simple unequal": {
			a: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"foo": pointer.MustParseResource("1"),
					"bar": pointer.MustParseResource("2"),
				},
			},
			b: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"foo": pointer.MustParseResource("1"),
					"bar": pointer.MustParseResource("3"),
				},
			},
			expected: false,
		},
		"zero and missing is equal": {
			a: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"foo": pointer.MustParseResource("1"),
					"bar": pointer.MustParseResource("0"),
				},
			},
			b: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"foo": pointer.MustParseResource("1"),
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

func TestResourceListSub(t *testing.T) {
	tests := map[string]struct {
		a        *ResourceList
		b        *ResourceList
		expected *ResourceList
	}{
		"basic subtraction": {
			a: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":    pointer.MustParseResource("10"),
					"memory": pointer.MustParseResource("100Gi"),
				},
			},
			b: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":    pointer.MustParseResource("3"),
					"memory": pointer.MustParseResource("20Gi"),
				},
			},
			expected: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":    pointer.MustParseResource("7"),
					"memory": pointer.MustParseResource("80Gi"),
				},
			},
		},
		"subtract with missing resource type": {
			a: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":             pointer.MustParseResource("10"),
					"nvidia.com/a100": pointer.MustParseResource("8"),
				},
			},
			b: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":            pointer.MustParseResource("2"),
					"nvidia.com/gpu": pointer.MustParseResource("1"),
				},
			},
			expected: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu":             pointer.MustParseResource("8"),
					"nvidia.com/a100": pointer.MustParseResource("8"),
				},
			},
		},
		"subtract empty from non-empty": {
			a: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu": pointer.MustParseResource("10"),
				},
			},
			b: &ResourceList{
				Resources: map[string]*resource.Quantity{},
			},
			expected: &ResourceList{
				Resources: map[string]*resource.Quantity{
					"cpu": pointer.MustParseResource("10"),
				},
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
