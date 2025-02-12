package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/api/resource"
)

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
