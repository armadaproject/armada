package nodedb

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// The memdb internally uses bytes.Compare to compare keys.
// Here, we test that byte representation comparison of quantities works as expected.
func TestEncodeQuantity(t *testing.T) {
	tests := map[string]struct {
		A resource.Quantity
		B resource.Quantity
	}{
		"10Mi 10Mi": {
			A: resource.MustParse("10Mi"),
			B: resource.MustParse("10Mi"),
		},
		"5Mi 10Mi": {
			A: resource.MustParse("5Mi"),
			B: resource.MustParse("10Mi"),
		},
		"10Gi 10Gi": {
			A: resource.MustParse("10Gi"),
			B: resource.MustParse("10Gi"),
		},
		"5Gi 10Gi": {
			A: resource.MustParse("5Gi"),
			B: resource.MustParse("10Gi"),
		},
		"1 1": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1"),
		},
		"1 2": {
			A: resource.MustParse("1"),
			B: resource.MustParse("2"),
		},
		"-1 1": {
			A: resource.MustParse("-1"),
			B: resource.MustParse("1"),
		},
		"100m 100m": {
			A: resource.MustParse("100M"),
			B: resource.MustParse("100M"),
		},
		"100m 200m": {
			A: resource.MustParse("100M"),
			B: resource.MustParse("200M"),
		},
		"54870m 54871m": {
			A: resource.MustParse("54870m"),
			B: resource.MustParse("54871m"),
		},
		"1000Ti 1001Ti": {
			A: resource.MustParse("1000Ti"),
			B: resource.MustParse("1001Ti"),
		},
		"1000Pi 1001Pi": {
			A: resource.MustParse("1000Pi"),
			B: resource.MustParse("1001Pi"),
		},
		"1 1001m": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1001m"),
		},
		"1 1000m": {
			A: resource.MustParse("1"),
			B: resource.MustParse("1000m"),
		},
		"1Gi 1001Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1001Mi"),
		},
		"1Gi 1000Mi": {
			A: resource.MustParse("1Gi"),
			B: resource.MustParse("1000Mi"),
		},
		"5188205838208Ki 5188205838209Ki": {
			A: resource.MustParse("5188205838208Ki"),
			B: resource.MustParse("5188205838209Ki"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			expected := tc.A.Cmp(tc.B)
			actual := bytes.Compare(EncodeQuantity(nil, tc.A), EncodeQuantity(nil, tc.B))
			assert.Equal(t, expected, actual)

			expected = tc.B.Cmp(tc.A)
			actual = bytes.Compare(EncodeQuantity(nil, tc.B), EncodeQuantity(nil, tc.A))
			assert.Equal(t, expected, actual)
		})
	}
}

func TestRoundQuantityToResolution(t *testing.T) {
	tests := map[string]struct {
		q                resource.Quantity
		resolutionMillis int64
		expected         resource.Quantity
	}{
		"1Ki": {
			q:                resource.MustParse("1Ki"),
			resolutionMillis: 1,
			expected:         resource.MustParse("1Ki"),
		},
		"resolution equal to quantity": {
			q:                resource.MustParse("1Ki"),
			resolutionMillis: 1024 * 1000,
			expected:         resource.MustParse("1Ki"),
		},
		"0": {
			q:                resource.MustParse("0"),
			resolutionMillis: 1,
			expected:         resource.MustParse("0"),
		},
		"1m": {
			q:                resource.MustParse("1m"),
			resolutionMillis: 1,
			expected:         resource.MustParse("1m"),
		},
		"1": {
			q:                resource.MustParse("1"),
			resolutionMillis: 1,
			expected:         resource.MustParse("1"),
		},
		"resolution 3": {
			q:                resource.MustParse("1"),
			resolutionMillis: 3,
			expected:         resource.MustParse("999m"),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			qc := tc.q.DeepCopy()
			actual := roundQuantityToResolution(tc.q, tc.resolutionMillis)
			assert.True(t, qc.Equal(tc.q))
			assert.Truef(t, actual.Equal(tc.expected), "expected %s, but got %s", tc.expected.String(), actual.String())

			qDec := tc.q.DeepCopy()
			qDec.ToDec()
			qDecCopy := qDec.DeepCopy()
			actualDec := roundQuantityToResolution(qDec, tc.resolutionMillis)
			assert.True(t, qDecCopy.Equal(qDec))
			assert.Truef(t, actualDec.Equal(tc.expected), "expected %s, but got %s", tc.expected.String(), actual.String())
		})
	}
}

func TestNodeIndexKeyComparison(t *testing.T) {
	v1 := resource.MustParse("1")
	actualRoundedKey := RoundedNodeIndexKeyFromResourceList(
		nil,
		0,
		[]string{
			"cpu",
			"memory",
			"nvidia.com/gpu",
			"nvidia.com/mig-1g.10gb",
			"nvidia.com/mig-1g.20gb",
			"nvidia.com/mig-1g.40gb",
		},
		[]int64{
			v1.MilliValue(),
			v1.MilliValue(),
			v1.MilliValue(),
			v1.MilliValue(),
			v1.MilliValue(),
			v1.MilliValue(),
		},
		schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":                    *resource.NewScaledQuantity(999958006, -9),
				"memory":                 *resource.NewScaledQuantity(11823681536, 0),
				"nvidia.com/gpu":         *resource.NewScaledQuantity(0, 0),
				"nvidia.com/mig-1g.10gb": *resource.NewScaledQuantity(0, 0),
				"nvidia.com/mig-1g.20gb": *resource.NewScaledQuantity(0, 0),
				"nvidia.com/mig-1g.40gb": *resource.NewScaledQuantity(0, 0),
			},
		},
		0,
	)
	actualKey := NodeIndexKey(
		nil,
		0,
		[]resource.Quantity{
			*resource.NewScaledQuantity(999958006, -9),
			*resource.NewScaledQuantity(11823681536, 0),
			*resource.NewScaledQuantity(0, 0),
			*resource.NewScaledQuantity(0, 0),
			*resource.NewScaledQuantity(0, 0),
			*resource.NewScaledQuantity(0, 0),
		},
	)
	expected := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nodeTypeId
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, // cpu
		0x80, 0x00, 0x0a, 0xc0, 0xea, 0x56, 0x80, 0x00, // memory
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nvidia.com.gpu
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nvidia.com/mig-1g.10gb
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nvidia.com/mig-1g.20gb
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nvidia.com/mig-1g.40gb
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nodeIndex
	}
	assert.Equal(t, expected, actualRoundedKey)
	assert.Equal(t, expected, actualKey)
}

func TestNodeIndexKey(t *testing.T) {
	type nodeIndexKeyValues struct {
		nodeTypeId uint64
		resources  []resource.Quantity
	}
	tests := map[string]struct {
		a nodeIndexKeyValues
		b nodeIndexKeyValues
	}{
		"equal nodeTypeId": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 10,
			},
		},
		"unequal nodeTypeId": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 11,
			},
		},
		"equal nodeTypeId and resources": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")},
			},
		},
		"equal nodeTypeId and unequal resources": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []resource.Quantity{resource.MustParse("2"), resource.MustParse("1")},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")},
			},
		},
		"unequal nodeTypeId and equal resources": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 11,
				resources:  []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")},
			},
		},
		"negative resource": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []resource.Quantity{resource.MustParse("-1"), resource.MustParse("2")},
			},
		},
	}
	expectedCmp := func(a, b nodeIndexKeyValues) int {
		if a.nodeTypeId < b.nodeTypeId {
			return -1
		} else if a.nodeTypeId > b.nodeTypeId {
			return 1
		}
		for i, qa := range a.resources {
			qb := b.resources[i]
			if cmp := qa.Cmp(qb); cmp != 0 {
				return cmp
			}
		}
		return 0
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			keyA := NodeIndexKey(nil, tc.a.nodeTypeId, tc.a.resources)
			keyB := NodeIndexKey(nil, tc.b.nodeTypeId, tc.b.resources)
			assert.Equal(
				t,
				expectedCmp(tc.a, tc.b),
				bytes.Compare(keyA, keyB),
				"comparison failed for %v, %v", tc.a, tc.b,
			)
		})
	}
}

func TestRoundedNodeIndexKeyFromResourceList(t *testing.T) {
	assert.Equal(
		t,
		NodeIndexKey(nil, 0, []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")}),
		RoundedNodeIndexKeyFromResourceList(
			nil,
			0,
			[]string{"foo", "bar"},
			[]int64{1000, 2000},
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{"foo": resource.MustParse("1"), "bar": resource.MustParse("2")},
			},
			0,
		),
	)
	assert.NotEqual(
		t,
		NodeIndexKey(nil, 0, []resource.Quantity{resource.MustParse("1"), resource.MustParse("2")}),
		RoundedNodeIndexKeyFromResourceList(
			nil,
			0,
			[]string{"foo", "bar"},
			[]int64{1000, 1500},
			schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{"foo": resource.MustParse("1"), "bar": resource.MustParse("2")},
			},
			0,
		),
	)
}

func BenchmarkEncodeQuantityBuffer(b *testing.B) {
	out := make([]byte, 8)
	q := resource.MustParse("16Gi")
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		out = out[0:0]
		EncodeQuantity(out, q)
	}
}
