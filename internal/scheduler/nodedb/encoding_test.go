package nodedb

import (
	"bytes"
	"testing"

	"github.com/armadaproject/armada/internal/scheduler/testfixtures"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestRoundQuantityToResolution(t *testing.T) {
	tests := map[string]struct {
		q          int64
		resolution int64
		expected   int64
	}{
		"resolution equal to quantity": {
			q:          1024,
			resolution: 1024,
			expected:   1024,
		},
		"just above cutoff": {
			q:          2001,
			resolution: 1000,
			expected:   2000,
		},
		"just below cutoff": {
			q:          2999,
			resolution: 1000,
			expected:   2000,
		},
		"0": {
			q:          0,
			resolution: 1000,
			expected:   0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual := roundQuantityToResolution(tc.q, tc.resolution)
			assert.Equal(t, tc.expected, actual, name)
		})
	}
}

func TestNodeIndexKeyComparison(t *testing.T) {
	actualRoundedKey := RoundedNodeIndexKeyFromResourceList(
		nil,
		0,
		[]string{
			"cpu",
			"memory",
			"gpu",
		},
		[]int64{
			1000,
			1000,
			1000,
		},
		testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(
			map[string]resource.Quantity{
				"cpu":    *resource.NewScaledQuantity(999958006, -9),
				"memory": *resource.NewScaledQuantity(11823681536, 0),
				"gpu":    *resource.NewScaledQuantity(0, 0),
			}),
		0,
	)

	actualKey := NodeIndexKey(
		nil,
		0,
		[]int64{1000, 11823681000, 0},
	)

	expected := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nodeTypeId
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xe8, // cpu
		0x80, 0x00, 0x00, 0x02, 0xc0, 0xbf, 0x0d, 0xe8, // memory
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nvidia.com.gpu
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // nodeIndex
	}
	assert.Equal(t, expected, actualRoundedKey)
	assert.Equal(t, expected, actualKey)
}

func TestNodeIndexKey(t *testing.T) {
	type nodeIndexKeyValues struct {
		nodeTypeId uint64
		resources  []int64
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
				resources:  []int64{1, 2},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []int64{1, 2},
			},
		},
		"equal nodeTypeId and unequal resources": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []int64{2, 1},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []int64{1, 2},
			},
		},
		"unequal nodeTypeId and equal resources": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []int64{1, 2},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 11,
				resources:  []int64{1, 2},
			},
		},
		"negative resource": {
			a: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []int64{1, 2},
			},
			b: nodeIndexKeyValues{
				nodeTypeId: 10,
				resources:  []int64{-1, 2},
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
			if qa > qb {
				return 1
			}
			if qb > qa {
				return -1
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
		NodeIndexKey(nil, 0, []int64{1, 2000}),
		RoundedNodeIndexKeyFromResourceList(
			nil,
			0,
			[]string{"memory", "cpu"},
			[]int64{1, 2000},
			testfixtures.TestResourceListFactory.FromNodeProto(map[string]resource.Quantity{"memory": resource.MustParse("1"), "cpu": resource.MustParse("2")}),
			0,
		),
	)
	assert.Equal(
		t,
		NodeIndexKey(nil, 0, []int64{1, 1500}),
		RoundedNodeIndexKeyFromResourceList(
			nil,
			0,
			[]string{"memory", "cpu"},
			[]int64{1, 1500},
			testfixtures.TestResourceListFactory.FromNodeProto(map[string]resource.Quantity{"memory": resource.MustParse("1"), "cpu": resource.MustParse("2")}),
			0,
		),
	)
}
