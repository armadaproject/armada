package scheduler

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestSelectNodeForPod(t *testing.T) {
	tests := map[string]struct {
		Nodes         []*SchedulerNode
		Req           *PodSchedulingRequirements
		ExpectSuccess bool
	}{
		"cpu 1": {
			Nodes: testNodeItems1,
			Req: &PodSchedulingRequirements{
				Priority: 0,
				ResourceRequirements: map[string]resource.Quantity{
					"cpu": resource.MustParse("1"),
				},
			},
			ExpectSuccess: true,
		},
		"cpu 7": {
			Nodes: testNodeItems1,
			Req: &PodSchedulingRequirements{
				Priority: 0,
				ResourceRequirements: map[string]resource.Quantity{
					"cpu": resource.MustParse("7"),
				},
			},
			ExpectSuccess: true,
		},
		"cpu 8": {
			Nodes: testNodeItems1,
			Req: &PodSchedulingRequirements{
				Priority: 0,
				ResourceRequirements: map[string]resource.Quantity{
					"cpu": resource.MustParse("8"),
				},
			},
			ExpectSuccess: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			db, err := NewNodeDb(testPriorities, testResources)
			if !assert.NoError(t, err) {
				return
			}
			err = db.Upsert(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			node, err := db.SelectAndBindNodeToPod(uuid.New(), tc.Req)
			if tc.ExpectSuccess {
				assert.NoError(t, err)
				assert.NotNil(t, node)
			} else {
				assert.Error(t, err)
				assert.Nil(t, node)
			}
		})
	}
}

func benchmarkUpsert(numNodes int, b *testing.B) {
	db, err := NewNodeDb(testPriorities, testResources)
	if !assert.NoError(b, err) {
		return
	}
	nodes := testNodeItems2(testPriorities, testResources, numNodes)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := db.Upsert(nodes)
		if !assert.NoError(b, err) {
			return
		}
	}
}

func BenchmarkUpsert1(b *testing.B)      { benchmarkUpsert(1, b) }
func BenchmarkUpsert1000(b *testing.B)   { benchmarkUpsert(1000, b) }
func BenchmarkUpsert100000(b *testing.B) { benchmarkUpsert(100000, b) }

func benchmarkSelectNodeForPod(numNodes int, b *testing.B) {
	db, err := NewNodeDb(testPriorities, testResources)
	if !assert.NoError(b, err) {
		return
	}
	nodes := testNodeItems2(testPriorities, testResources, numNodes)
	err = db.Upsert(nodes)
	if !assert.NoError(b, err) {
		return
	}
	req := &PodSchedulingRequirements{
		Priority: 0,
		ResourceRequirements: map[string]resource.Quantity{
			"cpu":    resource.MustParse("50"),
			"memory": resource.MustParse("50"),
			"gpu":    resource.MustParse("50"),
		},
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		db.SelectAndBindNodeToPod(uuid.New(), req)
	}
}

func BenchmarkSelectNodeForPod1(b *testing.B)      { benchmarkSelectNodeForPod(1, b) }
func BenchmarkSelectNodeForPod1000(b *testing.B)   { benchmarkSelectNodeForPod(1000, b) }
func BenchmarkSelectNodeForPod100000(b *testing.B) { benchmarkSelectNodeForPod(100000, b) }
