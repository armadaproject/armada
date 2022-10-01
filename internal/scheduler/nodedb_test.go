package scheduler

import (
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
)

func simpleCpuRequirements(cpu int) *v1.ResourceRequirements {
	return &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			"cpu": resource.MustParse(strconv.Itoa(cpu)),
		},
	}
}

func createNodeDb(nodes []*SchedulerNode) (*NodeDb, error) {
	db, err := NewNodeDb(testPriorities, testResources)
	if err != nil {
		return nil, err
	}
	err = db.Upsert(nodes)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func TestSelectNodeForPod_Simple(t *testing.T) {

	// Test that jobs up to 7 cores can be scheduled
	for i := 1; i < 7; i++ {
		testName := "cpu 1"
		t.Run(testName, func(t *testing.T) {
			db, err := createNodeDb(testNodeItems1)
			assert.NoError(t, err)
			request := simpleCpuRequirements(i)
			report, err := db.SelectAndBindNodeToPod(uuid.New(), request)
			assert.NoError(t, err)
			assert.NotNil(t, report.Node)
		})
	}
}

func TestSelectNodeForPod(t *testing.T) {

	tests := map[string]struct {
		Nodes []*SchedulerNode
		Reqs  []*ReqWithExpectation
	}{
		"cpu 1": {
			Nodes: testNodeItems1,
			Reqs: []*ReqWithExpectation{
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("1"),
							},
						},
					},
					ExpectSuccess: true,
				},
			},
		},
		"cpu 7": {
			Nodes: testNodeItems1,
			Reqs: []*ReqWithExpectation{
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("7"),
							},
						},
					},
					ExpectSuccess: true,
				},
			},
		},
		"cpu 8": {
			Nodes: testNodeItems1,
			Reqs: []*ReqWithExpectation{
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("8"),
							},
						},
					},
					ExpectSuccess: false,
				},
			},
		},
		"all cpu at priority 0": {
			Nodes: testNodeItems1,
			Reqs: []*ReqWithExpectation{
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("7"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("4"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("1"),
							},
						},
					},
					ExpectSuccess: true,
				},
			},
		},
		"running total": {
			Nodes: testNodeItems1,
			Reqs: []*ReqWithExpectation{
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("7"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("5"),
							},
						},
					},
					ExpectSuccess: false,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("4"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("2"),
							},
						},
					},
					ExpectSuccess: false,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("1"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu": resource.MustParse("1"),
							},
						},
					},
					ExpectSuccess: false,
				},
			},
		},
		"running total with memory": {
			Nodes: testNodeItems1,
			Reqs: []*ReqWithExpectation{
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("7"),
								"memory": resource.MustParse("7Gi"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("5"),
								"memory": resource.MustParse("5Gi"),
							},
						},
					},
					ExpectSuccess: false,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("4"),
								"memory": resource.MustParse("4Gi"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("2"),
								"memory": resource.MustParse("2Gi"),
							},
						},
					},
					ExpectSuccess: false,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("1Gi"),
							},
						},
					},
					ExpectSuccess: true,
				},
				{
					Req: &schedulerobjects.PodRequirements{
						Priority: 0,
						ResourceRequirements: &v1.ResourceRequirements{
							Requests: v1.ResourceList{
								"cpu":    resource.MustParse("1"),
								"memory": resource.MustParse("1Gi"),
							},
						},
					},
					ExpectSuccess: false,
				},
			},
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

			for _, req := range tc.Reqs {
				report, err := db.SelectAndBindNodeToPod(uuid.New(), req.Req)
				assert.NoError(t, err)
				assert.NotNil(t, report)
				if req.ExpectSuccess {
					assert.NotNil(t, report.Node)
				} else {
					assert.Nil(t, report.Node)
				}
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
	req := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("50"),
				"memory": resource.MustParse("50"),
				"gpu":    resource.MustParse("50"),
			},
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

func TestAvailableByPriorityAndResourceType(t *testing.T) {
	tests := map[string]struct {
		Priorities          []int32
		AvailableAtPriority int32
		UsedAtPriority      int32
		Resources           map[string]resource.Quantity
	}{
		"lowest priority": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 1,
			UsedAtPriority:      1,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"mid priority": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 5,
			UsedAtPriority:      5,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"highest priority": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 10,
			UsedAtPriority:      10,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"low-mid": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 1,
			UsedAtPriority:      5,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := NewAvailableByPriorityAndResourceType(tc.Priorities)
			assert.Equal(t, len(tc.Priorities), len(m))

			m.MarkAvailable(tc.AvailableAtPriority, tc.Resources)
			for resourceType, quantity := range tc.Resources {
				for _, p := range tc.Priorities {
					actual := m.Get(p, resourceType)
					if p >= tc.AvailableAtPriority {
						assert.Equal(t, 0, quantity.Cmp(actual))
					} else {
						expected := resource.MustParse("0")
						assert.Equal(t, 0, expected.Cmp(actual))
					}
				}
			}

			m.MarkUsed(tc.UsedAtPriority, tc.Resources)
			for resourceType, quantity := range tc.Resources {
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
		})
	}
}

func TestAssignedByPriorityAndResourceType(t *testing.T) {
	tests := map[string]struct {
		Priorities          []int32
		AvailableAtPriority int32
		UsedAtPriority      int32
		Resources           map[string]resource.Quantity
	}{
		"lowest priority": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 1,
			UsedAtPriority:      1,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"mid priority": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 5,
			UsedAtPriority:      5,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"highest priority": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 10,
			UsedAtPriority:      10,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
		"low-mid": {
			Priorities:          []int32{1, 5, 10},
			AvailableAtPriority: 1,
			UsedAtPriority:      5,
			Resources: map[string]resource.Quantity{
				"cpu": resource.MustParse("1"),
				"gpu": resource.MustParse("2"),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := NewAssignedByPriorityAndResourceType(tc.Priorities)
			assert.Equal(t, len(tc.Priorities), len(m))

			m.MarkUsed(tc.UsedAtPriority, tc.Resources)
			for resourceType, quantity := range tc.Resources {
				for _, p := range tc.Priorities {
					actual := m.Get(p, resourceType)
					if p >= tc.UsedAtPriority {
						assert.Equal(t, 0, quantity.Cmp(actual))
					} else {
						expected := resource.MustParse("0")
						assert.Equal(t, 0, expected.Cmp(actual))
					}
				}
			}

			m.MarkAvailable(tc.AvailableAtPriority, tc.Resources)
			for resourceType, quantity := range tc.Resources {
				for _, p := range tc.Priorities {
					actual := m.Get(p, resourceType)
					if p > tc.AvailableAtPriority {
						assert.Equal(t, 0, quantity.Cmp(actual))
					} else {
						expected := resource.MustParse("0")
						assert.Equal(t, 0, expected.Cmp(actual))
					}
				}
			}
		})
	}
}
