package scheduler

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
)

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

// testNodeItems1 has max of 1Gb and 7cpu available, so check that such jobs requesting less than this
// can be scheduled
func TestSelectNodeForPod_SimpleSuccess(t *testing.T) {
	for i := 1; i < 7; i++ {
		testName := fmt.Sprintf("cpu %d", i)
		t.Run(testName, func(t *testing.T) {
			db, err := createNodeDb(testNodeItems1)
			assert.NoError(t, err)
			report, err := db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
				Priority: 0,
				ResourceRequirements: &v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"cpu":    resource.MustParse(strconv.Itoa(i)),
						"memory": resource.MustParse("1Gi"),
					},
				},
			})
			assert.NoError(t, err)
			assert.NotNil(t, report.Node)
		})
	}
}

// testNodeItems1 has max of 1Gb and 7cpu available, so check that such jobs requesting more than this
// cant be scheduled
func TestSelectNodeForPod_SimpleCantSchedule(t *testing.T) {

	db, err := createNodeDb(testNodeItems1)
	assert.NoError(t, err)

	invalidResources := []v1.ResourceList{
		{"cpu": resource.MustParse("8"), "memory": resource.MustParse("1Gi")},
		{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10Gi")},
		{"cpu": resource.MustParse("8000Mi")},
	}

	for _, r := range invalidResources {
		report, err := db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
			Priority:             0,
			ResourceRequirements: &v1.ResourceRequirements{Requests: r},
		})
		assert.NoError(t, err)
		assert.Nil(t, report.Node)
	}
}

// Test that some resource we don't know about causes an error:
// TODO:  Is returning an error here correct?
func TestSelectNodeForPod_InvalidResource(t *testing.T) {
	db, err := createNodeDb(testNodeItems1)
	assert.NoError(t, err)

	report, err := db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "someResourceWeDontHave": resource.MustParse("1")},
		},
	})
	assert.Error(t, err)
	assert.Nil(t, report)
}

// Fill up all the priority zero space on testNodeItems1
func TestSelectNodeForPod_FillPriorityZero(t *testing.T) {

	db, err := createNodeDb(testNodeItems1)
	assert.NoError(t, err)

	requirements := []*schedulerobjects.PodRequirements{
		{
			Priority: 0,
			ResourceRequirements: &v1.ResourceRequirements{
				Requests: v1.ResourceList{"cpu": resource.MustParse("7")},
			},
		},
		{
			Priority: 0,
			ResourceRequirements: &v1.ResourceRequirements{
				Requests: v1.ResourceList{"cpu": resource.MustParse("4")},
			},
		},
		{
			Priority: 0,
			ResourceRequirements: &v1.ResourceRequirements{
				Requests: v1.ResourceList{"cpu": resource.MustParse("1")},
			},
		},
	}

	// Fill up everything
	for _, r := range requirements {
		report, err := db.SelectAndBindNodeToPod(uuid.New(), r)
		assert.NoError(t, err)
		assert.Nil(t, report.Node)
	}
}

// Check that each job that is scheduled reduces the available resource for the next
func TestSelectNodeForPod_RunningTotal(t *testing.T) {

	db, err := createNodeDb(testNodeItems1)
	assert.NoError(t, err)

	// First job can be scheduled
	report, err := db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("7")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Second job can't be scheduled (too much cpu)
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// third job can be scheduled
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("4")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// fourth job can't be scheduled (we only have one cpu left)
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("2")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// fifth job can be scheduled
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// sixth job can't be scheduled (we have no cpu left)
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)
}

// Check that each job that is scheduled reduces the available resource for the next: including memory
func TestSelectNodeForPod_RunningTotalWithMemory(t *testing.T) {
	db, err := createNodeDb(testNodeItems1)
	assert.NoError(t, err)

	// First job can be scheduled
	report, err := db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("7"), "memory": resource.MustParse("7Gi")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Second job can't be scheduled (too much cpu)
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// Third job can be scheduled
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("4"), "memory": resource.MustParse("4Gi")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Fourth job cant be scheduled
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// Fifth job can be scheduled
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Sixth job cant be scheduled
	report, err = db.SelectAndBindNodeToPod(uuid.New(), &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)
}

// Benchmarking
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

func benchmarkSelectAndBindNodeToPod(
	numCpuNodes, numTaintedCpuNodes, numGpuNodes,
	numSmallCpuJobsToSchedule, numLargeCpuJobsToSchedule, numGpuJobsToSchedule int,
	b *testing.B) {

	db, err := NewNodeDb(testPriorities, testResources)
	if !assert.NoError(b, err) {
		return
	}
	nodes := testNodes3(numCpuNodes, numTaintedCpuNodes, numGpuNodes, testPriorities)
	err = db.Upsert(nodes)
	if !assert.NoError(b, err) {
		return
	}

	smallCpuJob := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("4Gi"),
			},
		},
	}
	largeCpuJob := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("32"),
				"memory": resource.MustParse("256Gi"),
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "largeJobsOnly",
				Value: "true",
			},
		},
	}
	gpuJob := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("4"),
				"memory": resource.MustParse("16Gi"),
				"gpu":    resource.MustParse("1"),
			},
		},
		Tolerations: []v1.Toleration{
			{
				Key:   "gpu",
				Value: "true",
			},
		},
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		jobIds := make([]uuid.UUID, 0)
		for i := 0; i < numSmallCpuJobsToSchedule; i++ {
			jobId := uuid.New()
			jobIds = append(jobIds, jobId)
			report, err := db.SelectAndBindNodeToPod(jobId, smallCpuJob)
			if !assert.NoError(b, err) {
				return
			}
			if !assert.NotNil(b, report.Node) {
				return
			}
		}
		for i := 0; i < numLargeCpuJobsToSchedule; i++ {
			jobId := uuid.New()
			jobIds = append(jobIds, jobId)
			report, err := db.SelectAndBindNodeToPod(jobId, largeCpuJob)
			if !assert.NoError(b, err) {
				return
			}
			if !assert.NotNil(b, report.Node) {
				return
			}
		}
		for i := 0; i < numGpuJobsToSchedule; i++ {
			jobId := uuid.New()
			jobIds = append(jobIds, jobId)
			report, err := db.SelectAndBindNodeToPod(jobId, gpuJob)
			if !assert.NoError(b, err) {
				return
			}
			if !assert.NotNil(b, report.Node) {
				return
			}
		}

		// Release resources for the next iteration.
		for _, jobId := range jobIds {
			db.MarkJobRunning(jobId)
		}
	}
}

func BenchmarkSelectAndBindNodeToPod100(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(70, 20, 10, 7, 2, 1, b)
}
func BenchmarkSelectAndBindNodeToPod1000(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(700, 200, 100, 70, 20, 10, b)
}
func BenchmarkSelectAndBindNodeToPod10000(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(7000, 2000, 1000, 700, 200, 100, b)
}

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
