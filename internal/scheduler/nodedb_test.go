package scheduler

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestNodeDbSchema(t *testing.T) {
	err := nodeDbSchema(testPriorities, testResources).Validate()
	assert.NoError(t, err)
}

// Test the accounting of total resources across all nodes.
func TestTotalResources(t *testing.T) {
	nodeDb, err := createNodeDb([]*schedulerobjects.Node{})
	if !assert.NoError(t, err) {
		return
	}
	expected := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity)}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting nodes for the first time should increase the resource count.
	nodes := testNCpuNode(2, testPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.Upsert(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting the same nodes again should not affect total resource count.
	err = nodeDb.Upsert(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))

	// Upserting new nodes should increase the resource count.
	nodes = testNGpuNode(3, testPriorities)
	for _, node := range nodes {
		expected.Add(node.TotalResources)
	}
	err = nodeDb.Upsert(nodes)
	if !assert.NoError(t, err) {
		return
	}
	assert.True(t, expected.Equal(nodeDb.totalResources))
}

// testNodeItems1() has max of 1Gb and 7cpu available, so check that such jobs requesting less than this
// can be scheduled
func TestSelectNodeForPod_SimpleSuccess(t *testing.T) {
	for i := 1; i < 7; i++ {
		testName := fmt.Sprintf("cpu %d", i)
		t.Run(testName, func(t *testing.T) {
			db, err := createNodeDb(testNodeItems1())
			assert.NoError(t, err)
			report, err := db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
				Priority: 0,
				ResourceRequirements: v1.ResourceRequirements{
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

// testNodeItems1() has max of 1Gb and 7cpu available, so check that such jobs requesting more than this
// cant be scheduled
func TestSelectNodeForPod_SimpleCantSchedule(t *testing.T) {
	db, err := createNodeDb(testNodeItems1())
	assert.NoError(t, err)

	invalidResources := []v1.ResourceList{
		{"cpu": resource.MustParse("8"), "memory": resource.MustParse("1Gi")},
		{"cpu": resource.MustParse("1"), "memory": resource.MustParse("10Gi")},
		{"cpu": resource.MustParse("8000Mi")},
	}

	for _, r := range invalidResources {
		report, err := db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
			Priority:             0,
			ResourceRequirements: v1.ResourceRequirements{Requests: r},
		})
		assert.NoError(t, err)
		assert.Nil(t, report.Node)
	}
}

// Test that some resource we don't know about causes an error.
func TestSelectNodeForPod_InvalidResource(t *testing.T) {
	db, err := createNodeDb(testNodeItems1())
	assert.NoError(t, err)

	report, err := db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "someResourceWeDontHave": resource.MustParse("1")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)
}

// Fill up all the priority zero space on testNodeItems1()
func TestSelectNodeForPod_FillPriorityZero(t *testing.T) {
	db, err := createNodeDb(testNodeItems1())
	assert.NoError(t, err)

	requirements := []*schedulerobjects.PodRequirements{
		{
			Priority: 0,
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{"cpu": resource.MustParse("7")},
			},
		},
		{
			Priority: 0,
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{"cpu": resource.MustParse("4")},
			},
		},
		{
			Priority: 0,
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{"cpu": resource.MustParse("1")},
			},
		},
	}

	// Fill up everything
	for _, r := range requirements {
		report, err := db.SelectAndBindNodeToPod(r)
		assert.NoError(t, err)
		assert.NotNil(t, report.Node)
	}
}

// Check that each job that is scheduled reduces the available resource for the next
func TestSelectNodeForPod_RunningTotal(t *testing.T) {
	db, err := createNodeDb(testNodeItems1())
	assert.NoError(t, err)

	// First job can be scheduled
	report, err := db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("7")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Second job can't be scheduled (too much cpu)
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// third job can be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("4")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// fourth job can't be scheduled (we only have one cpu left)
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("2")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// fifth job can be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// sixth job can't be scheduled (we have no cpu left)
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)
}

// Check that each job that is scheduled reduces the available resource for the next: including memory
func TestSelectNodeForPod_RunningTotalWithMemory(t *testing.T) {
	db, err := createNodeDb(testNodeItems1())
	assert.NoError(t, err)

	// First job can be scheduled
	report, err := db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("7"), "memory": resource.MustParse("7Gi")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Second job can't be scheduled (too much cpu)
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// Third job can be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("4"), "memory": resource.MustParse("4Gi")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Fourth job cant be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// Fifth job can be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Sixth job cant be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)
}

// Check that all jobs scheduled at priority 2 can get the correct cpus
func TestSelectNodeForPod_HigherPriorityMoreResource(t *testing.T) {
	db, err := createNodeDb(testNodeItems1())
	assert.NoError(t, err)

	// First job can be scheduled
	report, err := db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 2,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("9")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Second job can't be scheduled (too much cpu)
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 2,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("7"), "memory": resource.MustParse("5Gi")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// third job can be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 2,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("6")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// fourth job can't be scheduled (we only have three cpu left)
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 2,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("4")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// fifth job can be scheduled
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 2,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("3")},
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// sixth job can't be scheduled (we have no cpu left)
	report, err = db.SelectAndBindNodeToPod(&schedulerobjects.PodRequirements{
		Priority: 2,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1")},
		},
	})
	assert.NoError(t, err)
	assert.Nil(t, report.Node)
}

func TestSelectNodeForPod_RespectTaints(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		{
			Id: "tainted-1",
			Taints: []v1.Taint{
				{Key: "fish", Value: "chips", Effect: v1.TaintEffectNoSchedule},
			},
			AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
				0: {
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("1"),
						"memory": resource.MustParse("1Gi"),
					},
				},
			},
			TotalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("1"),
					"memory": resource.MustParse("1Gi"),
				},
			},
		},
	}

	jobWithoutToleration := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	jobWithDifferentToleration := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
		Tolerations: []v1.Toleration{{Key: "salt", Value: "pepper"}},
	}

	jobWithToleration := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
		Tolerations: []v1.Toleration{{Key: "fish", Value: "chips", Operator: v1.TolerationOpEqual, Effect: v1.TaintEffectNoSchedule}},
	}

	db, err := createNodeDb(nodes)
	assert.NoError(t, err)

	// No toleration means can't be scheduled
	report, err := db.SelectAndBindNodeToPod(jobWithoutToleration)
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// Some random toleration means can't be scheduled
	report, err = db.SelectAndBindNodeToPod(jobWithDifferentToleration)
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// Correct toleration means can be scheduled
	report, err = db.SelectAndBindNodeToPod(jobWithToleration)
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)
}

func TestSelectNodeForPod_RespectNodeSelector(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		{
			Id:     "labelled-1",
			Labels: map[string]string{"foo": "bar"},
			AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
				0: {
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2Gi"),
					},
				},
			},
			TotalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("2"),
					"memory": resource.MustParse("2Gi"),
				},
			},
		},
	}

	jobWithoutSelector := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	jobWithDifferentSelector := &schedulerobjects.PodRequirements{
		Priority:     0,
		NodeSelector: map[string]string{"fish": "chips"},
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	jobWithSelector := &schedulerobjects.PodRequirements{
		Priority:     0,
		NodeSelector: map[string]string{"foo": "bar"},
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	db, err := createNodeDb(nodes)
	assert.NoError(t, err)

	// No Node selector means we can schedule the job
	report, err := db.SelectAndBindNodeToPod(jobWithoutSelector)
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// A node selector that doesn't match means we can't schedule the job
	report, err = db.SelectAndBindNodeToPod(jobWithDifferentSelector)
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// A node selector that does match means we can schedule the job
	report, err = db.SelectAndBindNodeToPod(jobWithSelector)
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)
}

func TestSelectNodeForPod_RespectNodeAffinity(t *testing.T) {
	nodes := []*schedulerobjects.Node{
		{
			Id:     "labelled-1",
			Labels: map[string]string{"foo": "bar"},
			AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
				0: {
					Resources: map[string]resource.Quantity{
						"cpu":    resource.MustParse("2"),
						"memory": resource.MustParse("2Gi"),
					},
				},
			},
			TotalResources: schedulerobjects.ResourceList{
				Resources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("2"),
					"memory": resource.MustParse("2Gi"),
				},
			},
		},
	}

	jobWithoutAffinity := &schedulerobjects.PodRequirements{
		Priority: 0,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	jobWithDifferentAffinity := &schedulerobjects.PodRequirements{
		Priority: 0,
		Affinity: &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "fish",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"chips"},
								},
							},
						},
					},
				},
			},
		},
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	jobWithAffinity := &schedulerobjects.PodRequirements{
		Priority: 0,
		Affinity: &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{
						{
							MatchExpressions: []v1.NodeSelectorRequirement{
								{
									Key:      "foo",
									Operator: v1.NodeSelectorOpIn,
									Values:   []string{"bar"},
								},
							},
						},
					},
				},
			},
		},
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")},
		},
	}

	db, err := createNodeDb(nodes)
	assert.NoError(t, err)

	// No Affinity means we can schedule the job
	report, err := db.SelectAndBindNodeToPod(jobWithoutAffinity)
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)

	// Affinity that doesn't match means we can't schedule the job
	report, err = db.SelectAndBindNodeToPod(jobWithDifferentAffinity)
	assert.NoError(t, err)
	assert.Nil(t, report.Node)

	// Affinity that does match means we can schedule the job
	report, err = db.SelectAndBindNodeToPod(jobWithAffinity)
	assert.NoError(t, err)
	assert.NotNil(t, report.Node)
}

func TestScheduleMany(t *testing.T) {
	tests := map[string]struct {
		// Nodes to schedule across.
		Nodes []*schedulerobjects.Node
		// Schedule one group of jobs at a time.
		// Each group is composed of a slice of pods.
		Reqs [][]*schedulerobjects.PodRequirements
		// For each group, whether we expect scheduling to succeed.
		ExpectSuccess []bool
	}{
		"simple success": {
			Nodes:         testNCpuNode(1, testPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{testNSmallCpuJob(0, 32)},
			ExpectSuccess: []bool{true},
		},
		"simple failure": {
			Nodes:         testNCpuNode(1, testPriorities),
			Reqs:          [][]*schedulerobjects.PodRequirements{testNSmallCpuJob(0, 33)},
			ExpectSuccess: []bool{false},
		},
		"correct rollback": {
			Nodes: testNCpuNode(2, testPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				testNSmallCpuJob(0, 32),
				testNSmallCpuJob(0, 33),
				testNSmallCpuJob(0, 32),
			},
			ExpectSuccess: []bool{true, false, true},
		},
		"varying job size": {
			Nodes: testNCpuNode(2, testPriorities),
			Reqs: [][]*schedulerobjects.PodRequirements{
				append(testNLargeCpuJob(0, 1), testNSmallCpuJob(0, 32)...),
				testNSmallCpuJob(0, 1),
			},
			ExpectSuccess: []bool{true, false},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeDb, err := createNodeDb(tc.Nodes)
			if !assert.NoError(t, err) {
				return
			}
			for i, reqs := range tc.Reqs {
				reports, ok, err := nodeDb.ScheduleMany(reqs)
				if !assert.NoError(t, err) {
					return
				}
				if tc.ExpectSuccess[i] {
					assert.Equal(t, len(reqs), len(reports))
					for _, report := range reports {
						if !assert.NotNil(t, report.Node) {
							return
						}
					}
					assert.True(t, ok)
				} else {
					assert.False(t, ok)
				}
			}
		})
	}
}

func benchmarkUpsert(nodes []*schedulerobjects.Node, b *testing.B) {
	db, err := NewNodeDb(testPriorities, testResources, testIndexedTaints, testIndexedNodeLabels, testNodeIdLabel)
	if !assert.NoError(b, err) {
		return
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := db.Upsert(nodes)
		if !assert.NoError(b, err) {
			return
		}
	}
}

func BenchmarkUpsert1(b *testing.B)      { benchmarkUpsert(testNCpuNode(1, testPriorities), b) }
func BenchmarkUpsert1000(b *testing.B)   { benchmarkUpsert(testNCpuNode(1000, testPriorities), b) }
func BenchmarkUpsert100000(b *testing.B) { benchmarkUpsert(testNCpuNode(100000, testPriorities), b) }

func benchmarkSelectAndBindNodeToPod(nodes []*schedulerobjects.Node, reqs []*schedulerobjects.PodRequirements, b *testing.B) {
	db, err := NewNodeDb(testPriorities, testResources, testIndexedTaints, testIndexedNodeLabels, testNodeIdLabel)
	if !assert.NoError(b, err) {
		return
	}

	err = db.Upsert(nodes)
	if !assert.NoError(b, err) {
		return
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		txn := db.db.Txn(true)
		for _, req := range reqs {
			_, err := db.SelectAndBindNodeToPodWithTxn(txn, req)
			if !assert.NoError(b, err) {
				return
			}
		}
		txn.Abort()
	}
}

func BenchmarkSelectAndBindNodeToPodOneCpuNode(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testNCpuNode(1, testPriorities),
		testNSmallCpuJob(0, 32),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testNCpuNode(100, testPriorities),
		testNSmallCpuJob(0, 320),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		testNCpuNode(10000, testPriorities),
		testNSmallCpuJob(0, 32000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod100CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		withUsedResources(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testNCpuNode(100, testPriorities),
		),
		testNSmallCpuJob(0, 100),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod1000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		withUsedResources(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testNCpuNode(1000, testPriorities),
		),
		testNSmallCpuJob(0, 1000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPod10000CpuNodes1CpuUnused(b *testing.B) {
	benchmarkSelectAndBindNodeToPod(
		withUsedResources(
			0,
			schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("31")}},
			testNCpuNode(10000, testPriorities),
		),
		testNSmallCpuJob(0, 10000),
		b,
	)
}

func BenchmarkSelectAndBindNodeToPodResourceConstrained(b *testing.B) {
	nodes := append(append(
		testNCpuNode(500, testPriorities),
		testNGpuNode(1, testPriorities)...),
		testNCpuNode(499, testPriorities)...,
	)
	benchmarkSelectAndBindNodeToPod(
		nodes,
		testNGPUJob(0, 1),
		b,
	)
}

func testNSmallCpuJob(priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = testSmallCpuJob(priority)
	}
	return rv
}

func testNLargeCpuJob(priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = testLargeCpuJob(priority)
	}
	return rv
}

func testNGPUJob(priority int32, n int) []*schedulerobjects.PodRequirements {
	rv := make([]*schedulerobjects.PodRequirements, n)
	for i := 0; i < n; i++ {
		rv[i] = testGpuJob(priority)
	}
	return rv
}

func testSmallCpuJob(priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("4Gi"),
			},
		},
	}
}

func testLargeCpuJob(priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
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
}

func testGpuJob(priority int32) *schedulerobjects.PodRequirements {
	return &schedulerobjects.PodRequirements{
		Priority: priority,
		ResourceRequirements: v1.ResourceRequirements{
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
}

func createNodeDb(nodes []*schedulerobjects.Node) (*NodeDb, error) {
	db, err := NewNodeDb(testPriorities, testResources, testIndexedTaints, testIndexedNodeLabels, testNodeIdLabel)
	if err != nil {
		return nil, err
	}
	err = db.Upsert(nodes)
	if err != nil {
		return nil, err
	}
	return db, nil
}
