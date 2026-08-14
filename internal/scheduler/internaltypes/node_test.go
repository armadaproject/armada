package internaltypes

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/types"
	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
)

func TestNode(t *testing.T) {
	resourceListFactory, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "memory", Resolution: resource.MustParse("1")},
			{Name: "cpu", Resolution: resource.MustParse("1m")},
		},
		nil,
	)
	assert.Nil(t, err)

	const id = "id"
	const reportingNodeType = "type"
	const pool = "pool"
	const index = uint64(1)
	const executor = "executor"
	const name = "name"
	taints := []v1.Taint{
		{
			Key:   "foo",
			Value: "bar",
		},
	}
	labels := map[string]string{
		"key": "value",
	}
	totalResources := resourceListFactory.FromNodeProto(
		map[string]*resource.Quantity{
			"cpu":    pointer.MustParseResource("16"),
			"memory": pointer.MustParseResource("32Gi"),
		},
	)
	allocatableResources := resourceListFactory.FromNodeProto(
		map[string]*resource.Quantity{
			"cpu":    pointer.MustParseResource("8"),
			"memory": pointer.MustParseResource("16Gi"),
		},
	)
	allocatableByPriority := map[int32]ResourceList{
		1: resourceListFactory.FromNodeProto(
			map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("0"),
				"memory": pointer.MustParseResource("0Gi"),
			},
		),
		2: resourceListFactory.FromNodeProto(
			map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("8"),
				"memory": pointer.MustParseResource("16Gi"),
			},
		),
		3: resourceListFactory.FromNodeProto(
			map[string]*resource.Quantity{
				"cpu":    pointer.MustParseResource("16"),
				"memory": pointer.MustParseResource("32Gi"),
			},
		),
	}
	allocatedByQueue := map[string]ResourceList{
		"queue": resourceListFactory.FromJobResourceListIgnoreUnknown(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("8"),
				"memory": resource.MustParse("16Gi"),
			},
		),
	}
	allocatedByJobId := map[string]ResourceList{
		"jobId": resourceListFactory.FromJobResourceListIgnoreUnknown(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("8"),
				"memory": resource.MustParse("16Gi"),
			},
		),
	}
	evictedJobRunIds := map[string]bool{
		"jobId":        false,
		"evictedJobId": true,
	}
	keys := [][]byte{
		{
			0, 1, 255,
		},
	}

	nodeType := NewNodeType(
		taints,
		labels,
		map[string]bool{"foo": true},
		map[string]bool{"key": true},
	)

	node := CreateNode(
		id,
		nodeType,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		taints,
		labels,
		false,
		totalResources,
		allocatableResources,
		allocatableByPriority,
		allocatedByQueue,
		allocatedByJobId,
		evictedJobRunIds,
		keys,
	)

	assert.Equal(t, id, node.GetId())
	assert.Equal(t, reportingNodeType, node.GetReportingNodeType())
	assert.Equal(t, nodeType.GetId(), node.GetNodeTypeId())
	assert.Equal(t, nodeType.GetId(), node.GetNodeType().GetId())
	assert.Equal(t, index, node.GetIndex())
	assert.Equal(t, executor, node.GetExecutor())
	assert.Equal(t, name, node.GetName())
	assert.Equal(t, taints, node.GetTaints())
	assert.Equal(t, labels, node.GetLabels())
	assert.Equal(t, totalResources, node.GetTotalResources())
	assert.Equal(t, allocatableByPriority, node.AllocatableByPriority)
	assert.Equal(t, allocatedByQueue, node.AllocatedByQueue)
	assert.Equal(t, allocatedByJobId, node.AllocatedByJobId)
	assert.Equal(t, keys, node.Keys)

	val, ok := node.GetLabelValue("key")
	assert.True(t, ok)
	assert.Equal(t, "value", val)

	val, ok = node.GetLabelValue("missing")
	assert.False(t, ok)
	assert.Empty(t, val)

	tolerations := node.GetTolerationsForTaints()
	assert.Equal(t, []v1.Toleration{{Key: "foo", Value: "bar"}}, tolerations)

	nodeCopy := node.DeepCopyNilKeys()
	node.Keys = nil // UnsafeCopy() sets Keys to nil
	assert.Equal(t, node, nodeCopy)
}

func TestMarkResourceUnallocatable(t *testing.T) {
	resourceListFactory, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "cpu", Resolution: resource.MustParse("1m")},
		},
		nil,
	)
	require.Nil(t, err)

	allocatableResources := makeCpuResourceList(resourceListFactory, "10")
	allocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "8"),
		2: makeCpuResourceList(resourceListFactory, "6"),
	}

	node := createNode(allocatableResources, allocatableByPriority)

	unallocatable := makeCpuResourceList(resourceListFactory, "2")
	expectedAllocatableResources := makeCpuResourceList(resourceListFactory, "8")
	expectedAllocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "6"),
		2: makeCpuResourceList(resourceListFactory, "4"),
	}

	result := node.MarkResourceUnallocatable(unallocatable)

	assert.Equal(t, expectedAllocatableResources, result.allocatableResources)
	assert.Equal(t, expectedAllocatableByPriority, result.AllocatableByPriority)
}

func TestMarkResourceUnallocatable_ProtectsFromNegativeValues(t *testing.T) {
	resourceListFactory, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "cpu", Resolution: resource.MustParse("1m")},
		},
		nil,
	)
	assert.Nil(t, err)

	allocatableResources := makeCpuResourceList(resourceListFactory, "10")
	allocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "8"),
		2: makeCpuResourceList(resourceListFactory, "6"),
	}

	node := createNode(allocatableResources, allocatableByPriority)

	unallocatable := makeCpuResourceList(resourceListFactory, "9")
	expectedAllocatableResources := makeCpuResourceList(resourceListFactory, "1")
	expectedAllocatableByPriority := map[int32]ResourceList{
		1: makeCpuResourceList(resourceListFactory, "0"),
		2: makeCpuResourceList(resourceListFactory, "0"),
	}

	result := node.MarkResourceUnallocatable(unallocatable)

	assert.Equal(t, expectedAllocatableResources, result.allocatableResources)
	assert.Equal(t, expectedAllocatableByPriority, result.AllocatableByPriority)
}

func makeCpuResourceList(factory *ResourceListFactory, cpu string) ResourceList {
	return factory.FromNodeProto(
		map[string]*resource.Quantity{
			"cpu": pointer.MustParseResource(cpu),
		},
	)
}

func TestCrossPoolPriorityConstants(t *testing.T) {
	// CrossPoolPriority must sit strictly between EvictedPriority and the lowest real priority (0).
	assert.Equal(t, int32(-2), EvictedPriority)
	assert.Equal(t, int32(-1), CrossPoolPriority)
	assert.Equal(t, EvictedPriority, MinPriority)
	assert.Less(t, EvictedPriority, CrossPoolPriority)
	assert.Less(t, CrossPoolPriority, int32(0))
}

type testSchedJob struct {
	id            string
	queue         string
	requests      ResourceList
	priorityClass types.PriorityClass
}

func (j *testSchedJob) Id() string                                   { return j.id }
func (j *testSchedJob) Queue() string                                { return j.queue }
func (j *testSchedJob) KubernetesResourceRequirements() ResourceList { return j.requests }

func testAccountingFactory(t *testing.T) *ResourceListFactory {
	t.Helper()
	factory, err := NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "memory", Resolution: resource.MustParse("1")},
			{Name: "cpu", Resolution: resource.MustParse("1m")},
		},
		nil,
	)
	require.NoError(t, err)
	return factory
}

func testJobRequests(factory *ResourceListFactory, cpu, memory string) ResourceList {
	return factory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{
		"cpu":    resource.MustParse(cpu),
		"memory": resource.MustParse(memory),
	})
}

// testAccountingNode builds a node with an empty ledger and AllocatableByPriority
// buckets at priorities 1, 10, and EvictedPriority all equal to total resources.
func testAccountingNode(t *testing.T, factory *ResourceListFactory) *Node {
	t.Helper()
	total := factory.FromNodeProto(map[string]*resource.Quantity{
		"cpu":    pointer.MustParseResource("16"),
		"memory": pointer.MustParseResource("32Gi"),
	})
	allocatableByPriority := map[int32]ResourceList{
		EvictedPriority: total,
		1:               total,
		10:              total,
	}
	nodeType := NewNodeType(nil, nil, map[string]bool{}, map[string]bool{})
	return CreateNode(
		"node-1", nodeType, 1, "executor", "node-1", "pool", "type",
		nil, nil, false, total, total,
		allocatableByPriority,
		map[string]ResourceList{},
		map[string]ResourceList{},
		map[string]bool{},
		nil,
	)
}

func TestNode_AddJob_TracksOwnershipAndAllocatable(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests, priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}

	err := node.AddJob(job, 10)
	require.NoError(t, err)

	assert.Equal(t, requests, node.AllocatedByJobId["job-1"])
	assert.Equal(t, requests, node.AllocatedByQueue["queue-a"])
}

func TestNode_AddJob_DuplicateReturnsError(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests, priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}

	require.NoError(t, node.AddJob(job, 10))
	err := node.AddJob(job, 10)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already has resources allocated")
}

func TestNode_EvictJob_MovesResourcesToEvictedPriority(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests, priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}

	require.NoError(t, node.AddJob(job, 10))
	require.NoError(t, node.EvictJob(job))

	assert.True(t, node.EvictedJobRunIds["job-1"])
	assert.Equal(t, requests, node.AllocatedByJobId["job-1"], "eviction must not release ownership")
}

func TestNode_EvictJob_UnknownJobErrors(t *testing.T) {
	factory := testAccountingFactory(t)
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "ghost", queue: "queue-a", requests: testJobRequests(factory, "1", "1Gi"), priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}

	err := node.EvictJob(job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no resources allocated")
}

func TestNode_RemoveJob_ReleasesOwnershipAndAllocatable(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests, priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}

	require.NoError(t, node.AddJob(job, 10))
	before := node.AllocatableByPriority[10]

	require.NoError(t, node.RemoveJob(job))

	_, hasJob := node.AllocatedByJobId["job-1"]
	assert.False(t, hasJob)
	_, hasQueue := node.AllocatedByQueue["queue-a"]
	assert.False(t, hasQueue, "queue entry must be deleted when it reaches zero")
	assert.Equal(t, before.Add(requests), node.AllocatableByPriority[10])
}

func TestNode_RemoveJob_AlreadyUnboundIsNoop(t *testing.T) {
	factory := testAccountingFactory(t)
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: testJobRequests(factory, "1", "1Gi"), priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}

	err := node.RemoveJob(job)
	require.NoError(t, err)
}

func TestNode_RemoveJob_UsesCutoffStoredAtAdd(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests}

	beforeLow := node.AllocatableByPriority[1]
	beforeHigh := node.AllocatableByPriority[10]

	require.NoError(t, node.AddJob(job, 1))
	require.NoError(t, node.RemoveJob(job))

	assert.Equal(t, beforeLow, node.AllocatableByPriority[1], "bucket 1 must be restored")
	assert.Equal(t, beforeHigh, node.AllocatableByPriority[10], "bucket 10 was never debited and must be unchanged")
}

func TestNode_RemoveJob_HighCutoffReleasesEveryBucket(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests}

	beforeLow := node.AllocatableByPriority[1]
	beforeHigh := node.AllocatableByPriority[10]

	require.NoError(t, node.AddJob(job, math.MaxInt32))
	assert.NotEqual(t, beforeLow, node.AllocatableByPriority[1], "a max cutoff must debit every bucket")

	require.NoError(t, node.RemoveJob(job))

	assert.Equal(t, beforeLow, node.AllocatableByPriority[1])
	assert.Equal(t, beforeHigh, node.AllocatableByPriority[10])
}

func TestNode_EvictThenRemove_ReleasesAtEvictedPriority(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests}

	beforeEvicted := node.AllocatableByPriority[EvictedPriority]
	beforeTen := node.AllocatableByPriority[10]

	require.NoError(t, node.AddJob(job, 10))
	require.NoError(t, node.EvictJob(job))
	require.NoError(t, node.RemoveJob(job))

	assert.Equal(t, beforeEvicted, node.AllocatableByPriority[EvictedPriority])
	assert.Equal(t, beforeTen, node.AllocatableByPriority[10])
	assert.Empty(t, node.EvictedJobRunIds)
	assert.Empty(t, node.AllocatedByJobId)
}

func createNode(allocatableResource ResourceList, allocatableByPriority map[int32]ResourceList) *Node {
	const id = "id"
	const reportingNodeType = "re"
	const pool = "pool"
	const index = uint64(1)
	const executor = "executor"
	const name = "name"
	node := CreateNode(
		id,
		nil,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		nil,
		nil,
		false,
		allocatableResource,
		allocatableResource,
		allocatableByPriority,
		nil,
		nil,
		nil,
		nil,
	)

	return node
}
