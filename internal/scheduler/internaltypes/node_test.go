package internaltypes

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
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
	allowedPriorities := []int32{1, 2, 3}
	keys := [][]byte{
		{
			0, 1, 255,
		},
	}

	indexedTaints := map[string]bool{"foo": true}
	indexedNodeLabels := map[string]bool{"key": true}

	node := CreateNode(
		id,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		taints,
		labels,
		indexedTaints,
		indexedNodeLabels,
		false,
		totalResources,
		allocatableResources,
		allowedPriorities,
		keys,
	)

	// NodeType is derived from the taints and labels filtered by the indexed keys.
	nodeType := NewNodeType(taints, labels, indexedTaints, indexedNodeLabels)

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
	// Allocatable-by-priority is derived: every allowed priority starts with all of
	// the node's allocatable resources, plus the two sentinel priorities.
	assert.Equal(t, []int32{EvictedPriority, CrossPoolPriority, 1, 2, 3}, node.KnownPriorities())
	for _, priority := range node.KnownPriorities() {
		assert.Equal(t, allocatableResources, node.AllocatableAtPriority(priority),
			"priority %d should start fully allocatable", priority)
	}
	// A new node has nothing allocated on it.
	assert.Empty(t, node.AllocatedByJob())
	assert.Empty(t, node.GetRunningJobIds())
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

func createNodeWithIndexedLabelsAndTaints(taints []v1.Taint, labels map[string]string) *Node {
	indexedTaints := map[string]bool{"foo": true, unschedulableTaintKey: true}
	indexedNodeLabels := map[string]bool{"key": true}
	return CreateNode(
		"id", 1, "executor", "name", "pool", "type",
		taints, labels, indexedTaints, indexedNodeLabels,
		false, ResourceList{}, ResourceList{},
		[]int32{1, 2}, nil,
	)
}

func TestWithSchedulable_AddsAndRemovesUnschedulableTaint(t *testing.T) {
	node := createNodeWithIndexedLabelsAndTaints([]v1.Taint{{Key: "foo", Value: "bar"}}, map[string]string{"key": "value"})
	require.False(t, node.IsUnschedulable())
	require.NotContains(t, node.GetTaints(), UnschedulableTaint())

	unschedulable := node.WithSchedulable(false)
	assert.True(t, unschedulable.IsUnschedulable())
	assert.Contains(t, unschedulable.GetTaints(), UnschedulableTaint())

	// Making the node schedulable again must remove the taint, not just clear the flag.
	reschedulable := unschedulable.WithSchedulable(true)
	assert.False(t, reschedulable.IsUnschedulable())
	assert.NotContains(t, reschedulable.GetTaints(), UnschedulableTaint())
	// Unrelated taints are preserved.
	assert.Contains(t, reschedulable.GetTaints(), v1.Taint{Key: "foo", Value: "bar"})

	// The node type indexes the unschedulable taint, so it must track the change.
	assert.NotEqual(t, node.GetNodeTypeId(), unschedulable.GetNodeTypeId())
	assert.Equal(t, node.GetNodeTypeId(), reschedulable.GetNodeTypeId())
}

func TestWithTaints_RecomputeNodeType(t *testing.T) {
	node := createNodeWithIndexedLabelsAndTaints([]v1.Taint{{Key: "foo", Value: "bar"}}, map[string]string{"key": "value"})

	indexedTaintChanged := node.WithTaints([]v1.Taint{{Key: "foo", Value: "baz"}})
	assert.Equal(t, []v1.Taint{{Key: "foo", Value: "baz"}}, indexedTaintChanged.GetTaints())
	assert.NotEqual(t, node.GetNodeTypeId(), indexedTaintChanged.GetNodeTypeId())

	nonIndexedTaintChanged := node.WithTaints(append([]v1.Taint{{Key: "non-indexed", Value: "baz"}}, node.GetTaints()...))
	assert.Len(t, nonIndexedTaintChanged.GetTaints(), 2)
	assert.Contains(t, nonIndexedTaintChanged.GetTaints(), v1.Taint{Key: "foo", Value: "bar"})
	assert.Contains(t, nonIndexedTaintChanged.GetTaints(), v1.Taint{Key: "non-indexed", Value: "baz"})
	assert.Equal(t, node.GetNodeTypeId(), nonIndexedTaintChanged.GetNodeTypeId())
}

func TestWithLabels_RecomputeNodeType(t *testing.T) {
	node := createNodeWithIndexedLabelsAndTaints([]v1.Taint{{Key: "foo", Value: "bar"}}, map[string]string{"key": "value"})

	indexedLabelChanged := node.WithLabels(map[string]string{"key": "other"})
	assert.Equal(t, map[string]string{"key": "other"}, indexedLabelChanged.GetLabels())
	assert.NotEqual(t, node.GetNodeTypeId(), indexedLabelChanged.GetNodeTypeId())

	nonIndexedLabelChanged := node.WithLabels(map[string]string{"key": "value", "non-indexed": "bar"})
	assert.Equal(t, nonIndexedLabelChanged.GetLabels(), map[string]string{"key": "value", "non-indexed": "bar"})
	assert.Equal(t, node.GetNodeTypeId(), nonIndexedLabelChanged.GetNodeTypeId())
}

func TestWithTaints_RecomputesReservation(t *testing.T) {
	reservationTaint := v1.Taint{Key: constants.ReservationTaintKey, Value: "res-1", Effect: v1.TaintEffectNoSchedule}
	node := createNodeWithIndexedLabelsAndTaints(nil, nil)
	require.Equal(t, util.NoReservationName, node.GetReservation())

	reserved := node.WithTaints([]v1.Taint{reservationTaint})
	assert.Equal(t, "res-1", reserved.GetReservation())
	// Dropping the taint must drop the reservation with it.
	assert.Equal(t, util.NoReservationName, reserved.WithTaints(nil).GetReservation())
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

	// Use 2 CPU at priority 1 so the buckets differ (priority 1 has 8, priority 2
	// still has 10), showing unallocatable resources come off every bucket.
	node := createNode(allocatableResources, []int32{1, 2})
	node = node.WithResourcesUsedAtPriority(1, makeCpuResourceList(resourceListFactory, "2"))

	unallocatable := makeCpuResourceList(resourceListFactory, "2")
	expectedAllocatableResources := makeCpuResourceList(resourceListFactory, "8")

	result := node.MarkResourceUnallocatable(unallocatable)

	assert.Equal(t, expectedAllocatableResources, result.allocatableResources)
	assert.Equal(t, makeCpuResourceList(resourceListFactory, "6"), result.AllocatableAtPriority(1))
	assert.Equal(t, makeCpuResourceList(resourceListFactory, "8"), result.AllocatableAtPriority(2))
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

	node := createNode(allocatableResources, []int32{1, 2})
	node = node.WithResourcesUsedAtPriority(1, makeCpuResourceList(resourceListFactory, "2"))

	// Subtracting more than any bucket holds floors at zero rather than going negative.
	unallocatable := makeCpuResourceList(resourceListFactory, "9")
	expectedAllocatableResources := makeCpuResourceList(resourceListFactory, "1")

	result := node.MarkResourceUnallocatable(unallocatable)

	assert.Equal(t, expectedAllocatableResources, result.allocatableResources)
	assert.Equal(t, makeCpuResourceList(resourceListFactory, "0"), result.AllocatableAtPriority(1))
	assert.Equal(t, makeCpuResourceList(resourceListFactory, "1"), result.AllocatableAtPriority(2))
}

func TestDeepCopyNilKeysClonesUrgencyMap(t *testing.T) {
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
	require.Equal(t, node.AllocatableByPriority, node.AllocatableByPriorityNoEviction)

	cp := node.DeepCopyNilKeys()
	for p := range cp.AllocatableByPriorityNoEviction {
		delete(cp.AllocatableByPriorityNoEviction, p)
		break
	}

	// Mutated copy is mutated
	require.NotEqual(t, cp.AllocatableByPriority, cp.AllocatableByPriorityNoEviction)
	// Original still intact as copy was a deep copy and shouldn't mutate the original
	require.Equal(t, node.AllocatableByPriority, node.AllocatableByPriorityNoEviction)
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

// testAccountingNode builds a node with an empty ledger and allocatable-by-priority
// buckets at priorities 1 and 10 (plus the sentinel priorities) all equal to total
// resources.
// assertJobAllocation asserts the node has accounted for exactly expected on behalf of jobId.
func assertJobAllocation(t *testing.T, node *Node, jobId string, expected ResourceList, msgAndArgs ...interface{}) {
	t.Helper()
	actual, ok := node.AllocatedByJob()[jobId]
	require.True(t, ok, "job %s should own resources on the node", jobId)
	assert.Equal(t, expected, actual, msgAndArgs...)
}

func testAccountingNode(t *testing.T, factory *ResourceListFactory) *Node {
	t.Helper()
	total := factory.FromNodeProto(map[string]*resource.Quantity{
		"cpu":    pointer.MustParseResource("16"),
		"memory": pointer.MustParseResource("32Gi"),
	})
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

	assertJobAllocation(t, node, "job-1", requests)
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

	assert.True(t, node.IsJobEvicted("job-1"))
	assertJobAllocation(t, node, "job-1", requests, "eviction must not release ownership")
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
	before := node.AllocatableAtPriority(10)

	require.NoError(t, node.RemoveJob(job))

	_, hasJob := node.AllocatedByJob()["job-1"]
	assert.False(t, hasJob)
	assert.Equal(t, before.Add(requests), node.AllocatableAtPriority(10))
}

func TestNode_RemoveJob_AlreadyUnboundIsNoop(t *testing.T) {
	factory := testAccountingFactory(t)
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: testJobRequests(factory, "1", "1Gi"), priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}

	err := node.RemoveJob(job)
	require.NoError(t, err)
}

// Copying a node that already has jobs on it must isolate the accounting maps, because the
// copy is mutated in place afterwards while the original stays in the NodeDb index.
func TestNode_DeepCopyIsolatesAccountingFromOriginal(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	first := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests, priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}
	second := &testSchedJob{id: "job-2", queue: "queue-a", requests: requests, priorityClass: types.PriorityClass{Priority: 10, Preemptible: true}}
	require.NoError(t, node.AddJob(first, 10))

	before := node.AllocatableAtPriority(10)
	copied := node.DeepCopyNilKeys()

	require.NoError(t, copied.AddJob(second, 10))
	require.NoError(t, copied.EvictJob(first))

	// The original must see none of it.
	assert.False(t, node.HasJobAllocation("job-2"))
	assert.False(t, node.IsJobEvicted("job-1"))
	assert.Equal(t, before, node.AllocatableAtPriority(10))

	// And unbinding on the original must not disturb the copy.
	require.NoError(t, node.RemoveJob(first))
	assert.True(t, copied.HasJobAllocation("job-1"))
}

func TestNode_RemoveJob_UsesCutoffStoredAtAdd(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests}

	beforeLow := node.AllocatableAtPriority(1)
	beforeHigh := node.AllocatableAtPriority(10)

	require.NoError(t, node.AddJob(job, 1))
	require.NoError(t, node.RemoveJob(job))

	assert.Equal(t, beforeLow, node.AllocatableAtPriority(1), "bucket 1 must be restored")
	assert.Equal(t, beforeHigh, node.AllocatableAtPriority(10), "bucket 10 was never debited and must be unchanged")
}

func TestNode_RemoveJob_HighCutoffReleasesEveryBucket(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests}

	beforeLow := node.AllocatableAtPriority(1)
	beforeHigh := node.AllocatableAtPriority(10)

	require.NoError(t, node.AddJob(job, math.MaxInt32))
	assert.NotEqual(t, beforeLow, node.AllocatableAtPriority(1), "a max cutoff must debit every bucket")

	require.NoError(t, node.RemoveJob(job))

	assert.Equal(t, beforeLow, node.AllocatableAtPriority(1))
	assert.Equal(t, beforeHigh, node.AllocatableAtPriority(10))
}

func TestNode_EvictThenRemove_ReleasesAtEvictedPriority(t *testing.T) {
	factory := testAccountingFactory(t)
	requests := testJobRequests(factory, "1", "1Gi")
	node := testAccountingNode(t, factory)
	job := &testSchedJob{id: "job-1", queue: "queue-a", requests: requests}

	beforeEvicted := node.AllocatableAtPriority(EvictedPriority)
	beforeTen := node.AllocatableAtPriority(10)

	require.NoError(t, node.AddJob(job, 10))
	require.NoError(t, node.EvictJob(job))
	require.NoError(t, node.RemoveJob(job))

	assert.Equal(t, beforeEvicted, node.AllocatableAtPriority(EvictedPriority))
	assert.Equal(t, beforeTen, node.AllocatableAtPriority(10))
	assert.Empty(t, node.EvictedJobRunIds())
	assert.Empty(t, node.AllocatedByJob())
}

func createNode(allocatableResource ResourceList, allowedPriorities []int32) *Node {
	const id = "id"
	const reportingNodeType = "re"
	const pool = "pool"
	const index = uint64(1)
	const executor = "executor"
	const name = "name"
	node := CreateNode(
		id,
		index,
		executor,
		name,
		pool,
		reportingNodeType,
		nil,
		nil,
		nil,
		nil,
		false,
		allocatableResource,
		allocatableResource,
		allowedPriorities,
		nil,
	)

	return node
}
