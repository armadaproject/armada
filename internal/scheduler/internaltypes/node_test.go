package internaltypes

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/hami"
	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/pkg/hamiapi"
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
	id       string
	queue    string
	requests ResourceList
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

func testAccountingNodeTotal(factory *ResourceListFactory) ResourceList {
	return factory.FromNodeProto(map[string]*resource.Quantity{
		"cpu":    pointer.MustParseResource("16"),
		"memory": pointer.MustParseResource("32Gi"),
	})
}

// testAccountingJobRequests is the request size of every job used by the accounting tests, so that
// expectations can be written as a count of jobs consumed per bucket.
func testAccountingJobRequests(factory *ResourceListFactory) ResourceList {
	return factory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("1Gi"),
	})
}

func testAccountingJob(factory *ResourceListFactory, jobId string) *testSchedJob {
	return &testSchedJob{id: jobId, queue: "queue-a", requests: testAccountingJobRequests(factory)}
}

// testAccountingNode builds an empty node whose allocatable-by-priority buckets are the sentinel
// priorities plus 1 and 10, all equal to its total resources.
func testAccountingNode(t *testing.T, factory *ResourceListFactory) *Node {
	t.Helper()
	total := testAccountingNodeTotal(factory)
	return CreateNode(
		"node-1", 1, "executor", "node-1", "pool", "type",
		nil, nil, map[string]bool{}, map[string]bool{},
		false, total, total,
		[]int32{1, 10},
		nil,
	)
}

// boundJob describes a job already accounted for on the node before the operation under test runs.
type boundJob struct {
	id       string
	priority int32
	evicted  bool
}

func applyBoundJobs(t *testing.T, node *Node, factory *ResourceListFactory, jobs []boundJob) {
	t.Helper()
	for _, bound := range jobs {
		job := testAccountingJob(factory, bound.id)
		require.NoError(t, node.AddJob(job, bound.priority))
		if bound.evicted {
			require.NoError(t, node.EvictJob(job))
		}
	}
}

// nodeAccountingState is the expected observable accounting state of a node.
//
// used and usedNoEviction give, per priority bucket, how many jobs worth of resource that bucket has
// consumed, i.e. the bucket is expected to equal total - n*requests. A bucket left out of the map is
// expected to be fully allocatable. The two maps track the node's two resource views:
// the eviction-aware one, which gives resources back when a job is evicted, and the no-eviction one,
// which does not and is what urgency-based preemption reads.
type nodeAccountingState struct {
	used            map[int32]int
	usedNoEviction  map[int32]int
	urgencyPreempts bool
	ownedJobIds     []string
	evictedJobIds   []string
}

func assertNodeAccounting(t *testing.T, node *Node, factory *ResourceListFactory, expected nodeAccountingState) {
	t.Helper()
	total := testAccountingNodeTotal(factory)
	requests := testAccountingJobRequests(factory)

	allocatable := node.AllocatableByPriority()
	allocatableNoEviction := node.AllocatableByPriorityNoEviction()
	require.ElementsMatch(t, node.KnownPriorities(), maps.Keys(allocatable))
	require.ElementsMatch(t, node.KnownPriorities(), maps.Keys(allocatableNoEviction))

	for _, priority := range node.KnownPriorities() {
		assert.Equal(t, consume(total, requests, expected.used[priority]), node.AllocatableAtPriority(priority),
			"AllocatableAtPriority(%d) should have %d job(s) consumed", priority, expected.used[priority])
		assert.Equal(t, consume(total, requests, expected.usedNoEviction[priority]), node.AllocatableAtPriorityNoEviction(priority),
			"AllocatableAtPriorityNoEviction(%d) should have %d job(s) consumed", priority, expected.usedNoEviction[priority])

		// The map accessors must agree with the single-priority ones.
		assert.Equal(t, node.AllocatableAtPriority(priority), allocatable[priority])
		assert.Equal(t, node.AllocatableAtPriorityNoEviction(priority), allocatableNoEviction[priority])
	}

	assert.Equal(t, expected.urgencyPreempts, node.HasUrgencyPreemptibleResources(),
		"HasUrgencyPreemptibleResources is true only when a job is accounted for below the highest known priority")
	assert.ElementsMatch(t, expected.ownedJobIds, maps.Keys(node.AllocatedByJob()))
	assert.ElementsMatch(t, expected.evictedJobIds, maps.Keys(node.EvictedJobRunIds()))
	for _, jobId := range expected.ownedJobIds {
		assert.True(t, node.HasJobAllocation(jobId), "job %s should own resources", jobId)
		assert.Equal(t, requests, node.AllocatedByJob()[jobId], "job %s allocation", jobId)
	}
	for _, jobId := range expected.evictedJobIds {
		assert.True(t, node.IsJobEvicted(jobId), "job %s should be evicted", jobId)
	}
}

func consume(total ResourceList, requests ResourceList, n int) ResourceList {
	result := total
	for i := 0; i < n; i++ {
		result = result.Subtract(requests)
	}
	return result
}

// AddJob deducts the job's resources from every bucket at or below the priority it is bound at, in
// both resource views, and records ownership.
func TestNode_AddJob(t *testing.T) {
	tests := map[string]struct {
		existingJobs []boundJob
		addJobId     string
		addPriority  int32
		expectedErr  string
		expected     nodeAccountingState
	}{
		"bound at the highest known priority, so every bucket is debited": {
			addJobId:    "job-1",
			addPriority: 10,
			expected: nodeAccountingState{
				used:           map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				usedNoEviction: map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				// Nothing sits below priority 10, so nothing can be urgency-preempted.
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-1"},
			},
		},
		"bound at a low priority, so higher buckets are untouched": {
			addJobId:    "job-1",
			addPriority: 1,
			expected: nodeAccountingState{
				used:           map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1},
				usedNoEviction: map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1},
				// Bucket 10 still sees the job's resources as available, i.e. preemptable by a
				// priority 10 job.
				urgencyPreempts: true,
				ownedJobIds:     []string{"job-1"},
			},
		},
		"bound above every known bucket, so all of them are debited": {
			addJobId:    "job-1",
			addPriority: math.MaxInt32,
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-1"},
			},
		},
		"jobs at different priorities accumulate per bucket": {
			existingJobs: []boundJob{{id: "job-1", priority: 1}},
			addJobId:     "job-2",
			addPriority:  10,
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 2, CrossPoolPriority: 2, 1: 2, 10: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 2, CrossPoolPriority: 2, 1: 2, 10: 1},
				urgencyPreempts: true,
				ownedJobIds:     []string{"job-1", "job-2"},
			},
		},
		"re-adding an evicted job un-evicts it and clears the evicted bucket": {
			existingJobs: []boundJob{{id: "job-1", priority: 10, evicted: true}},
			addJobId:     "job-1",
			addPriority:  10,
			expected: nodeAccountingState{
				// Back to the state the job had before it was evicted, and no longer evicted.
				used:            map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-1"},
			},
		},
		"re-adding an evicted job at a different priority errors and changes nothing": {
			// An evicted job is only ever re-bound at the priority it was evicted from. Accepting a
			// different one would leave the job accounted for at priority 1 in the no-eviction view
			// but at 10 elsewhere, so RemoveJob would later release buckets it never debited.
			existingJobs: []boundJob{{id: "job-1", priority: 1, evicted: true}},
			addJobId:     "job-1",
			addPriority:  10,
			expectedErr:  "is evicted from node",
			expected: nodeAccountingState{
				// Still evicted at priority 1, exactly as before the rejected call.
				used:            map[int32]int{EvictedPriority: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1},
				urgencyPreempts: true,
				ownedJobIds:     []string{"job-1"},
				evictedJobIds:   []string{"job-1"},
			},
		},
		"adding a job that is already bound errors and changes nothing": {
			existingJobs: []boundJob{{id: "job-1", priority: 10}},
			addJobId:     "job-1",
			addPriority:  10,
			expectedErr:  "already has resources allocated",
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-1"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			factory := testAccountingFactory(t)
			node := testAccountingNode(t, factory)
			applyBoundJobs(t, node, factory, tc.existingJobs)

			err := node.AddJob(testAccountingJob(factory, tc.addJobId), tc.addPriority)
			if tc.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
			}

			assertNodeAccounting(t, node, factory, tc.expected)
		})
	}
}

// EvictJob moves a job's resources into the EvictedPriority bucket of the eviction-aware view,
// leaving the no-eviction view and the job's ownership alone.
func TestNode_EvictJob(t *testing.T) {
	tests := map[string]struct {
		existingJobs []boundJob
		evictJobId   string
		expectedErr  string
		expected     nodeAccountingState
	}{
		"evicting a job bound at the highest priority gives its resources back above EvictedPriority": {
			existingJobs: []boundJob{{id: "job-1", priority: 10}},
			evictJobId:   "job-1",
			expected: nodeAccountingState{
				used: map[int32]int{EvictedPriority: 1},
				// The no-eviction view keeps counting the job, which is what stops urgency-based
				// preemption from treating the give-back as free capacity.
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-1"},
				evictedJobIds:   []string{"job-1"},
			},
		},
		"evicting a job bound at a low priority": {
			existingJobs: []boundJob{{id: "job-1", priority: 1}},
			evictJobId:   "job-1",
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1},
				urgencyPreempts: true,
				ownedJobIds:     []string{"job-1"},
				evictedJobIds:   []string{"job-1"},
			},
		},
		"evicting one of two jobs leaves the other fully accounted for": {
			existingJobs: []boundJob{{id: "job-1", priority: 10}, {id: "job-2", priority: 10}},
			evictJobId:   "job-1",
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 2, CrossPoolPriority: 1, 1: 1, 10: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 2, CrossPoolPriority: 2, 1: 2, 10: 2},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-1", "job-2"},
				evictedJobIds:   []string{"job-1"},
			},
		},
		"evicting a job that is not bound errors and changes nothing": {
			evictJobId:  "ghost",
			expectedErr: "no resources allocated",
			expected:    nodeAccountingState{},
		},
		"evicting an already-evicted job errors and changes nothing": {
			existingJobs: []boundJob{{id: "job-1", priority: 10, evicted: true}},
			evictJobId:   "job-1",
			expectedErr:  "already evicted",
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-1"},
				evictedJobIds:   []string{"job-1"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			factory := testAccountingFactory(t)
			node := testAccountingNode(t, factory)
			applyBoundJobs(t, node, factory, tc.existingJobs)

			err := node.EvictJob(testAccountingJob(factory, tc.evictJobId))
			if tc.expectedErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
			}

			assertNodeAccounting(t, node, factory, tc.expected)
		})
	}
}

// RemoveJob releases a job's resources from both views and drops its ownership. It uses the priority
// stored when the job was added, and releases from EvictedPriority if the job was evicted.
func TestNode_RemoveJob(t *testing.T) {
	tests := map[string]struct {
		existingJobs []boundJob
		removeJobId  string
		expected     nodeAccountingState
	}{
		"removing a bound job releases every bucket it debited": {
			existingJobs: []boundJob{{id: "job-1", priority: 10}},
			removeJobId:  "job-1",
			expected:     nodeAccountingState{},
		},
		"removing a low-priority job leaves a higher-priority job intact": {
			existingJobs: []boundJob{{id: "job-1", priority: 1}, {id: "job-2", priority: 10}},
			removeJobId:  "job-1",
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-2"},
			},
		},
		"removing an evicted job releases the evicted bucket and the no-eviction view": {
			existingJobs: []boundJob{{id: "job-1", priority: 10, evicted: true}},
			removeJobId:  "job-1",
			expected:     nodeAccountingState{},
		},
		"removing a job bound above every bucket releases them all": {
			existingJobs: []boundJob{{id: "job-2", priority: 10}, {id: "job-1", priority: math.MaxInt32}},
			removeJobId:  "job-1",
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1, 10: 1},
				urgencyPreempts: false,
				ownedJobIds:     []string{"job-2"},
			},
		},
		"removing a job that was never bound is a no-op": {
			removeJobId: "job-1",
			expected:    nodeAccountingState{},
		},
		"removing a job that was never bound leaves other jobs alone": {
			existingJobs: []boundJob{{id: "job-2", priority: 1}},
			removeJobId:  "job-1",
			expected: nodeAccountingState{
				used:            map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1},
				usedNoEviction:  map[int32]int{EvictedPriority: 1, CrossPoolPriority: 1, 1: 1},
				urgencyPreempts: true,
				ownedJobIds:     []string{"job-2"},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			factory := testAccountingFactory(t)
			node := testAccountingNode(t, factory)
			applyBoundJobs(t, node, factory, tc.existingJobs)

			require.NoError(t, node.RemoveJob(testAccountingJob(factory, tc.removeJobId)))

			assertNodeAccounting(t, node, factory, tc.expected)
		})
	}
}

// A job's view of its requests can change after it is bound. Evicting, rebinding and removing it
// must move exactly the amount recorded at bind time, so the node's accounting stays balanced.
func TestNode_UsesAmountRecordedAtBind(t *testing.T) {
	tests := map[string]func(t *testing.T, node *Node, job *testSchedJob){
		"remove": func(t *testing.T, node *Node, job *testSchedJob) {
			require.NoError(t, node.RemoveJob(job))
		},
		"evict then remove": func(t *testing.T, node *Node, job *testSchedJob) {
			require.NoError(t, node.EvictJob(job))
			require.NoError(t, node.RemoveJob(job))
		},
		"evict, rebind, remove": func(t *testing.T, node *Node, job *testSchedJob) {
			require.NoError(t, node.EvictJob(job))
			require.NoError(t, node.AddJob(job, 10))
			assert.Equal(t, testAccountingJobRequests(job.requests.factory), node.AllocatedByJob()[job.id])
			require.NoError(t, node.RemoveJob(job))
		},
	}
	for name, operations := range tests {
		t.Run(name, func(t *testing.T) {
			factory := testAccountingFactory(t)
			node := testAccountingNode(t, factory)
			job := testAccountingJob(factory, "job-1")
			require.NoError(t, node.AddJob(job, 10))

			// Double the job's requests after binding.
			job.requests = job.requests.Add(job.requests)
			operations(t, node, job)

			assertNodeAccounting(t, node, factory, nodeAccountingState{})
		})
	}
}

// Copying a node that already has jobs on it must isolate the accounting maps, because the
// copy is mutated in place afterwards while the original stays in the NodeDb index.
func TestNode_DeepCopyIsolatesAccountingFromOriginal(t *testing.T) {
	factory := testAccountingFactory(t)
	node := testAccountingNode(t, factory)
	first := testAccountingJob(factory, "job-1")
	second := testAccountingJob(factory, "job-2")
	// job-3 is left evicted so the two resource views hold different values, which catches a copy
	// that clones one of the two maps twice.
	third := testAccountingJob(factory, "job-3")
	require.NoError(t, node.AddJob(first, 10))
	require.NoError(t, node.AddJob(third, 10))
	require.NoError(t, node.EvictJob(third))
	require.NotEqual(t, node.AllocatableAtPriority(10), node.AllocatableAtPriorityNoEviction(10))

	before := node.AllocatableAtPriority(10)
	beforeNoEviction := node.AllocatableAtPriorityNoEviction(10)
	copied := node.DeepCopyNilKeys()
	require.Equal(t, before, copied.AllocatableAtPriority(10))
	require.Equal(t, beforeNoEviction, copied.AllocatableAtPriorityNoEviction(10))

	require.NoError(t, copied.AddJob(second, 10))
	require.NoError(t, copied.EvictJob(first))

	// The original must see none of it.
	assert.False(t, node.HasJobAllocation("job-2"))
	assert.False(t, node.IsJobEvicted("job-1"))
	assert.Equal(t, before, node.AllocatableAtPriority(10))
	assert.Equal(t, beforeNoEviction, node.AllocatableAtPriorityNoEviction(10))

	// And unbinding on the original must not disturb the copy.
	require.NoError(t, node.RemoveJob(first))
	assert.True(t, copied.HasJobAllocation("job-1"))
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

func TestNode_HamiDeviceUsage(t *testing.T) {
	factory := testAccountingFactory(t)
	node := testAccountingNode(t, factory).WithHamiInventory(&hamiapi.NodeInventory{
		Status:  hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE,
		Devices: []*hamiapi.DeviceInfo{{Id: "gpu-0", SlotCount: 4, MemoryMib: 16384, CorePercent: 100, Healthy: true, Usable: true}},
	})
	low, high, evicted := testAccountingJob(factory, "low"), testAccountingJob(factory, "high"), testAccountingJob(factory, "evicted")
	for _, bound := range []struct {
		job      *testSchedJob
		priority int32
	}{{low, 1}, {high, 10}, {evicted, 10}} {
		require.NoError(t, node.AddJob(bound.job, bound.priority))
		node.SetHamiDeviceAllocations(bound.job.id, []*hamiapi.DeviceAllocation{{Id: "gpu-0", MemoryMib: 4096, CorePercent: 25}})
	}
	require.NoError(t, node.EvictJob(evicted))
	node = node.WithReservedHamiUsage(hami.Usage{"gpu-0": {Slots: 1, MemoryMiB: 1024, CorePercent: 10}})

	memory := func(priority int32, urgency bool, exclude string) int64 {
		return node.HamiDeviceUsage(priority, urgency, exclude)["gpu-0"].MemoryMiB
	}
	// Other pools' reservations always count; bound jobs count at or above their priority.
	assert.Equal(t, int64(1024+4096+4096+4096), memory(EvictedPriority, false, ""))
	assert.Equal(t, int64(1024+4096+4096), memory(1, false, ""), "the evicted job is released above EvictedPriority")
	assert.Equal(t, int64(1024+4096), memory(10, false, ""), "lower-priority jobs are released for higher priorities")
	assert.Equal(t, int64(1024+4096+4096), memory(10, true, ""), "with urgency preemption the evicted job still counts")
	assert.Equal(t, int64(1024+4096+4096), memory(EvictedPriority, false, "low"))
	assert.Empty(t, node.HamiOversubscribedPriorities())

	// Rebinding keeps the evicted job's GPUs; removing a job releases them.
	require.NoError(t, node.AddJob(evicted, 10))
	assert.NotNil(t, node.HamiDeviceAllocations("evicted"))
	copied := node.DeepCopyNilKeys()
	require.NoError(t, copied.RemoveJob(evicted))
	assert.Nil(t, copied.HamiDeviceAllocations("evicted"))
	assert.NotNil(t, node.HamiDeviceAllocations("evicted"), "copies do not share the ledger")

	// A job placed onto GPUs held by lower-priority jobs oversubscribes them at the lower priorities.
	urgent := testAccountingJob(factory, "urgent")
	require.NoError(t, node.AddJob(urgent, 10))
	node.SetHamiDeviceAllocations("urgent", []*hamiapi.DeviceAllocation{{Id: "gpu-0", MemoryMib: 4096, CorePercent: 25}})
	assert.ElementsMatch(t, []int32{CrossPoolPriority, 1}, node.HamiOversubscribedPriorities())
}
