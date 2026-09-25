package nodedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/hami"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/hamiapi"
)

// hamiTestNode is an 8-slot GPU node with two 16 GiB HAMi GPUs of 4 slots each.
func hamiTestNode(hamiPool bool) *internaltypes.Node {
	inventory := &hamiapi.NodeInventory{
		Status: hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE,
		Devices: []*hamiapi.DeviceInfo{
			{Id: "gpu-0", SlotCount: 4, MemoryMib: 16384, CorePercent: 100, Mode: hami.SupportedMode, Healthy: true, Usable: true},
			{Id: "gpu-1", SlotCount: 4, MemoryMib: 16384, CorePercent: 100, Mode: hami.SupportedMode, Healthy: true, Usable: true},
		},
	}
	node := testfixtures.Test8GpuNode(testfixtures.TestPriorities).WithHamiInventory(inventory)
	if hamiPool {
		node = node.WithHamiPool()
	}
	return node
}

func hamiTestJob(priorityClass string, requests map[string]string) *jobdb.Job {
	list := v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")}
	for name, value := range requests {
		list[v1.ResourceName(name)] = resource.MustParse(value)
	}
	return testfixtures.TestJob("queue", util.ULID(), priorityClass, &internaltypes.PodRequirements{
		ResourceRequirements: v1.ResourceRequirements{Requests: list, Limits: list},
	})
}

func quarterGpuJob(priorityClass string) *jobdb.Job {
	return hamiTestJob(priorityClass, map[string]string{hami.GPUResource: "1", hami.GPUMemoryResource: "4096", hami.GPUCoreResource: "25"})
}

func wholeGpuJob(priorityClass string) *jobdb.Job {
	return hamiTestJob(priorityClass, map[string]string{hami.GPUResource: "1"})
}

// runningOn returns job running on node with the given GPU reservation.
func runningOn(t *testing.T, job *jobdb.Job, node *internaltypes.Node, allocations ...*hamiapi.DeviceAllocation) *jobdb.Job {
	t.Helper()
	job = job.WithQueued(false).WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), job.PriorityClass().Priority)
	job, err := job.WithHamiDeviceAllocations(allocations)
	require.NoError(t, err)
	return job
}

func newHamiNodeDb(t *testing.T, node *internaltypes.Node, jobs []*jobdb.Job, opts ...func(*NodeDb)) *NodeDb {
	t.Helper()
	nodeDb, err := newNodeDbWithNodes(nil, opts...)
	require.NoError(t, err)
	txn := nodeDb.Txn(true)
	require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobs, node))
	txn.Commit()
	return nodeDb
}

func scheduleOne(t *testing.T, nodeDb *NodeDb, jctx *context.JobSchedulingContext) (bool, []*JobPreemptionInfo, *internaltypes.Node) {
	t.Helper()
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	ok, preempted, err := nodeDb.ScheduleManyWithTxn(txn, context.NewGangSchedulingContext([]*context.JobSchedulingContext{jctx}))
	require.NoError(t, err)
	if !ok {
		return false, nil, nil
	}
	node, err := nodeDb.GetNodeWithTxn(txn, jctx.PodSchedulingContext.NodeId)
	require.NoError(t, err)
	return true, preempted, node
}

func TestHamiRetainedRunKeepsExactAllocation(t *testing.T) {
	node := hamiTestNode(true)
	// Best fit on an otherwise empty node would pick gpu-0; the running pod is on gpu-1.
	incumbent := runningOn(t, quarterGpuJob(testfixtures.PriorityClass0), node, &hamiapi.DeviceAllocation{Id: "gpu-1", MemoryMib: 4096, CorePercent: 25})
	other := runningOn(t, quarterGpuJob(testfixtures.PriorityClass0), node, &hamiapi.DeviceAllocation{Id: "gpu-0", MemoryMib: 4096, CorePercent: 25})
	nodeDb := newHamiNodeDb(t, node, []*jobdb.Job{incumbent, other})
	evictJobs(t, nodeDb, []*jobdb.Job{incumbent, other}, node.GetId(), 0)

	jctx := context.JobSchedulingContextFromJob(incumbent)
	jctx.SetAssignedNode(node)
	ok, _, boundNode := scheduleOne(t, nodeDb, jctx)
	require.True(t, ok)
	assert.Equal(t, incumbent.LatestRun().HamiDeviceAllocations(), jctx.PodSchedulingContext.HamiDeviceAllocations)
	assert.Equal(t, incumbent.LatestRun().HamiDeviceAllocations(), boundNode.HamiDeviceAllocations(incumbent.Id()))
	assert.Equal(t, other.LatestRun().HamiDeviceAllocations(), boundNode.HamiDeviceAllocations(other.Id()), "other evicted job keeps its GPUs")
}

func TestHamiRetainedRunThatNoLongerFitsIsNotRescheduled(t *testing.T) {
	node := hamiTestNode(true)
	incumbent := runningOn(t, wholeGpuJob(testfixtures.PriorityClass0), node, &hamiapi.DeviceAllocation{Id: "gpu-0", MemoryMib: 16384, CorePercent: 100})
	nodeDb := newHamiNodeDb(t, node, []*jobdb.Job{incumbent})
	evictJobs(t, nodeDb, []*jobdb.Job{incumbent}, node.GetId(), 0)

	// A higher-priority job takes gpu-0 while the incumbent is evicted.
	storedNode, err := nodeDb.GetNode(node.GetId())
	require.NoError(t, err)
	usurper := wholeGpuJob(testfixtures.PriorityClass1)
	storedNode, err = nodeDb.BindJobToNode(storedNode, usurper, 1)
	require.NoError(t, err)
	storedNode.SetHamiDeviceAllocations(usurper.Id(), []*hamiapi.DeviceAllocation{{Id: "gpu-0", MemoryMib: 16384, CorePercent: 100}})
	require.NoError(t, nodeDb.Upsert(storedNode))

	jctx := context.JobSchedulingContextFromJob(incumbent)
	jctx.SetAssignedNode(node)
	ok, _, _ := scheduleOne(t, nodeDb, jctx)
	assert.False(t, ok, "the incumbent's pod is on gpu-0, so it cannot move to gpu-1")
}

func TestHamiUrgencyPreemption(t *testing.T) {
	for name, urgencyFirst := range map[string]bool{"fair share first": false, "urgency first": true} {
		t.Run(name, func(t *testing.T) {
			node := hamiTestNode(true)
			var jobs []*jobdb.Job
			for _, id := range []string{"gpu-0", "gpu-1"} {
				for range 4 {
					jobs = append(jobs, runningOn(t, quarterGpuJob(testfixtures.PriorityClass0), node, &hamiapi.DeviceAllocation{Id: id, MemoryMib: 4096, CorePercent: 25}))
				}
			}
			nodeDb := newHamiNodeDb(t, node, jobs, func(nodeDb *NodeDb) {
				nodeDb.ConfigureScheduling(SchedulingOptions{UrgencyBeforeFairsharePreemption: urgencyFirst})
			})

			jctx := context.JobSchedulingContextFromJob(quarterGpuJob(testfixtures.PriorityClass1))
			ok, _, boundNode := scheduleOne(t, nodeDb, jctx)
			require.True(t, ok, "a higher-priority job can take GPUs held by lower-priority jobs")
			require.Len(t, jctx.PodSchedulingContext.HamiDeviceAllocations, 1)
			// The GPUs are now oversubscribed at the lower priority, so the jobs there are evicted.
			assert.Contains(t, boundNode.HamiOversubscribedPriorities(), int32(0))
			assert.NotContains(t, boundNode.HamiOversubscribedPriorities(), int32(1))
		})
	}
}

func TestHamiFairSharePreemptionFreesTheChosenGPU(t *testing.T) {
	node := hamiTestNode(true)
	first := runningOn(t, wholeGpuJob(testfixtures.PriorityClass0), node, &hamiapi.DeviceAllocation{Id: "gpu-0", MemoryMib: 16384, CorePercent: 100})
	second := runningOn(t, wholeGpuJob(testfixtures.PriorityClass0), node, &hamiapi.DeviceAllocation{Id: "gpu-1", MemoryMib: 16384, CorePercent: 100})
	nodeDb := newHamiNodeDb(t, node, []*jobdb.Job{first, second})
	evictJobs(t, nodeDb, []*jobdb.Job{first, second}, node.GetId(), 0)

	jctx := context.JobSchedulingContextFromJob(wholeGpuJob(testfixtures.PriorityClass0))
	ok, preempted, boundNode := scheduleOne(t, nodeDb, jctx)
	require.True(t, ok)
	// The last evicted job is preempted first, and its GPU is the one reserved.
	require.Len(t, preempted, 1)
	assert.Equal(t, second.Id(), preempted[0].PreemptedJob.JobId)
	require.Len(t, jctx.PodSchedulingContext.HamiDeviceAllocations, 1)
	assert.Equal(t, "gpu-1", jctx.PodSchedulingContext.HamiDeviceAllocations[0].Id)
	assert.Nil(t, boundNode.HamiDeviceAllocations(second.Id()))
}

func TestHamiRequirementsMet(t *testing.T) {
	unavailable := &hamiapi.NodeInventory{Status: hamiapi.InventoryStatus_INVENTORY_STATUS_UNAVAILABLE}
	invalid := &hamiapi.NodeInventory{Status: hamiapi.InventoryStatus_INVENTORY_STATUS_INVALID}
	ordinary := testfixtures.Test8GpuNode(testfixtures.TestPriorities)
	invalidJob := hamiTestJob(testfixtures.PriorityClass0, map[string]string{hami.GPUResource: "1", hami.GPUMemoryResource: "4Gi"})

	tests := map[string]struct {
		node     *internaltypes.Node
		job      *jobdb.Job
		expected bool
	}{
		"HAMi pool, usable HAMi node":                      {node: hamiTestNode(true), job: quarterGpuJob(testfixtures.PriorityClass0), expected: true},
		"HAMi pool, CPU job on ordinary node":              {node: ordinary.WithHamiPool(), job: testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass0), expected: true},
		"HAMi pool, GPU job on ordinary node":              {node: ordinary.WithHamiPool(), job: wholeGpuJob(testfixtures.PriorityClass0)},
		"HAMi pool, node with no usable GPU":               {node: ordinary.WithHamiInventory(unavailable).WithHamiPool(), job: wholeGpuJob(testfixtures.PriorityClass0)},
		"HAMi pool, invalid inventory":                     {node: ordinary.WithHamiInventory(invalid).WithHamiPool(), job: wholeGpuJob(testfixtures.PriorityClass0)},
		"HAMi pool, invalid device request":                {node: hamiTestNode(true), job: invalidJob},
		"ordinary pool, GPU job on ordinary node":          {node: ordinary, job: wholeGpuJob(testfixtures.PriorityClass0), expected: true},
		"ordinary pool, GPU job on HAMi node":              {node: hamiTestNode(false), job: wholeGpuJob(testfixtures.PriorityClass0)},
		"ordinary pool, invalid inventory is not ordinary": {node: ordinary.WithHamiInventory(invalid), job: wholeGpuJob(testfixtures.PriorityClass0)},
		"ordinary pool, HAMi amounts on ordinary node":     {node: ordinary, job: quarterGpuJob(testfixtures.PriorityClass0)},
		"ordinary pool, CPU job on HAMi node":              {node: hamiTestNode(false), job: testfixtures.Test1Cpu4GiJob("queue", testfixtures.PriorityClass0), expected: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			matches, reason := HamiRequirementsMet(tc.node, context.JobSchedulingContextFromJob(tc.job))
			assert.Equal(t, tc.expected, matches)
			if !tc.expected {
				assert.NotEmpty(t, reason.String())
			}
		})
	}
}
