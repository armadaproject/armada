package scheduling

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/hami"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/hamiapi"
)

// hamiGangSchedulerFixture is a HAMi pool with two single-GPU nodes: node-small
// with a 16 GiB GPU and node-large with an 80 GiB GPU. The pool's mean GPU
// memory is 48 GiB.
type hamiGangSchedulerFixture struct {
	jobDb     *jobdb.JobDb
	nodeDb    *nodedb.NodeDb
	sctx      *context.SchedulingContext
	scheduler *GangScheduler
}

func newHamiGangSchedulerFixture(t *testing.T, gpuMemoryFractionLimit float64, occupied ...string) *hamiGangSchedulerFixture {
	t.Helper()
	factory, err := internaltypes.NewResourceListFactory([]configuration.ResourceType{
		{Name: "cpu", Resolution: resource.MustParse("1m")},
		{Name: "memory", Resolution: resource.MustParse("1")},
		{Name: hami.GPUResource, Resolution: resource.MustParse("1")},
		{Name: hami.GPUMemoryResource, Resolution: resource.MustParse("1")},
		{Name: hami.GPUCoreResource, Resolution: resource.MustParse("1")},
	}, nil)
	require.NoError(t, err)
	priorityClasses := map[string]types.PriorityClass{
		"pc": {Priority: 1, Preemptible: true, MaximumResourceFractionPerQueue: map[string]float64{hami.GPUMemoryResource: gpuMemoryFractionLimit}},
	}
	config := configuration.SchedulingConfig{
		PriorityClasses:                             priorityClasses,
		DefaultPriorityClassName:                    "pc",
		DominantResourceFairnessResourcesToConsider: []string{"cpu", hami.GPUMemoryResource, hami.GPUCoreResource},
		MaximumSchedulingRate:                       1000,
		MaximumSchedulingBurst:                      1000,
		MaximumPerQueueSchedulingRate:               1000,
		MaximumPerQueueSchedulingBurst:              1000,
	}
	jobDb := jobdb.NewJobDb(priorityClasses, "pc", stringinterner.New(1024), factory)

	nodeResources := factory.FromNodeProto(map[string]*resource.Quantity{
		"cpu": resource.NewQuantity(64, resource.DecimalSI), "memory": resource.NewQuantity(1<<40, resource.BinarySI),
		hami.GPUResource: resource.NewQuantity(4, resource.DecimalSI),
	})
	nodeDb, err := nodedb.NewNodeDb(priorityClasses, []configuration.ResourceType{{Name: "cpu", Resolution: resource.MustParse("1")}}, nil, nil, nil, factory)
	require.NoError(t, err)
	txn := nodeDb.Txn(true)
	for i, gpu := range []struct {
		id        string
		memoryMiB int64
	}{{"gpu-small", 16384}, {"gpu-large", 81920}} {
		name := "node-" + strings.TrimPrefix(gpu.id, "gpu-")
		node := internaltypes.CreateNodeAndType(name, uint64(i), "executor", name, "pool", "type", false, nil,
			map[string]string{configuration.NodeIdLabel: name}, nil, nil, nodeResources, nodeResources, []int32{0, 1},
		).WithHamiInventory(&hamiapi.NodeInventory{
			Status:  hamiapi.InventoryStatus_INVENTORY_STATUS_USABLE,
			Devices: []*hamiapi.DeviceInfo{{Id: gpu.id, SlotCount: 4, MemoryMib: gpu.memoryMiB, CorePercent: 100, Mode: hami.SupportedMode, Healthy: true, Usable: true}},
		}).WithHamiPool()
		if slices.Contains(occupied, gpu.id) {
			node = node.WithReservedHamiUsage(hami.Usage{gpu.id: {Slots: 1, MemoryMiB: 1, CorePercent: 1}})
		}
		require.NoError(t, nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, nil, node))
	}
	txn.Commit()

	totalResources := nodeDb.TotalKubernetesResources().Add(nodeDb.TotalHamiDeviceResources())
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, "pool", config)
	require.NoError(t, err)
	sctx := context.NewSchedulingContext("pool", fairnessCostProvider, rate.NewLimiter(1000, 1000), rate.NewLimiter(1000, 1000), totalResources)
	require.NoError(t, sctx.AddQueueSchedulingContext("queue", 1, 1, nil,
		internaltypes.ResourceList{}, internaltypes.ResourceList{}, internaltypes.ResourceList{}, rate.NewLimiter(1000, 1000)))
	constraints := schedulerconstraints.NewSchedulingConstraints("pool", totalResources, config, []*api.Queue{{Name: "queue"}})
	floatingResourceTypes, err := floatingresources.NewFloatingResourceTypes(nil, factory)
	require.NoError(t, err)
	scheduler, err := NewGangScheduler(sctx, constraints, floatingResourceTypes, nodeDb, false)
	require.NoError(t, err)
	return &hamiGangSchedulerFixture{jobDb: jobDb, nodeDb: nodeDb, sctx: sctx, scheduler: scheduler}
}

func (f *hamiGangSchedulerFixture) wholeGpuJob(t *testing.T) *jobdb.Job {
	t.Helper()
	requests := v1.ResourceList{"cpu": resource.MustParse("1"), hami.GPUResource: resource.MustParse("1")}
	job, err := f.jobDb.NewJob(util.NewULID(), "set", "queue", 0, &internaltypes.JobSchedulingInfo{
		PriorityClass:   "pc",
		PodRequirements: &internaltypes.PodRequirements{ResourceRequirements: v1.ResourceRequirements{Requests: requests, Limits: requests}},
	}, true, 0, false, false, false, 0, true, []string{"pool"}, 0)
	require.NoError(t, err)
	// The pool mean of a 16 GiB and an 80 GiB GPU.
	return job.WithHamiCharge(49152)
}

func allocatedGpuMemory(sctx *context.SchedulingContext) int64 {
	return sctx.QueueSchedulingContexts["queue"].Allocated.GetRawByNameZeroIfMissing(hami.GPUMemoryResource)
}

func TestGangScheduler_ChargesTheChosenHamiGPUs(t *testing.T) {
	f := newHamiGangSchedulerFixture(t, 1.0)
	jctx := context.JobSchedulingContextFromJob(f.wholeGpuJob(t))
	ok, reason, err := f.scheduler.Schedule(armadacontext.Background(), context.NewGangSchedulingContext([]*context.JobSchedulingContext{jctx}))
	require.NoError(t, err)
	require.True(t, ok, reason)

	require.Len(t, jctx.PodSchedulingContext.HamiDeviceAllocations, 1)
	assert.Equal(t, "gpu-small", jctx.PodSchedulingContext.HamiDeviceAllocations[0].Id)
	// Queued at the 48 GiB estimate, charged the 16 GiB it was given.
	assert.Equal(t, int64(16384), allocatedGpuMemory(f.sctx))
	assert.Equal(t, int64(16384), f.sctx.Allocated.GetRawByNameZeroIfMissing(hami.GPUMemoryResource))
}

func TestGangScheduler_RechecksQueueLimitsAgainstTheChosenHamiGPUs(t *testing.T) {
	// The queue may use 60% of the pool's 96 GiB of GPU memory. The 48 GiB
	// estimate fits, but only the 80 GiB GPU is free.
	f := newHamiGangSchedulerFixture(t, 0.6, "gpu-small")
	jctx := context.JobSchedulingContextFromJob(f.wholeGpuJob(t))
	ok, reason, err := f.scheduler.Schedule(armadacontext.Background(), context.NewGangSchedulingContext([]*context.JobSchedulingContext{jctx}))
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, schedulerconstraints.UnschedulableReasonMaximumResourcesExceeded, reason)
	assert.Zero(t, allocatedGpuMemory(f.sctx), "no charge is left behind")

	node, err := f.nodeDb.GetNode("node-large")
	require.NoError(t, err)
	assert.Nil(t, node.HamiDeviceAllocations(jctx.JobId), "no reservation is left behind")
}
