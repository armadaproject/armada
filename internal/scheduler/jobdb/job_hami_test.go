package jobdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/hami"
	schedulerconfiguration "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/hamiapi"
)

func newHamiTestJobDb(t *testing.T) *JobDb {
	t.Helper()
	factory, err := internaltypes.NewResourceListFactory(
		[]schedulerconfiguration.ResourceType{
			{Name: "cpu", Resolution: resource.MustParse("1m")},
			{Name: hami.GPUResource, Resolution: resource.MustParse("1")},
			{Name: hami.GPUMemoryResource, Resolution: resource.MustParse("1")},
			{Name: hami.GPUCoreResource, Resolution: resource.MustParse("1")},
		},
		nil,
	)
	require.NoError(t, err)
	return NewJobDb(map[string]types.PriorityClass{"pc": {}}, "pc", stringinterner.New(1024), factory)
}

func newHamiTestJob(t *testing.T, jobDb *JobDb, requests map[string]string) *Job {
	t.Helper()
	resources := v1.ResourceList{}
	for name, value := range requests {
		resources[v1.ResourceName(name)] = resource.MustParse(value)
	}
	job, err := jobDb.NewJob(
		"job-1", "set", "queue", 0,
		&internaltypes.JobSchedulingInfo{
			PriorityClass: "pc",
			PodRequirements: &internaltypes.PodRequirements{
				ResourceRequirements: v1.ResourceRequirements{Requests: resources, Limits: resources},
			},
		},
		true, 0, false, false, false, 0, true, []string{"pool"}, 0,
	)
	require.NoError(t, err)
	return job
}

func deviceCharge(job *Job) (int64, int64) {
	rr := job.AllResourceRequirements()
	return rr.GetRawByNameZeroIfMissing(hami.GPUMemoryResource), rr.GetRawByNameZeroIfMissing(hami.GPUCoreResource)
}

func TestJob_HamiDeviceResourcesAreNotKubernetesResources(t *testing.T) {
	jobDb := newHamiTestJobDb(t)
	job := newHamiTestJob(t, jobDb, map[string]string{"cpu": "1", hami.GPUResource: "2", hami.GPUMemoryResource: "4096", hami.GPUCoreResource: "25"})

	request, err := job.HamiRequest()
	require.NoError(t, err)
	assert.Equal(t, hami.Request{Devices: 2, MemoryMiB: 4096, CorePercent: 25}, request)

	// The canonical charge is the explicit request across both GPUs.
	memory, cores := deviceCharge(job)
	assert.Equal(t, int64(8192), memory)
	assert.Equal(t, int64(50), cores)

	// Device resources never take part in node fit; GPU slots do.
	k8s := job.KubernetesResourceRequirements()
	assert.Equal(t, int64(0), k8s.GetRawByNameZeroIfMissing(hami.GPUMemoryResource))
	assert.Equal(t, int64(0), k8s.GetRawByNameZeroIfMissing(hami.GPUCoreResource))
	assert.Equal(t, int64(2), k8s.GetRawByNameZeroIfMissing(hami.GPUResource))
}

func TestJob_WithHamiCharge(t *testing.T) {
	jobDb := newHamiTestJobDb(t)
	reservation := []*hamiapi.DeviceAllocation{
		{Id: "gpu-0", MemoryMib: 16384, CorePercent: 100},
		{Id: "gpu-1", MemoryMib: 16384, CorePercent: 100},
	}

	queued := newHamiTestJob(t, jobDb, map[string]string{hami.GPUResource: "2"})
	memory, cores := deviceCharge(queued)
	assert.Equal(t, int64(0), memory, "canonical charge of omitted memory is unresolved")
	assert.Equal(t, int64(200), cores)

	estimated := queued.WithHamiCharge(49152)
	memory, _ = deviceCharge(estimated)
	assert.Equal(t, int64(2*49152), memory, "queued job is charged the pool mean per GPU")
	assert.Equal(t, queued.KubernetesResourceRequirements(), estimated.KubernetesResourceRequirements())
	assert.Equal(t, queued.SchedulingKey(), estimated.SchedulingKey())

	running := queued.WithQueued(false).WithNewRun("executor", "node", "node", "pool", 0)
	running, err := running.WithHamiDeviceAllocations(reservation)
	require.NoError(t, err)
	assert.Equal(t, reservation, running.LatestRun().HamiDeviceAllocations())
	for _, estimate := range []int64{49152, 0, 1} {
		// A running job is charged its reservation whatever the pool's inventory.
		memory, cores = deviceCharge(running.WithHamiCharge(estimate))
		assert.Equal(t, int64(32768), memory)
		assert.Equal(t, int64(200), cores)
	}

	// Once the run finishes and the job is requeued, it is estimated again.
	requeued := running.WithUpdatedRun(running.LatestRun().WithFailed(true)).WithQueued(true)
	memory, _ = deviceCharge(requeued.WithHamiCharge(1000))
	assert.Equal(t, int64(2000), memory)

	// Jobs without GPUs are returned unchanged.
	cpuJob := newHamiTestJob(t, jobDb, map[string]string{"cpu": "1"})
	assert.Same(t, cpuJob, cpuJob.WithHamiCharge(49152))
}

func TestJob_InvalidHamiRequest(t *testing.T) {
	jobDb := newHamiTestJobDb(t)
	job := newHamiTestJob(t, jobDb, map[string]string{hami.GPUMemoryResource: "4096"})
	_, err := job.HamiRequest()
	assert.Error(t, err)
	assert.True(t, job.RequestsHamiDevices())
	memory, cores := deviceCharge(job)
	assert.Zero(t, memory)
	assert.Zero(t, cores)
}

func TestJobRun_HamiDeviceAllocations(t *testing.T) {
	allocations := []*hamiapi.DeviceAllocation{{Id: "gpu-0", MemoryMib: 16384, CorePercent: 100}}
	run := MinimalRun("run", 0)
	withAllocations := run.WithHamiDeviceAllocations(allocations)

	assert.Nil(t, run.HamiDeviceAllocations())
	assert.Equal(t, allocations, withAllocations.HamiDeviceAllocations())
	assert.False(t, run.Equal(withAllocations))
	assert.True(t, withAllocations.Equal(run.WithHamiDeviceAllocations(allocations)))

	// The run keeps its own copy.
	allocations[0].Id = "mutated"
	assert.Equal(t, "gpu-0", withAllocations.HamiDeviceAllocations()[0].Id)
	assert.Equal(t, "gpu-0", withAllocations.DeepCopy().HamiDeviceAllocations()[0].Id)
}

func TestReconcileJobDifferences_RecoversHamiReservationFromOverlay(t *testing.T) {
	jobDb := newHamiTestJobDb(t)
	allocations := []*hamiapi.DeviceAllocation{
		{Id: "gpu-0", MemoryMib: 16384, CorePercent: 100},
		{Id: "gpu-1", MemoryMib: 81920, CorePercent: 100},
	}
	overlay := protoutil.MustMarshall(&schedulerobjects.PodRequirements{
		Tolerations:           []*v1.Toleration{{Key: "gang", Operator: v1.TolerationOpExists}},
		Annotations:           map[string]string{"gang": "value"},
		HamiDeviceAllocations: allocations,
	})
	dbJob := &database.Job{JobID: "job-1", JobSet: "set", Queue: "queue", SchedulingInfo: testSchedulingInfoBytes}
	dbRun := &database.Run{RunID: "run-1", JobID: "job-1", Executor: "executor", Node: "node", Pool: "pool", Running: true, PodRequirementsOverlay: overlay}

	jst, err := jobDb.reconcileJobDifferences(nil, dbJob, []*database.Run{dbRun})
	require.NoError(t, err)
	run := jst.Job.RunById("run-1")
	require.NotNil(t, run)
	assert.Equal(t, allocations, run.HamiDeviceAllocations())

	// A run without an overlay has no reservation.
	dbRun.RunID = "run-2"
	dbRun.PodRequirementsOverlay = nil
	jst, err = jobDb.reconcileJobDifferences(nil, dbJob, []*database.Run{dbRun})
	require.NoError(t, err)
	assert.Nil(t, jst.Job.RunById("run-2").HamiDeviceAllocations())

	// A corrupt overlay is an error rather than a silently lost reservation.
	dbRun.RunID = "run-3"
	dbRun.PodRequirementsOverlay = []byte{0xff, 0xff}
	_, err = jobDb.reconcileJobDifferences(nil, dbJob, []*database.Run{dbRun})
	assert.Error(t, err)
}
