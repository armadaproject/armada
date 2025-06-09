package jobdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

var jobSchedulingInfo = &internaltypes.JobSchedulingInfo{
	PodRequirements: &internaltypes.PodRequirements{
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":                 k8sResource.MustParse("1"),
				"storage-connections": k8sResource.MustParse("1"),
			},
		},
		Annotations: map[string]string{
			"foo": "bar",
		},
	},
}

var baseJob, _ = jobDb.NewJob(
	"test-job",
	"test-jobSet",
	"test-queue",
	2,
	jobSchedulingInfo,
	true,
	0,
	false,
	false,
	false,
	3,
	false,
	[]string{},
	0,
)

var baseRun = &JobRun{
	id:        uuid.New().String(),
	created:   3,
	executor:  "test-executor",
	running:   true,
	succeeded: true,
	failed:    true,
	cancelled: true,
	returned:  true,
}

// Test methods that only have getters
func TestJob_TestGetter(t *testing.T) {
	assert.Equal(t, baseJob.id, baseJob.Id())
	assert.Equal(t, baseJob.queue, baseJob.Queue())
	assert.Equal(t, baseJob.submittedTime, baseJob.Created())
	assert.Equal(t, jobSchedulingInfo, baseJob.JobSchedulingInfo())
	assert.Equal(t, baseJob.Annotations(), map[string]string{
		"foo": "bar",
	})
}

func TestJob_TestPriority(t *testing.T) {
	newJob := baseJob.WithPriority(3)
	assert.Equal(t, uint32(2), baseJob.Priority())
	assert.Equal(t, uint32(3), newJob.Priority())
}

func TestJob_TestRequestedPriority(t *testing.T) {
	newJob := baseJob.WithRequestedPriority(3)
	assert.Equal(t, uint32(2), baseJob.RequestedPriority())
	assert.Equal(t, uint32(3), newJob.RequestedPriority())
}

func TestJob_TestQueued(t *testing.T) {
	newJob := baseJob.WithQueued(false)
	assert.Equal(t, true, baseJob.Queued())
	assert.Equal(t, false, newJob.Queued())
}

func TestJob_QueuedVersion(t *testing.T) {
	newJob := baseJob.WithQueuedVersion(1)
	assert.Equal(t, int32(0), baseJob.QueuedVersion())
	assert.Equal(t, int32(1), newJob.QueuedVersion())
}

func TestJob_TestCancelRequested(t *testing.T) {
	newJob := baseJob.WithCancelRequested(true)
	assert.Equal(t, false, baseJob.CancelRequested())
	assert.Equal(t, true, newJob.CancelRequested())
}

func TestJob_TestCancelByJobsetRequested(t *testing.T) {
	newJob := baseJob.WithCancelByJobsetRequested(true)
	assert.Equal(t, false, baseJob.CancelByJobsetRequested())
	assert.Equal(t, true, newJob.CancelByJobsetRequested())
}

func TestJob_TestCancelled(t *testing.T) {
	newJob := baseJob.WithCancelled(true)
	assert.Equal(t, false, baseJob.Cancelled())
	assert.Equal(t, true, newJob.Cancelled())
}

func TestJob_TestSucceeded(t *testing.T) {
	newJob := baseJob.WithSucceeded(true)
	assert.Equal(t, false, baseJob.Succeeded())
	assert.Equal(t, true, newJob.Succeeded())
}

func TestJob_TestFailed(t *testing.T) {
	newJob := baseJob.WithFailed(true)
	assert.Equal(t, false, baseJob.Failed())
	assert.Equal(t, true, newJob.Failed())
}

func TestJob_TestInTerminalState(t *testing.T) {
	assert.Equal(t, false, baseJob.InTerminalState())
	assert.Equal(t, true, baseJob.WithSucceeded(true).InTerminalState())
	assert.Equal(t, true, baseJob.WithFailed(true).InTerminalState())
	assert.Equal(t, true, baseJob.WithCancelled(true).InTerminalState())
}

func TestJob_TestHasRuns(t *testing.T) {
	assert.Equal(t, false, baseJob.HasRuns())
	assert.Equal(t, true, baseJob.WithNewRun("test-executor", "test-nodeId", "nodeId", "pool", 5).HasRuns())
}

func TestJob_TestWithNewRun(t *testing.T) {
	scheduledAtPriority := int32(10)
	jobWithRun := baseJob.WithNewRun("test-executor", "test-nodeId", "nodeId", "pool", scheduledAtPriority)
	assert.Equal(t, true, jobWithRun.HasRuns())
	run := jobWithRun.LatestRun()
	created := jobDb.clock.Now()
	assert.NotNil(t, run)
	assert.Equal(
		t,
		&JobRun{
			id:                  run.id,
			jobId:               "test-job",
			created:             created.UnixNano(),
			executor:            "test-executor",
			nodeId:              "test-nodeId",
			nodeName:            "nodeId",
			pool:                "pool",
			scheduledAtPriority: &scheduledAtPriority,
			leaseTime:           &created,
		},
		run,
	)
}

func TestJob_TestWithUpdatedRun_NewRun(t *testing.T) {
	jobWithRun := baseJob.WithUpdatedRun(baseRun)
	assert.Equal(t, true, jobWithRun.HasRuns())
	retrievedRun := jobWithRun.LatestRun()
	assert.NotNil(t, baseRun)
	assert.Equal(t, baseRun, retrievedRun)
}

func TestJob_TestWithUpdatedRun_UpdateRun(t *testing.T) {
	run := &JobRun{
		id:        uuid.New().String(),
		created:   3,
		executor:  "test-executor",
		running:   true,
		succeeded: false,
		failed:    false,
		cancelled: false,
		returned:  false,
	}
	updatedRun := run.WithReturned(true)
	jobWithRun := baseJob.WithUpdatedRun(run)
	jobWithUpdatedRun := jobWithRun.WithUpdatedRun(updatedRun)
	assert.Equal(t, run, jobWithRun.LatestRun())
	assert.Equal(t, updatedRun, jobWithUpdatedRun.LatestRun())
}

func TestJob_TestWithUpdatedRun_AdditionalRun(t *testing.T) {
	additionalRun := &JobRun{
		id:       uuid.New().String(),
		created:  baseRun.created + 1,
		executor: "test-executor",
		running:  true,
	}
	jobWithRun := baseJob.WithUpdatedRun(baseRun)
	jobWithAdditionalRun := jobWithRun.WithUpdatedRun(additionalRun)
	assert.Equal(t, baseRun, jobWithRun.LatestRun())
	assert.Equal(t, additionalRun, jobWithAdditionalRun.LatestRun())
}

func TestJob_TestWithUpdatedRun_AdditionalEarlierRun(t *testing.T) {
	additionalRun := &JobRun{
		id:       uuid.New().String(),
		created:  baseRun.created - 1,
		executor: "test-executor",
		running:  true,
	}
	jobWithRun := baseJob.WithUpdatedRun(baseRun)
	jobWithAdditionalRun := jobWithRun.WithUpdatedRun(additionalRun)
	assert.Equal(t, baseRun, jobWithRun.LatestRun())
	assert.Equal(t, baseRun, jobWithAdditionalRun.LatestRun())
}

func TestJob_TestNumReturned(t *testing.T) {
	returnedRun := func() *JobRun {
		return &JobRun{
			id:       uuid.New().String(),
			created:  baseRun.created,
			returned: true,
		}
	}

	nonReturnedRun := func() *JobRun {
		return &JobRun{
			id:       uuid.New().String(),
			created:  baseRun.created,
			returned: false,
		}
	}
	// initial job has no runs
	assert.Equal(t, uint(0), baseJob.NumReturned())

	// one returned run
	returned1 := baseJob.WithUpdatedRun(returnedRun())
	assert.Equal(t, uint(1), returned1.NumReturned())

	// still one returned run
	returned2 := returned1.WithUpdatedRun(nonReturnedRun())
	assert.Equal(t, uint(1), returned2.NumReturned())

	// two returned runs
	returned3 := returned2.WithUpdatedRun(returnedRun())
	assert.Equal(t, uint(2), returned3.NumReturned())
}

func TestJob_TestNumAttempts(t *testing.T) {
	attemptedRun := func() *JobRun {
		return &JobRun{
			id:           uuid.New().String(),
			created:      baseRun.created,
			returned:     true,
			runAttempted: true,
		}
	}

	nonAttemptedRun := func() *JobRun {
		return &JobRun{
			id:           uuid.New().String(),
			created:      baseRun.created,
			returned:     true,
			runAttempted: false,
		}
	}
	// initial job has no runs
	assert.Equal(t, uint(0), baseJob.NumAttempts())

	// one returned run
	returned1 := baseJob.WithUpdatedRun(attemptedRun())
	assert.Equal(t, uint(1), returned1.NumAttempts())

	// still one returned run
	returned2 := returned1.WithUpdatedRun(nonAttemptedRun())
	assert.Equal(t, uint(1), returned2.NumAttempts())

	// two returned runs
	returned3 := returned2.WithUpdatedRun(attemptedRun())
	assert.Equal(t, uint(2), returned3.NumAttempts())
}

func TestJob_TestRunsById(t *testing.T) {
	runs := make([]*JobRun, 10)
	job := baseJob
	for i := 0; i < len(runs); i++ {
		runs[i] = &JobRun{id: uuid.New().String()}
		job = job.WithUpdatedRun(runs[i])
	}
	for i := 0; i < len(runs); i++ {
		assert.Equal(t, runs[i], job.runsById[runs[i].id])
	}
}

func TestJob_TestWithJobset(t *testing.T) {
	newJob := baseJob.WithJobset("fish")
	assert.Equal(t, "test-jobSet", baseJob.Jobset())
	assert.Equal(t, "fish", newJob.Jobset())
}

func TestJob_TestWithPriorityClass(t *testing.T) {
	pc := types.PriorityClass{
		Priority:    100,
		Preemptible: true,
	}
	newJob := baseJob.WithPriorityClass(pc)
	assert.Equal(t, types.PriorityClass{Priority: 3, Preemptible: false}, baseJob.PriorityClass())
	assert.Equal(t, pc, newJob.PriorityClass())
}

func TestJob_TestWithQueue(t *testing.T) {
	newJob := baseJob.WithQueue("fish")
	assert.Equal(t, "test-queue", baseJob.Queue())
	assert.Equal(t, "fish", newJob.Queue())
}

func TestJob_TestWithCreated(t *testing.T) {
	newJob := baseJob.WithCreated(456)
	assert.Equal(t, int64(3), baseJob.Created())
	assert.Equal(t, int64(456), newJob.Created())
}

func TestJob_DeepCopy(t *testing.T) {
	original, err := jobDb.NewJob("test-job", "test-jobSet", "test-queue", 2, jobSchedulingInfo, true, 0, false, false, false, 3, false, []string{}, 0)
	assert.Nil(t, err)
	original = original.WithUpdatedRun(baseJobRun.DeepCopy())
	expected, err := jobDb.NewJob("test-job", "test-jobSet", "test-queue", 2, jobSchedulingInfo, true, 0, false, false, false, 3, false, []string{}, 0)
	assert.Nil(t, err)
	expected = expected.WithUpdatedRun(baseJobRun.DeepCopy())

	result := original.DeepCopy()
	assert.Equal(t, expected, result)
	assert.Equal(t, expected, original)

	// Modify and confirm original hasn't changed
	result.activeRun.nodeName = "test"
	result.runsById[baseJobRun.id].nodeName = "test"
	result.queue = "test"
	result.jobSchedulingInfo.Priority = 1

	assert.NotEqual(t, expected, result)
	assert.Equal(t, expected, original)
}

func TestJob_TestWithJobSchedulingInfo(t *testing.T) {
	newSchedInfo := &internaltypes.JobSchedulingInfo{
		PodRequirements: &internaltypes.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":                 k8sResource.MustParse("2"),
					"storage-connections": k8sResource.MustParse("2"),
				},
			},
			Annotations: map[string]string{
				"fish": "chips",
			},
		},
	}
	newJob := JobWithJobSchedulingInfo(baseJob, newSchedInfo)
	assert.Equal(t, jobSchedulingInfo, baseJob.JobSchedulingInfo())
	assert.Equal(t, newSchedInfo, newJob.JobSchedulingInfo())

	assert.Equal(t, int64(1000), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(1), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
	assert.Equal(t, int64(1000), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(0), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("storage-connections"))

	assert.Equal(t, int64(2000), newJob.AllResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(2), newJob.AllResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
	assert.Equal(t, int64(2000), newJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(0), newJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
}

func TestRequestsFloatingResources(t *testing.T) {
	noFloatingResourcesJob := JobWithJobSchedulingInfo(baseJob, &internaltypes.JobSchedulingInfo{
		PodRequirements: &internaltypes.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu":                 k8sResource.MustParse("1"),
					"storage-connections": k8sResource.MustParse("0"),
				},
			},
		},
	})
	assert.True(t, baseJob.RequestsFloatingResources())
	assert.False(t, noFloatingResourcesJob.RequestsFloatingResources())
}

func TestJobSchedulingInfoFieldsInitialised(t *testing.T) {
	infoWithNilFields := &internaltypes.JobSchedulingInfo{
		PodRequirements: &internaltypes.PodRequirements{},
	}

	infoWithNilFieldsCopy := infoWithNilFields.DeepCopy()
	assert.Nil(t, infoWithNilFields.PodRequirements.NodeSelector)
	assert.Nil(t, infoWithNilFields.PodRequirements.Annotations)

	job, err := jobDb.NewJob("test-job", "test-jobSet", "test-queue", 2, infoWithNilFieldsCopy, true, 0, false, false, false, 3, false, []string{}, 0)
	assert.Nil(t, err)
	assert.NotNil(t, job.NodeSelector())
	assert.NotNil(t, job.Annotations())

	// Copy again here, as the fields get mutated so we want a clean copy
	infoWithNilFieldsCopy2 := infoWithNilFields
	updatedJob := JobWithJobSchedulingInfo(baseJob, infoWithNilFieldsCopy2)
	assert.NotNil(t, updatedJob.NodeSelector())
	assert.NotNil(t, updatedJob.Annotations())
}

func TestJob_TestResolvedPools(t *testing.T) {
	jobWithNoPool := baseJob
	jobWithPool := baseJob.WithPools([]string{"testPool"})
	jobWithJobRunPool := jobWithPool.
		WithQueued(false).
		WithNewRun("testExecutor", "testNode", "testNode", "testPool2", 1)

	// Job without pool
	assert.Equal(t, []string{}, jobWithNoPool.ResolvedPools())

	// Queued job withPool
	assert.Equal(t, []string{"testPool"}, jobWithPool.ResolvedPools())

	// Job with an active run
	assert.Equal(t, []string{"testPool2"}, jobWithJobRunPool.ResolvedPools())
}

func TestJob_TestAllResourceRequirements(t *testing.T) {
	assert.Equal(t, int64(1000), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(1), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
}

func TestJob_TestKubernetesResourceRequirements(t *testing.T) {
	assert.Equal(t, int64(1000), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(0), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
}
