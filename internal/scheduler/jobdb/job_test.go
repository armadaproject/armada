package jobdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	armadaconfiguration "github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/preemption"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/pkg/bidstore"
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
		Tolerations: []v1.Toleration{
			{
				Key:   armadaconfiguration.ReservationTaintKey,
				Value: "reservation-1",
			},
		},
	},
}

var gangJobSchedulingInfo = &internaltypes.JobSchedulingInfo{
	PodRequirements: &internaltypes.PodRequirements{
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":                 k8sResource.MustParse("1"),
				"storage-connections": k8sResource.MustParse("1"),
			},
		},
		Annotations: map[string]string{
			"foo": "bar",
			armadaconfiguration.GangCardinalityAnnotation:         "2",
			armadaconfiguration.GangNodeUniformityLabelAnnotation: "uniformity",
			armadaconfiguration.GangIdAnnotation:                  "id",
		},
	},
}

var preemptibleJobSchedulingInfo = &internaltypes.JobSchedulingInfo{
	PriorityClass: PriorityClass0,
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

var jobSchedulingInfoWithRetryEnabled = &internaltypes.JobSchedulingInfo{
	PodRequirements: &internaltypes.PodRequirements{
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":                 k8sResource.MustParse("1"),
				"storage-connections": k8sResource.MustParse("1"),
			},
		},
		Annotations: map[string]string{
			armadaconfiguration.PreemptionRetryEnabledAnnotation:  "true",
			armadaconfiguration.PreemptionMaxRetryCountAnnotation: "1",
		},
	},
}

var jobSchedulingInfoWithRetryDisabled = &internaltypes.JobSchedulingInfo{
	PodRequirements: &internaltypes.PodRequirements{
		ResourceRequirements: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu":                 k8sResource.MustParse("1"),
				"storage-connections": k8sResource.MustParse("1"),
			},
		},
		Annotations: map[string]string{
			armadaconfiguration.PreemptionRetryEnabledAnnotation: "false",
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
	int32(1),
)

var preemptibleJob, _ = jobDb.NewJob(
	"test-job",
	"test-jobSet",
	"test-queue",
	2,
	preemptibleJobSchedulingInfo,
	true,
	0,
	false,
	false,
	false,
	3,
	false,
	[]string{},
	int32(1),
)

var baseJobWithRetryEnabled, _ = jobDb.NewJob(
	"test-job",
	"test-jobSet",
	"test-queue",
	2,
	jobSchedulingInfoWithRetryEnabled,
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

var baseJobWithRetryDisabled, _ = jobDb.NewJob(
	"test-job",
	"test-jobSet",
	"test-queue",
	2,
	jobSchedulingInfoWithRetryDisabled,
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
	assert.Equal(t, bidstore.PriceBand_PRICE_BAND_A, baseJob.GetPriceBand())
}

func TestJob_TestPriority(t *testing.T) {
	newJob := baseJob.WithPriority(3)
	assert.Equal(t, uint32(2), baseJob.Priority())
	assert.Equal(t, uint32(3), newJob.Priority())
}

func TestJob_Reservation(t *testing.T) {
	newJob := baseJob.WithPriority(3)
	assert.Equal(t, []string{"reservation-1"}, newJob.GetReservations())

	assert.True(t, newJob.MatchesReservation("reservation-1"))
	assert.False(t, newJob.MatchesReservation("reservation-2"))
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

func TestJob_IsInGang(t *testing.T) {
	// Non-gang job
	job := baseJob.WithGangInfo(BasicJobGangInfo())
	assert.False(t, job.IsInGang())

	// Gang job
	job = job.WithGangInfo(CreateGangInfo("id", 2, "uniformity"))
	assert.True(t, job.IsInGang())
}

func TestJob_BidPrices_PreemptibleJob(t *testing.T) {
	pool1Bid := pricing.Bid{QueuedBid: 1, RunningBid: 2}
	pool2Bid := pricing.Bid{QueuedBid: 3, RunningBid: 4}

	// Job queued
	newJob := preemptibleJob.WithBidPrices(map[string]pricing.Bid{"pool1": pool1Bid, "pool2": pool2Bid})
	assert.Equal(t, float64(1), newJob.GetBidPrice("pool1"))
	assert.Equal(t, float64(3), newJob.GetBidPrice("pool2"))
	assert.Equal(t, float64(0), newJob.GetBidPrice("pool3")) // default to 0

	// Job running
	newJob = newJob.WithQueued(false)
	assert.Equal(t, float64(2), newJob.GetBidPrice("pool1"))
	assert.Equal(t, float64(4), newJob.GetBidPrice("pool2"))
	assert.Equal(t, float64(0), newJob.GetBidPrice("pool3")) // default to 0

	// Assert mutating external bid obj doesn't effect what the job returns
	pool1Bid.RunningBid = 5
	pool2Bid.RunningBid = 5
	assert.Equal(t, float64(2), newJob.GetBidPrice("pool1"))
	assert.Equal(t, float64(4), newJob.GetBidPrice("pool2"))
}

func TestJob_BidPrices_NonPreemptibleJob(t *testing.T) {
	pool1Bid := pricing.Bid{QueuedBid: 1, RunningBid: 2}
	pool2Bid := pricing.Bid{QueuedBid: 3, RunningBid: 4}

	// Job queued
	newJob := baseJob.WithBidPrices(map[string]pricing.Bid{"pool1": pool1Bid, "pool2": pool2Bid})
	assert.Equal(t, float64(1), newJob.GetBidPrice("pool1"))
	assert.Equal(t, float64(3), newJob.GetBidPrice("pool2"))
	assert.Equal(t, float64(0), newJob.GetBidPrice("pool3")) // default to 0

	// Job running
	newJob = newJob.WithQueued(false)
	assert.Equal(t, float64(pricing.NonPreemptibleRunningPrice), newJob.GetBidPrice("pool1"))
	assert.Equal(t, float64(pricing.NonPreemptibleRunningPrice), newJob.GetBidPrice("pool2"))
	assert.Equal(t, float64(pricing.NonPreemptibleRunningPrice), newJob.GetBidPrice("pool3")) // default to NonPreemptibleRunningPrice
}

func TestJob_GetAllBidPrices(t *testing.T) {
	pool1Bid := pricing.Bid{QueuedBid: 1, RunningBid: 2}
	pool2Bid := pricing.Bid{QueuedBid: 3, RunningBid: 4}
	inputPrices := map[string]pricing.Bid{"pool1": pool1Bid, "pool2": pool2Bid}
	expectedBidPrices := map[string]pricing.Bid{"pool1": pool1Bid, "pool2": pool2Bid}

	job := baseJob.WithBidPrices(inputPrices)
	assert.Equal(t, expectedBidPrices, job.GetAllBidPrices())

	// Assert mutating external bid obj doesn't effect what the job returns
	pool1Bid.RunningBid = 5
	pool2Bid.RunningBid = 5
	inputPrices["pool3"] = pricing.Bid{QueuedBid: 3, RunningBid: 7}
	assert.Equal(t, expectedBidPrices, job.GetAllBidPrices())
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

	assert.Equal(t, milliQuantity(1000), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, quantity(1), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
	assert.Equal(t, milliQuantity(1000), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, quantity(0), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("storage-connections"))

	assert.Equal(t, milliQuantity(2000), newJob.AllResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, quantity(2), newJob.AllResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
	assert.Equal(t, milliQuantity(2000), newJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, quantity(0), newJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
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
	assert.Equal(t, milliQuantity(1000), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, quantity(1), baseJob.AllResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
}

func TestJob_TestKubernetesResourceRequirements(t *testing.T) {
	assert.Equal(t, milliQuantity(1000), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, quantity(0), baseJob.KubernetesResourceRequirements().GetByNameZeroIfMissing("storage-connections"))
}

func quantity(val int) k8sResource.Quantity {
	return *k8sResource.NewQuantity(int64(val), k8sResource.DecimalSI)
}

func milliQuantity(millis int) k8sResource.Quantity {
	return *k8sResource.NewMilliQuantity(int64(millis), k8sResource.DecimalSI)
}

func TestNumPreemptedRuns(t *testing.T) {
	makePreemptedRun := func(created int64) *JobRun {
		return &JobRun{
			id:        uuid.New().String(),
			created:   created,
			executor:  "test-executor",
			preempted: true,
		}
	}
	makeNonPreemptedRun := func(created int64) *JobRun {
		return &JobRun{
			id:        uuid.New().String(),
			created:   created,
			executor:  "test-executor",
			preempted: false,
		}
	}

	tests := map[string]struct {
		job      *Job
		expected uint
	}{
		"job with no runs": {
			job:      baseJob,
			expected: 0,
		},
		"job with one non-preempted run": {
			job:      baseJob.WithUpdatedRun(makeNonPreemptedRun(4)),
			expected: 0,
		},
		"job with one preempted run": {
			job:      baseJob.WithUpdatedRun(makePreemptedRun(3)),
			expected: 1,
		},
		"job with mixed runs": {
			job:      baseJob.WithUpdatedRun(makePreemptedRun(3)).WithUpdatedRun(makeNonPreemptedRun(4)),
			expected: 1,
		},
		"job with multiple preempted runs": {
			job:      baseJob.WithUpdatedRun(makePreemptedRun(3)).WithUpdatedRun(makePreemptedRun(5)),
			expected: 2,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.job.NumPreemptedRuns())
		})
	}
}

func TestIsEligibleForPreemptionRetry(t *testing.T) {
	makePreemptedRun := func(created int64) *JobRun {
		return &JobRun{
			id:        uuid.New().String(),
			created:   created,
			executor:  "test-executor",
			preempted: true,
		}
	}

	defaultRetryCount := uint(5)
	zeroRetryCount := uint(0)
	retryEnabled := preemption.RetryConfig{Enabled: true, DefaultRetryCount: &defaultRetryCount}
	retryDisabled := preemption.RetryConfig{Enabled: false}
	retryEnabledZeroCount := preemption.RetryConfig{Enabled: true, DefaultRetryCount: &zeroRetryCount}

	tests := map[string]struct {
		job      *Job
		config   preemption.RetryConfig
		expected bool
	}{
		// Job annotation overrides platform config
		"job annotation enabled overrides platform disabled": {
			job:      baseJobWithRetryEnabled,
			config:   retryDisabled,
			expected: true,
		},
		"job annotation disabled overrides platform enabled": {
			job:      baseJobWithRetryDisabled,
			config:   retryEnabled,
			expected: false,
		},
		// Platform config applies when no job annotation
		"platform enabled with no job annotation": {
			job:      baseJob,
			config:   retryEnabled,
			expected: true,
		},
		"platform disabled with no job annotation": {
			job:      baseJob,
			config:   retryDisabled,
			expected: false,
		},
		// Non-preempted runs don't count toward retry limit
		"eligible when runs exist but none preempted": {
			job:      baseJob.WithUpdatedRun(baseRun).WithUpdatedRun(baseRun),
			config:   retryEnabled,
			expected: true,
		},
		// Preempted runs count toward limit
		"eligible with one preempted run under limit": {
			job:      baseJob.WithUpdatedRun(makePreemptedRun(3)),
			config:   retryEnabled,
			expected: true,
		},
		"eligible with preempted runs under limit": {
			job:      baseJob.WithUpdatedRun(makePreemptedRun(3)).WithUpdatedRun(makePreemptedRun(5)),
			config:   retryEnabled,
			expected: true, // 2 preemptions <= 5 max
		},
		// Retry exhaustion: job annotation max=1, 2 preemptions -> not eligible
		"not eligible when job annotation max exceeded": {
			job:      baseJobWithRetryEnabled.WithUpdatedRun(makePreemptedRun(3)).WithUpdatedRun(makePreemptedRun(5)),
			config:   retryDisabled, // job has max=1, 2 preemptions
			expected: false,
		},
		"not eligible when job annotation max exceeded even with platform enabled": {
			job:      baseJobWithRetryEnabled.WithUpdatedRun(makePreemptedRun(3)).WithUpdatedRun(makePreemptedRun(5)),
			config:   retryEnabled, // job has max=1, overrides platform max=5
			expected: false,
		},
		// Edge case: platform default is 0 (effectively disabled via count)
		"not eligible when platform default count is zero": {
			job:      baseJob.WithUpdatedRun(makePreemptedRun(3)),
			config:   retryEnabledZeroCount, // enabled but max=0 means no retries allowed
			expected: false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.job.IsEligibleForPreemptionRetry(tc.config))
		})
	}
}

func TestMaxPreemptionRetryCount(t *testing.T) {
	defaultRetryCount := uint(5)

	tests := map[string]struct {
		job      *Job
		config   preemption.RetryConfig
		expected uint
	}{
		"uses platform default when no job annotation": {
			job: baseJob,
			config: preemption.RetryConfig{
				Enabled:           true,
				DefaultRetryCount: &defaultRetryCount,
			},
			expected: 5,
		},
		"job annotation overrides platform default": {
			job: baseJobWithRetryEnabled, // has annotation with max=1
			config: preemption.RetryConfig{
				Enabled:           true,
				DefaultRetryCount: &defaultRetryCount,
			},
			expected: 1,
		},
		"returns zero when no defaults set": {
			job: baseJob,
			config: preemption.RetryConfig{
				Enabled: true,
			},
			expected: 0,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := tc.job.MaxPreemptionRetryCount(tc.config)
			assert.Equal(t, tc.expected, result)
		})
	}
}
