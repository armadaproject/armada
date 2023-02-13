package jobdb

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

var schedulingInfo = &schedulerobjects.JobSchedulingInfo{
	ObjectRequirements: []*schedulerobjects.ObjectRequirements{
		{
			Requirements: &schedulerobjects.ObjectRequirements_PodRequirements{
				PodRequirements: &schedulerobjects.PodRequirements{
					Annotations: map[string]string{
						"foo": "bar",
					},
				},
			},
		},
	},
}

var baseJob = NewJob(
	"test-job",
	"test-jobset",
	"test-queue",
	2,
	schedulingInfo,
	false,
	false,
	3)

var baseRun = &JobRun{
	id:        uuid.New(),
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
	assert.Equal(t, baseJob.id, baseJob.GetId())
	assert.Equal(t, baseJob.queue, baseJob.Queue())
	assert.Equal(t, baseJob.queue, baseJob.GetQueue())
	assert.Equal(t, baseJob.created, baseJob.Created())
	assert.Equal(t, schedulingInfo, baseJob.GetRequirements(nil))
	assert.Equal(t, schedulingInfo, baseJob.JobSchedulingInfo())
	assert.Equal(t, baseJob.GetAnnotations(), map[string]string{
		"foo": "bar",
	})
}

func TestJob_TestPriority(t *testing.T) {
	newJob := baseJob.WithPriority(3)
	assert.Equal(t, uint32(2), baseJob.Priority())
	assert.Equal(t, uint32(3), newJob.Priority())
}

func TestJob_TestQueued(t *testing.T) {
	newJob := baseJob.WithQueued(false)
	assert.Equal(t, true, baseJob.Queued())
	assert.Equal(t, false, newJob.Queued())
}

func TestJob_TestCancelRequested(t *testing.T) {
	newJob := baseJob.WithCancelRequested(true)
	assert.Equal(t, false, baseJob.CancelRequested())
	assert.Equal(t, true, newJob.CancelRequested())
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
	assert.Equal(t, true, baseJob.WithNewRun("test-executor").HasRuns())
}

func TestJob_TestWithNewRun(t *testing.T) {
	jobWithRun := baseJob.WithNewRun("test-executor")
	assert.Equal(t, true, jobWithRun.HasRuns())
	run := jobWithRun.LatestRun()
	assert.NotNil(t, run)
	assert.Equal(t, &JobRun{
		id:       run.id,
		created:  run.created,
		executor: "test-executor",
	}, run)
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
		id:        uuid.New(),
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
		id:       uuid.New(),
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
		id:       uuid.New(),
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
			id:       uuid.New(),
			created:  baseRun.created,
			returned: true,
		}
	}

	nonReturnedRun := func() *JobRun {
		return &JobRun{
			id:       uuid.New(),
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

func TestJob_TestRunsById(t *testing.T) {
	runs := make([]*JobRun, 10)
	job := baseJob
	for i := 0; i < len(runs); i++ {
		runs[i] = &JobRun{id: uuid.New()}
		job = job.WithUpdatedRun(runs[i])
	}
	for i := 0; i < len(runs); i++ {
		assert.Equal(t, runs[i], job.runsById[runs[i].id])
	}
}
