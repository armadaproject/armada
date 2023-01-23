package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
)

func TestWatchContext_ProcessEvent(t *testing.T) {
	watchContext := NewWatchContext()

	expected := &JobInfo{Status: Pending, PodStatus: []PodStatus{Pending}, MaxUsedResources: common.ComputeResources{}, PodLastUpdated: []time.Time{{}}}

	watchContext.ProcessEvent(&api.JobPendingEvent{JobId: "1"})
	result := watchContext.GetJobInfo("1")

	assert.Equal(t, expected, result)
}

func TestWatchContext_ProcessEvent_UpdatesExisting(t *testing.T) {
	watchContext := NewWatchContext()

	expected := &JobInfo{Status: Running, PodStatus: []PodStatus{Running}, MaxUsedResources: common.ComputeResources{}, PodLastUpdated: []time.Time{{}}}

	watchContext.ProcessEvent(&api.JobPendingEvent{JobId: "1"})
	watchContext.ProcessEvent(&api.JobRunningEvent{JobId: "1"})
	result := watchContext.GetJobInfo("1")

	assert.Equal(t, expected, result)
}

func TestWatchContext_ProcessEvent_SubmittedEventAddsJobToJobInfo(t *testing.T) {
	watchContext := NewWatchContext()

	job := api.Job{
		Id:       "1",
		JobSetId: "job-set-1",
		Queue:    "queue1",
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "Container1",
				},
			},
		},
	}

	expected := &JobInfo{
		Status:           Submitted,
		Job:              &job,
		MaxUsedResources: common.ComputeResources{},
	}

	watchContext.ProcessEvent(&api.JobSubmittedEvent{JobId: "1", Job: job})
	result := watchContext.GetJobInfo("1")

	assert.Equal(t, expected, result)
}

func TestWatchContext_GetCurrentState(t *testing.T) {
	watchContext := NewWatchContext()

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1"})
	watchContext.ProcessEvent(&api.JobPendingEvent{JobId: "2"})
	watchContext.ProcessEvent(&api.JobRunningEvent{JobId: "3"})

	expected := map[string]*JobInfo{
		"1": {Status: Queued, MaxUsedResources: common.ComputeResources{}},
		"2": {Status: Pending, PodStatus: []PodStatus{Pending}, MaxUsedResources: common.ComputeResources{}, PodLastUpdated: []time.Time{{}}},
		"3": {Status: Running, PodStatus: []PodStatus{Running}, MaxUsedResources: common.ComputeResources{}, PodLastUpdated: []time.Time{{}}},
	}

	result := watchContext.GetCurrentState()

	assert.Equal(t, expected, result)
}

func TestWatchContext_GetCurrentStateSummary(t *testing.T) {
	watchContext := NewWatchContext()

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1"})
	watchContext.ProcessEvent(&api.JobPendingEvent{JobId: "2"})
	watchContext.ProcessEvent(&api.JobRunningEvent{JobId: "3"})

	expected := "Queued:   1, Leased:   0, Pending:   1, Running:   1, Succeeded:   0, Failed:   0, Cancelled:   0"
	result := watchContext.GetCurrentStateSummary()

	assert.Equal(t, expected, result)
}

func TestWatchContext_GetCurrentStateSummary_IsCorrectlyAlteredOnUpdateToExistingJob(t *testing.T) {
	watchContext := NewWatchContext()

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1"})
	watchContext.ProcessEvent(&api.JobPendingEvent{JobId: "1"})
	expected := "Queued:   0, Leased:   0, Pending:   1, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0"
	result := watchContext.GetCurrentStateSummary()
	assert.Equal(t, result, expected)
}

func TestWatchContext_GetNumberOfJobsInStates(t *testing.T) {
	watchContext := NewWatchContext()

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1"})
	result := watchContext.GetNumberOfJobsInStates([]JobStatus{Queued})

	assert.Equal(t, 1, result)
}

func TestWatchContext_GetNumberOfJobs(t *testing.T) {
	watchContext := NewWatchContext()

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1"})
	result := watchContext.GetNumberOfJobs()

	assert.Equal(t, 1, result)

	watchContext.ProcessEvent(&api.JobSucceededEvent{JobId: "73"})
	result = watchContext.GetNumberOfJobs()

	assert.Equal(t, 2, result)
}

// Succeeded/failed/cancelled jobs are considered finished
func TestWatchContext_GetNumberOfFinishedJobs(t *testing.T) {
	watchContext := NewWatchContext()

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1"})
	assert.Equal(t, 0, watchContext.GetNumberOfFinishedJobs())

	watchContext.ProcessEvent(&api.JobCancelledEvent{JobId: "1"})
	assert.Equal(t, 1, watchContext.GetNumberOfFinishedJobs())

	watchContext.ProcessEvent(&api.JobPendingEvent{JobId: "1"})
	assert.Equal(t, 0, watchContext.GetNumberOfFinishedJobs())

	watchContext.ProcessEvent(&api.JobSucceededEvent{JobId: "1"})
	assert.Equal(t, 1, watchContext.GetNumberOfFinishedJobs())

	watchContext.ProcessEvent(&api.JobLeasedEvent{JobId: "1"})
	assert.Equal(t, 0, watchContext.GetNumberOfFinishedJobs())

	watchContext.ProcessEvent(&api.JobRunningEvent{JobId: "2"})
	assert.Equal(t, 0, watchContext.GetNumberOfFinishedJobs())

	watchContext.ProcessEvent(&api.JobSucceededEvent{JobId: "2"})
	watchContext.ProcessEvent(&api.JobFailedEvent{JobId: "1"})
	assert.Equal(t, 2, watchContext.GetNumberOfFinishedJobs())
}

func TestWatchContext_GetNumberOfJobsInStates_IsCorrectlyUpdatedOnUpdateToExistingJobState(t *testing.T) {
	watchContext := NewWatchContext()

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1"})
	assert.Equal(t, 1, watchContext.GetNumberOfJobsInStates([]JobStatus{Queued}))

	watchContext.ProcessEvent(&api.JobPendingEvent{JobId: "1"})
	assert.Equal(t, 0, watchContext.GetNumberOfJobsInStates([]JobStatus{Queued}))
	assert.Equal(t, 1, watchContext.GetNumberOfJobsInStates([]JobStatus{Pending}))
}

func TestWatchContext_EventsOutOfOrder(t *testing.T) {
	watchContext := NewWatchContext()

	now := time.Now()
	job := api.Job{
		Id:       "1",
		JobSetId: "job-set-1",
		Queue:    "queue1",
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "Container1",
				},
			},
		},
	}

	watchContext.ProcessEvent(&api.JobUtilisationEvent{JobId: "1", Created: now.Add(10 * time.Second)})
	assert.Equal(t, &JobInfo{MaxUsedResources: common.ComputeResources{}}, watchContext.GetJobInfo("1"))
	assert.Equal(t, map[JobStatus]int{}, watchContext.stateSummary)

	watchContext.ProcessEvent(&api.JobSucceededEvent{JobId: "1", Created: now})
	assert.Equal(
		t,
		&JobInfo{
			Status:           Succeeded,
			PodStatus:        []PodStatus{Succeeded},
			LastUpdate:       now,
			PodLastUpdated:   []time.Time{now},
			MaxUsedResources: common.ComputeResources{},
		},
		watchContext.GetJobInfo("1"),
	)
	assert.Equal(t, map[JobStatus]int{Succeeded: 1}, watchContext.stateSummary)

	watchContext.ProcessEvent(&api.JobQueuedEvent{JobId: "1", Created: now.Add(-1 * time.Second)})
	assert.Equal(
		t,
		&JobInfo{
			Status:           Succeeded,
			PodStatus:        []PodStatus{Succeeded},
			LastUpdate:       now,
			PodLastUpdated:   []time.Time{now},
			MaxUsedResources: common.ComputeResources{},
		},
		watchContext.GetJobInfo("1"),
	)
	assert.Equal(t, map[JobStatus]int{Succeeded: 1}, watchContext.stateSummary)

	watchContext.ProcessEvent(&api.JobSubmittedEvent{JobId: "1", Job: job, Created: now.Add(-2 * time.Second)})
	assert.Equal(
		t,
		&JobInfo{
			Status:           Succeeded,
			PodStatus:        []PodStatus{Succeeded},
			LastUpdate:       now,
			PodLastUpdated:   []time.Time{now},
			Job:              &job,
			MaxUsedResources: common.ComputeResources{},
		},
		watchContext.GetJobInfo("1"),
	)
	assert.Equal(t, map[JobStatus]int{Succeeded: 1}, watchContext.stateSummary)
}

func TestWatchContext_UtilisationEvent(t *testing.T) {
	watchContext := NewWatchContext()
	watchContext.ProcessEvent(&api.JobUtilisationEvent{
		JobId:        "job1",
		JobSetId:     "",
		Queue:        "",
		Created:      time.Now(),
		ClusterId:    "",
		KubernetesId: "",
		MaxResourcesForPeriod: common.ComputeResources{
			"cpu": resource.MustParse("1"),
		},
	})
	assert.Equal(t, resource.MustParse("1"), watchContext.GetJobInfo("job1").MaxUsedResources["cpu"])
}
