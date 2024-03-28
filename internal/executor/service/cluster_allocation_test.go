package service

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/armadaproject/armada/internal/common/healthmonitor"
	util2 "github.com/armadaproject/armada/internal/common/util"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/job/mocks"
	mocks2 "github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func TestAllocateSpareClusterCapacity(t *testing.T) {
	leaseRun := createRun("leased", job.Leased)
	clusterAllocationService, _, eventReporter, submitter, runStore := setupClusterAllocationServiceTest([]*job.RunState{leaseRun})

	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, submitter.ReceivedSubmitJobs, 1)
	assert.Equal(t, submitter.ReceivedSubmitJobs[0], leaseRun.Job)
	assert.Len(t, eventReporter.ReceivedEvents, 0)
	runState := runStore.Get(leaseRun.Meta.RunId)
	assert.Equal(t, runState.Phase, job.SuccessfulSubmission)
}

func TestAllocateSpareClusterCapacity_SkipsLeaseRunsWhereJobIsNil(t *testing.T) {
	invalidLeaseRun := createRun("invalid", job.Leased)
	invalidLeaseRun.Job = nil
	clusterAllocationService, _, eventReporter, submitter, _ := setupClusterAllocationServiceTest([]*job.RunState{invalidLeaseRun})

	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, submitter.ReceivedSubmitJobs, 0)
	assert.Len(t, eventReporter.ReceivedEvents, 0)
}

func TestAllocateSpareClusterCapacity_OnlySubmitsJobForLeasedRuns(t *testing.T) {
	leaseRun := createRun("leased", job.Leased)
	initialRuns := []*job.RunState{
		leaseRun,
		createRun("failedSubmission", job.Invalid),
		createRun("failedSubmission", job.FailedSubmission),
		createRun("successfulSubmission", job.SuccessfulSubmission),
		createRun("missing", job.Missing),
		createRun("active", job.Active),
	}
	clusterAllocationService, _, eventReporter, submitter, _ := setupClusterAllocationServiceTest(initialRuns)
	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, submitter.ReceivedSubmitJobs, 1)
	assert.Equal(t, submitter.ReceivedSubmitJobs[0], leaseRun.Job)
	assert.Len(t, eventReporter.ReceivedEvents, 0)
}

func TestAllocateSpareClusterCapacity_DoesNotSubmitJobs_WhenEtcdIsNotWithinSoftLimit(t *testing.T) {
	leaseRun := createRun("leased", job.Leased)
	clusterAllocationService, healthMonitor, eventReporter, submitter, _ := setupClusterAllocationServiceTest([]*job.RunState{leaseRun})
	healthMonitor.SetHealthStatus(false)

	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, eventReporter.ReceivedEvents, 0)
	assert.Len(t, submitter.ReceivedSubmitJobs, 0)
}

func TestAllocateSpareClusterCapacity_HandlesFailedPodCreations(t *testing.T) {
	tests := map[string]struct {
		recoverableSubmitFailure bool
		failOnReportingEvent     bool
		expectLeaseReturnedEvent bool
		expectFailEvent          bool
	}{
		"recoverable submission failure": {
			recoverableSubmitFailure: true,
			expectLeaseReturnedEvent: true,
		},
		"recoverable submission failure with event reporting failure": {
			recoverableSubmitFailure: true,
			failOnReportingEvent:     true,
		},
		"unrecoverable submission failure": {
			recoverableSubmitFailure: false,
			expectFailEvent:          true,
		},
		"unrecoverable submission failure with event reporting failure": {
			recoverableSubmitFailure: false,
			failOnReportingEvent:     true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			leaseRun := createRun(uuid.New().String(), job.Leased)
			clusterAllocationService, _, eventReporter, submitter, runStore := setupClusterAllocationServiceTest([]*job.RunState{leaseRun})
			eventReporter.ErrorOnReport = tc.failOnReportingEvent
			submitter.FailedSubmissionDetails = []*job.FailedSubmissionDetails{
				{
					JobRunMeta:  leaseRun.Meta,
					Pod:         leaseRun.Job.Pod,
					Error:       fmt.Errorf("failed"),
					Recoverable: tc.recoverableSubmitFailure,
				},
			}

			clusterAllocationService.AllocateSpareClusterCapacity()

			if tc.failOnReportingEvent {
				assert.Len(t, eventReporter.ReceivedEvents, 0)
				run := runStore.Get(leaseRun.Meta.RunId)
				assert.Nil(t, run)
			}

			if tc.expectLeaseReturnedEvent {
				run := runStore.Get(leaseRun.Meta.RunId)
				assert.Equal(t, run.Phase, job.FailedSubmission)
				assert.Len(t, eventReporter.ReceivedEvents, 1)
				assert.Len(t, eventReporter.ReceivedEvents[0].Event.Events, 1)
				event, ok := eventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
				assert.True(t, ok)
				assert.Len(t, event.JobRunErrors.Errors, 1)
				assert.True(t, event.JobRunErrors.Errors[0].GetPodLeaseReturned() != nil)
			}

			if tc.expectFailEvent {
				run := runStore.Get(leaseRun.Meta.RunId)
				assert.Equal(t, run.Phase, job.FailedSubmission)
				assert.Len(t, eventReporter.ReceivedEvents, 1)
				assert.Len(t, eventReporter.ReceivedEvents[0].Event.Events, 1)
				event, ok := eventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
				assert.True(t, ok)
				assert.Len(t, event.JobRunErrors.Errors, 1)
				assert.True(t, event.JobRunErrors.Errors[0].GetPodError() != nil)
			}
		})
	}
}

func setupClusterAllocationServiceTest(initialJobRuns []*job.RunState) (
	*ClusterAllocationService,
	*healthmonitor.ManualHealthMonitor,
	*mocks2.FakeEventReporter,
	*mocks.FakeSubmitter,
	*job.JobRunStateStore,
) {
	clusterId := fakecontext.NewFakeClusterIdentity("cluster-1", "pool-1")
	eventReporter := mocks2.NewFakeEventReporter()
	submitter := &mocks.FakeSubmitter{}
	jobRunStateManager := job.NewJobRunStateStoreWithInitialState(initialJobRuns)
	healthMonitor := &healthmonitor.ManualHealthMonitor{}
	healthMonitor.SetHealthStatus(true)

	return NewClusterAllocationService(
		clusterId,
		eventReporter,
		jobRunStateManager,
		submitter,
		healthMonitor,
	), healthMonitor, eventReporter, submitter, jobRunStateManager
}

func createRun(runId string, phase job.RunPhase) *job.RunState {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        util2.NewULID(),
			UID:         types.UID(util2.NewULID()),
			Namespace:   "default",
			Annotations: map[string]string{},
			Labels:      map[string]string{domain.Queue: "test", domain.JobId: util2.NewULID(), domain.JobRunId: runId},
		},
	}
	return &job.RunState{
		Meta: &job.RunMeta{
			RunId: runId,
		},
		Job: &job.SubmitJob{
			Meta: job.SubmitJobMeta{
				RunMeta: &job.RunMeta{
					RunId: runId,
				},
			},
			Pod: pod,
		},
		Phase: phase,
	}
}
