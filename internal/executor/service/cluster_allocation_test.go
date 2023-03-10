package service

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/healthmonitor"
	"github.com/armadaproject/armada/internal/executor/job"
	fake3 "github.com/armadaproject/armada/internal/executor/job/fake"
	"github.com/armadaproject/armada/internal/executor/reporter/fake"
	"github.com/armadaproject/armada/pkg/api"
)

var defaultLeaseRun = createRun("default-run", job.Leased)

func TestAllocateSpareClusterCapacity(t *testing.T) {
	clusterAllocationService, _, eventReporter, submitter, runStore := setupClusterAllocationServiceTest()
	runStore.JobRunState = map[string]*job.RunState{defaultLeaseRun.Meta.RunId: defaultLeaseRun}

	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, submitter.ReceivedSubmitJobs, 1)
	assert.Equal(t, submitter.ReceivedSubmitJobs[0], defaultLeaseRun.Job)
	assert.Len(t, eventReporter.ReceivedEvents, 0)
}

func TestAllocateSpareClusterCapacity_SkipsLeaseRunsWhereJobIsNil(t *testing.T) {
	clusterAllocationService, _, eventReporter, submitter, runStore := setupClusterAllocationServiceTest()
	invalidLeaseRun := createRun("invalid", job.Leased)
	invalidLeaseRun.Job = nil
	runStore.JobRunState = map[string]*job.RunState{invalidLeaseRun.Meta.RunId: invalidLeaseRun}

	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, submitter.ReceivedSubmitJobs, 0)
	assert.Len(t, eventReporter.ReceivedEvents, 0)
}

func TestAllocateSpareClusterCapacity_OnlySubmitsJobForLeasedRuns(t *testing.T) {
	clusterAllocationService, _, eventReporter, submitter, runStore := setupClusterAllocationServiceTest()
	runStore.JobRunState = map[string]*job.RunState{
		defaultLeaseRun.Meta.RunId: defaultLeaseRun,
		"invalid":                  createRun("failedSubmission", job.Invalid),
		"failedSubmission":         createRun("failedSubmission", job.FailedSubmission),
		"successfulSubmission":     createRun("successfulSubmission", job.SuccessfulSubmission),
		"missing":                  createRun("missing", job.Missing),
		"active":                   createRun("active", job.Active),
	}

	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, submitter.ReceivedSubmitJobs, 1)
	assert.Equal(t, submitter.ReceivedSubmitJobs[0], defaultLeaseRun.Job)
	assert.Len(t, eventReporter.ReceivedEvents, 0)
}

func TestAllocateSpareClusterCapacity_DoesNotSubmitJobs_WhenEtcdIsNotWithinSoftLimit(t *testing.T) {
	clusterAllocationService, etcdHealthMonitor, eventReporter, submitter, _ := setupClusterAllocationServiceTest()
	etcdHealthMonitor.IsWithinSoftLimit = false

	clusterAllocationService.AllocateSpareClusterCapacity()

	assert.Len(t, eventReporter.ReceivedEvents, 0)
	assert.Len(t, submitter.ReceivedSubmitJobs, 0)
}

func TestAllocateSpareClusterCapacity_HandlesFailedPodCreations(t *testing.T) {
	tests := map[string]struct {
		initialRunState          *job.RunState
		recoverableSubmitFailure bool
		failOnReportingEvent     bool
		expectLeaseReturnedEvent bool
		expectFailEvent          bool
	}{
		"recoverable submission failure": {
			initialRunState:          defaultLeaseRun,
			recoverableSubmitFailure: true,
			expectLeaseReturnedEvent: true,
		},
		"recoverable submission failure with event reporting failure": {
			initialRunState:          defaultLeaseRun,
			recoverableSubmitFailure: true,
			failOnReportingEvent:     true,
		},
		"unrecoverable submission failure": {
			initialRunState:          defaultLeaseRun,
			recoverableSubmitFailure: false,
			expectFailEvent:          true,
		},
		"unrecoverable submission failure with event reporting failure": {
			initialRunState:          defaultLeaseRun,
			recoverableSubmitFailure: false,
			failOnReportingEvent:     true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clusterAllocationService, _, eventReporter, submitter, runStore := setupClusterAllocationServiceTest()
			eventReporter.ErrorOnReport = tc.failOnReportingEvent
			runStore.JobRunState = map[string]*job.RunState{tc.initialRunState.Meta.RunId: tc.initialRunState}
			submitter.FailedSubmissionDetails = []*job.FailedSubmissionDetails{
				{
					JobRunMeta:  tc.initialRunState.Meta,
					Pod:         tc.initialRunState.Job.Pod,
					Error:       fmt.Errorf("failed"),
					Recoverable: tc.recoverableSubmitFailure,
				},
			}

			clusterAllocationService.AllocateSpareClusterCapacity()

			if tc.failOnReportingEvent {
				assert.Len(t, eventReporter.ReceivedEvents, 0)
				run := runStore.Get(tc.initialRunState.Meta.RunId)
				assert.Nil(t, run)
			}

			if tc.expectLeaseReturnedEvent {
				run := runStore.Get(tc.initialRunState.Meta.RunId)
				assert.Equal(t, run.Phase, job.FailedSubmission)
				assert.Len(t, eventReporter.ReceivedEvents, 1)
				_, ok := eventReporter.ReceivedEvents[0].Event.(*api.JobLeaseReturnedEvent)
				assert.True(t, ok)
			}

			if tc.expectFailEvent {
				run := runStore.Get(tc.initialRunState.Meta.RunId)
				assert.Equal(t, run.Phase, job.FailedSubmission)
				assert.Len(t, eventReporter.ReceivedEvents, 1)
				_, ok := eventReporter.ReceivedEvents[0].Event.(*api.JobFailedEvent)
				assert.True(t, ok)
			}
		})
	}
}

func setupClusterAllocationServiceTest() (
	*ClusterAllocationService,
	*healthmonitor.FakeEtcdLimitHealthMonitor,
	*fake.FakeEventReporter,
	*fake3.FakeSubmitter,
	*fake3.StubRunStateStore) {
	clusterId := fakecontext.NewFakeClusterIdentity("cluster-1", "pool-1")
	eventReporter := fake.NewFakeEventReporter()
	submitter := &fake3.FakeSubmitter{}
	etcdHealthChecker := &healthmonitor.FakeEtcdLimitHealthMonitor{IsWithinSoftLimit: true, IsWithinHardLimit: true}
	jobRunStateManager := fake3.NewStubRunStateStore([]*job.RunState{})

	clusterAllocationService := NewClusterAllocationService(
		clusterId, eventReporter, jobRunStateManager, submitter, etcdHealthChecker)

	return clusterAllocationService, etcdHealthChecker, eventReporter, submitter, jobRunStateManager
}

func createRun(runId string, phase job.RunPhase) *job.RunState {
	return &job.RunState{
		Meta: &job.RunMeta{
			RunId: runId,
		},
		Job: &job.SubmitJob{
			Meta: job.SubmitJobMeta{
				RunMeta: &job.RunMeta{
					RunId: "run-id",
				},
			},
			Pod: makePod("queue-1"),
		},
		Phase: phase,
	}
}
