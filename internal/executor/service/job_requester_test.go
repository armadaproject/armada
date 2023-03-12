package service

import (
	"testing"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter/fake"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestJobsRuns_ConstructsCorrectLeaseRequest(t *testing.T) {

}

func TestRequestJobsRuns_HandlesLeasedJobs(t *testing.T) {

}

func TestRequestJobsRuns_HandlesRunIdsToCancel(t *testing.T) {
	jobId := uuid.New()
	activeRun := createRun(jobId.String(), job.Active)
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest(t)

	stateStore.SetState(map[string]*job.RunState{
		activeRun.Meta.RunId: activeRun,
	})

	activeRunUuid, err := armadaevents.ProtoUuidFromUuidString(activeRun.Meta.RunId)
	require.NoError(t, err)

	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		RunIdsToCancel: []*armadaevents.Uuid{
			&armadaevents.Uuid{},                       // Invalid should be skipped
			armadaevents.ProtoUuidFromUuid(uuid.New()), // Belongs to no know runs, should be skipped
			activeRunUuid,
		},
	}

	expectedRunState := activeRun.DeepCopy()
	expectedRunState.CancelRequested = true

	jobRequester.RequestJobsRuns()

	assert.Len(t, eventReporter.ReceivedEvents, 0)
	allJobRuns := stateStore.GetAll()
	assert.Len(t, allJobRuns, 1)
	assert.Equal(t, allJobRuns[0], expectedRunState)
}

func TestRequestJobsRuns_HandlesRunIsToPreempt(t *testing.T) {
	runId := uuid.New()
	activeRun := createRun(runId.String(), job.Active)
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest(t)

	stateStore.SetState(map[string]*job.RunState{
		activeRun.Meta.RunId: activeRun,
	})

	activeRunUuid, err := armadaevents.ProtoUuidFromUuidString(activeRun.Meta.RunId)
	require.NoError(t, err)

	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		RunIdsToPreempt: []*armadaevents.Uuid{
			&armadaevents.Uuid{},                       // Invalid should be skipped
			armadaevents.ProtoUuidFromUuid(uuid.New()), // Belongs to no know runs, should be skipped
			activeRunUuid,
		},
	}

	expectedRunState := activeRun.DeepCopy()
	expectedRunState.PreemptionRequested = true

	jobRequester.RequestJobsRuns()

	assert.Len(t, eventReporter.ReceivedEvents, 0)
	allJobRuns := stateStore.GetAll()
	assert.Len(t, allJobRuns, 1)
	assert.Equal(t, allJobRuns[0], expectedRunState)
}

func TestRequestJobsRuns_HandlesPartiallyInvalidLeasedJobs(t *testing.T) {
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest(t)

	jobId := util.NewULID()
	protoJobId, err := armadaevents.ProtoUuidFromUlidString(jobId)
	require.NoError(t, err)
	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		LeasedRuns: []*executorapi.JobRunLease{
			// Valid job id info, but invalid submit job (no pod spec)
			{
				JobRunId: armadaevents.ProtoUuidFromUuid(uuid.New()),
				Queue:    "queue",
				Jobset:   "job-set",
				Job: &armadaevents.SubmitJob{
					JobId: protoJobId,
				},
			},
		},
	}

	jobRequester.RequestJobsRuns()

	assert.Len(t, eventReporter.ReceivedEvents, 1)
	event, ok := eventReporter.ReceivedEvents[0].Event.(*api.JobFailedEvent)
	assert.True(t, ok)
	assert.Equal(t, event.JobId, jobId)

	allJobRuns := stateStore.GetAll()
	assert.Len(t, allJobRuns, 1)
	assert.Equal(t, allJobRuns[0].Phase, job.Invalid)
}

func TestRequestJobsRuns_SkipsFullyInvalidLeasedJobs(t *testing.T) {
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest(t)

	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		LeasedRuns: []*executorapi.JobRunLease{
			// Invalid Id info
			{
				JobRunId: &armadaevents.Uuid{},
			},
		},
	}

	jobRequester.RequestJobsRuns()

	// Does not report events or record state
	assert.Len(t, eventReporter.ReceivedEvents, 0)
	assert.Len(t, stateStore.GetAll(), 0)
}

func setupJobRequesterTest(t *testing.T) (*JobRequester, *fake.FakeEventReporter, *StubLeaseRequester, *job.TestJobRunStateStore, *utilisation.StubUtilisationService) {
	clusterId := fakecontext.NewFakeClusterIdentity("cluster-1", "pool-1")
	eventReporter := fake.NewFakeEventReporter()
	stateStore := job.NewTestJobRunStateStore([]*job.RunState{})
	leaseRequester := &StubLeaseRequester{}
	podDefaults := &configuration.PodDefaults{}
	utilisationService := &utilisation.StubUtilisationService{}
	utilisationService.ClusterAvailableCapacityReport = &utilisation.ClusterAvailableCapacityReport{
		AvailableCapacity: &armadaresource.ComputeResources{},
	}
	jobRequester := NewJobRequester(clusterId, eventReporter, leaseRequester, stateStore, utilisationService, podDefaults)
	return jobRequester, eventReporter, leaseRequester, stateStore, utilisationService
}
