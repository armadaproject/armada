package service

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/job"
	mocks3 "github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/utilisation"
	mocks2 "github.com/armadaproject/armada/internal/executor/utilisation/mocks"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func TestRequestJobsRuns_HandlesLeaseRequestError(t *testing.T) {
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{})
	leaseRequester.LeaseJobRunError = fmt.Errorf("lease error")

	jobRequester.RequestJobsRuns()
	assert.Len(t, eventReporter.ReceivedEvents, 0)
	allJobRuns := stateStore.GetAll()
	assert.Len(t, allJobRuns, 0)
}

func TestRequestJobsRuns_HandlesGetClusterCapacityError(t *testing.T) {
	jobRequester, eventReporter, leaseRequester, stateStore, utilisationService := setupJobRequesterTest([]*job.RunState{})
	utilisationService.GetClusterAvailableCapacityError = fmt.Errorf("capacity report error")

	jobRequester.RequestJobsRuns()
	assert.Len(t, leaseRequester.ReceivedLeaseRequests, 0)
	assert.Len(t, eventReporter.ReceivedEvents, 0)
	allJobRuns := stateStore.GetAll()
	assert.Len(t, allJobRuns, 0)
}

func TestRequestJobsRuns_ConstructsCorrectLeaseRequest(t *testing.T) {
	runId1 := uuid.New()
	runId2 := uuid.New()
	initialRuns := []*job.RunState{createRun(runId1.String(), job.Active), createRun(runId2.String(), job.Leased)}
	jobRequester, _, leaseRequester, _, utilisationService := setupJobRequesterTest(initialRuns)

	capacityReport := &utilisation.ClusterAvailableCapacityReport{
		AvailableCapacity: &armadaresource.ComputeResources{
			"cpu":    resource.MustParse("1000"),
			"memory": resource.MustParse("1000Gi"),
		},
		Nodes: []api.NodeInfo{
			{
				Name:          "node-1",
				RunIdsByState: map[string]api.JobState{runId1.String(): api.JobState_RUNNING},
			},
		},
	}
	utilisationService.ClusterAvailableCapacityReport = capacityReport

	expectedRequest := &LeaseRequest{
		AvailableResource: *capacityReport.AvailableCapacity,
		Nodes:             []*api.NodeInfo{&capacityReport.Nodes[0]},
		// Should add any ids in the state but not in the capacity report into unassigned job run ids
		UnassignedJobRunIds: []armadaevents.Uuid{*armadaevents.ProtoUuidFromUuid(runId2)},
	}

	jobRequester.RequestJobsRuns()

	assert.Len(t, leaseRequester.ReceivedLeaseRequests, 1)
	assert.Equal(t, leaseRequester.ReceivedLeaseRequests[0], expectedRequest)
}

func TestRequestJobsRuns_HandlesLeasedJobs(t *testing.T) {
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{})

	jobId := util.NewULID()
	protoJobId, err := armadaevents.ProtoUuidFromUlidString(jobId)
	require.NoError(t, err)
	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		LeasedRuns: []*executorapi.JobRunLease{
			{
				JobRunId: armadaevents.ProtoUuidFromUuid(uuid.New()),
				Queue:    "queue",
				Jobset:   "job-set",
				Job: &armadaevents.SubmitJob{
					JobId: protoJobId,
					ObjectMeta: &armadaevents.ObjectMeta{
						Labels:      map[string]string{},
						Annotations: map[string]string{},
						Namespace:   "test-namespace",
					},
					MainObject: &armadaevents.KubernetesMainObject{
						Object: &armadaevents.KubernetesMainObject_PodSpec{
							PodSpec: &armadaevents.PodSpecWithAvoidList{
								PodSpec: &v1.PodSpec{},
							},
						},
					},
				},
			},
		},
	}

	jobRequester.RequestJobsRuns()

	assert.Len(t, eventReporter.ReceivedEvents, 0)
	allJobRuns := stateStore.GetAll()
	assert.Len(t, allJobRuns, 1)
	assert.Equal(t, allJobRuns[0].Phase, job.Leased)
	assert.Equal(t, allJobRuns[0].Meta.JobId, jobId)
}

func TestRequestJobsRuns_HandlesRunIdsToCancel(t *testing.T) {
	runId := uuid.New()
	activeRun := createRun(runId.String(), job.Active)
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{activeRun})

	activeRunUuid, err := armadaevents.ProtoUuidFromUuidString(activeRun.Meta.RunId)
	require.NoError(t, err)

	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		RunIdsToCancel: []*armadaevents.Uuid{
			nil, // Invalid should be skipped
			armadaevents.ProtoUuidFromUuid(uuid.New()), // Belongs to no known runs, should be skipped
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
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{activeRun})

	activeRunUuid, err := armadaevents.ProtoUuidFromUuidString(activeRun.Meta.RunId)
	require.NoError(t, err)

	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		RunIdsToPreempt: []*armadaevents.Uuid{
			nil, // Invalid should be skipped
			armadaevents.ProtoUuidFromUuid(uuid.New()), // Belongs to no known runs, should be skipped
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
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{})

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
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{})

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

func setupJobRequesterTest(initialJobRuns []*job.RunState) (*JobRequester, *mocks3.FakeEventReporter, *StubLeaseRequester, *job.JobRunStateStore, *mocks2.StubUtilisationService) {
	clusterId := fakecontext.NewFakeClusterIdentity("cluster-1", "pool-1")
	eventReporter := mocks3.NewFakeEventReporter()
	stateStore := job.NewJobRunStateStoreWithInitialState(initialJobRuns)
	leaseRequester := &StubLeaseRequester{}
	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{}
	podDefaults := &configuration.PodDefaults{}
	utilisationService := &mocks2.StubUtilisationService{}
	utilisationService.ClusterAvailableCapacityReport = &utilisation.ClusterAvailableCapacityReport{
		AvailableCapacity: &armadaresource.ComputeResources{},
	}
	jobRequester := NewJobRequester(clusterId, eventReporter, leaseRequester, stateStore, utilisationService, podDefaults)
	return jobRequester, eventReporter, leaseRequester, stateStore, utilisationService
}

type StubLeaseRequester struct {
	ReceivedLeaseRequests    []*LeaseRequest
	LeaseJobRunError         error
	LeaseJobRunLeaseResponse *LeaseResponse
}

func (s *StubLeaseRequester) LeaseJobRuns(_ *armadacontext.Context, request *LeaseRequest) (*LeaseResponse, error) {
	s.ReceivedLeaseRequests = append(s.ReceivedLeaseRequests, request)
	return s.LeaseJobRunLeaseResponse, s.LeaseJobRunError
}
