package service

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

const defaultMaxLeasedJobs int = 5

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
	activeRunId := uuid.NewString()
	leasedRunId := uuid.NewString()
	activeRun := createRun(activeRunId, job.Active)
	leasedRun := createRun(leasedRunId, job.Leased)

	capacityReport := &utilisation.ClusterAvailableCapacityReport{
		AvailableCapacity: &armadaresource.ComputeResources{
			"cpu":    resource.MustParse("100"),
			"memory": resource.MustParse("1000Gi"),
		},
		Nodes: []executorapi.NodeInfo{
			{
				Name:          "node-1",
				RunIdsByState: map[string]api.JobState{activeRunId: api.JobState_RUNNING},
			},
		},
	}

	tests := map[string]struct {
		hasExistingLeasedRun bool
		expectedRequest      *LeaseRequest
	}{
		"has no leased jobs - should request more": {
			hasExistingLeasedRun: false,
			expectedRequest: &LeaseRequest{
				AvailableResource: *capacityReport.AvailableCapacity,
				Nodes:             []*executorapi.NodeInfo{&capacityReport.Nodes[0]},
				// Should add any ids in the state but not in the capacity report into unassigned job run ids
				UnassignedJobRunIds: []string{},
				MaxJobsToLease:      uint32(defaultMaxLeasedJobs),
			},
		},
		"has existing leased jobs - should not request more": {
			hasExistingLeasedRun: true,
			expectedRequest: &LeaseRequest{
				AvailableResource: *capacityReport.AvailableCapacity,
				Nodes:             []*executorapi.NodeInfo{&capacityReport.Nodes[0]},
				// Should add any ids in the state but not in the capacity report into unassigned job run ids
				UnassignedJobRunIds: []string{leasedRunId},
				MaxJobsToLease:      0,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			initialRuns := []*job.RunState{activeRun}
			if tc.hasExistingLeasedRun {
				initialRuns = append(initialRuns, leasedRun)
			}

			jobRequester, _, leaseRequester, _, utilisationService := setupJobRequesterTest(initialRuns)
			utilisationService.ClusterAvailableCapacityReport = capacityReport
			jobRequester.RequestJobsRuns()

			assert.Len(t, leaseRequester.ReceivedLeaseRequests, 1)
			assert.Equal(t, leaseRequester.ReceivedLeaseRequests[0], tc.expectedRequest)
		})
	}
}

func TestRequestJobsRuns_HandlesLeasedJobs(t *testing.T) {
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{})

	jobId := util.NewULID()
	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		LeasedRuns: []*executorapi.JobRunLease{
			{
				JobRunId: uuid.NewString(),
				Queue:    "queue",
				Jobset:   "job-set",
				Job: &armadaevents.SubmitJob{
					JobId: jobId,
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
	runId := uuid.NewString()
	activeRun := createRun(runId, job.Active)
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{activeRun})

	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		RunIdsToCancel: []string{
			"",               // Invalid should be skipped
			uuid.NewString(), // Belongs to no known runs, should be skipped
			runId,
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
	runId := uuid.NewString()
	activeRun := createRun(runId, job.Active)
	jobRequester, eventReporter, leaseRequester, stateStore, _ := setupJobRequesterTest([]*job.RunState{activeRun})

	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		RunIdsToPreempt: []string{
			"",               // Invalid should be skipped
			uuid.NewString(), // Belongs to no known runs, should be skipped
			runId,
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
	leaseRequester.LeaseJobRunLeaseResponse = &LeaseResponse{
		LeasedRuns: []*executorapi.JobRunLease{
			// Valid job id info, but invalid submit job (no pod spec)
			{
				JobRunId: uuid.NewString(),
				Queue:    "queue",
				Jobset:   "job-set",
				Job: &armadaevents.SubmitJob{
					JobId: jobId,
				},
			},
		},
	}

	jobRequester.RequestJobsRuns()

	assert.Len(t, eventReporter.ReceivedEvents, 1)
	assert.Len(t, eventReporter.ReceivedEvents[0].Event.Events, 1)
	failedEvent, ok := eventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobRunErrors)
	assert.True(t, ok)
	assert.Len(t, failedEvent.JobRunErrors.Errors, 1)
	assert.NotNil(t, failedEvent.JobRunErrors.Errors[0].GetPodError())
	assert.Equal(t, failedEvent.JobRunErrors.JobId, jobId)

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
				JobRunId: "",
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
	jobRequester := NewJobRequester(clusterId, eventReporter, leaseRequester, stateStore, utilisationService, podDefaults, defaultMaxLeasedJobs)
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
