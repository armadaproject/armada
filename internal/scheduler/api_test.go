package scheduler

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	clock "k8s.io/utils/clock/testing"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/mocks"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/slices"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/testutil"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconfig "github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/internal/server/configuration"
	servermocks "github.com/armadaproject/armada/internal/server/mocks"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

const nodeIdName = "kubernetes.io/hostname"

const (
	armadaDefaultPriorityClassName     = "armada-default"
	armadaPreemptiblePriorityClassName = "armada-preemptible"
)

var priorityClasses = map[string]types.PriorityClass{
	armadaDefaultPriorityClassName:     {Preemptible: false},
	armadaPreemptiblePriorityClassName: {Preemptible: true},
}

func TestExecutorApi_LeaseJobRuns(t *testing.T) {
	const maxJobsPerCall = uint(100)
	testClock := clock.NewFakeClock(time.Now())
	runId1 := uuid.NewString()
	runId2 := uuid.NewString()
	runId3 := uuid.NewString()
	groups, compressedGroups := groups(t)
	defaultRequest := &executorapi.LeaseRequest{
		ExecutorId: "test-executor",
		Pool:       "test-pool",
		Nodes: []*executorapi.NodeInfo{
			{
				Name: "test-node",
				RunIdsByState: map[string]api.JobState{
					runId1: api.JobState_RUNNING,
					runId2: api.JobState_RUNNING,
				},
				NodeType:                    "node-type-1",
				ResourceUsageByQueueAndPool: []*executorapi.PoolQueueResource{},
			},
		},
		UnassignedJobRunIds: []string{runId3},
		MaxJobsToLease:      uint32(maxJobsPerCall),
	}
	defaultExpectedExecutor := &schedulerobjects.Executor{
		Id:   "test-executor",
		Pool: "test-pool",
		Nodes: []*schedulerobjects.Node{
			{
				Id:                          "test-executor-test-node",
				Name:                        "test-node",
				Executor:                    "test-executor",
				TotalResources:              &schedulerobjects.ResourceList{Resources: map[string]*resource.Quantity{}},
				StateByJobRunId:             map[string]schedulerobjects.JobRunState{runId1: schedulerobjects.JobRunState_RUNNING, runId2: schedulerobjects.JobRunState_RUNNING},
				UnallocatableResources:      map[int32]*schedulerobjects.ResourceList{},
				ResourceUsageByQueueAndPool: []*schedulerobjects.PoolQueueResource{},
				LastSeen:                    protoutil.ToTimestamp(testClock.Now().UTC()),
				ReportingNodeType:           "node-type-1",
			},
		},
		LastUpdateTime:    protoutil.ToTimestamp(testClock.Now().UTC()),
		UnassignedJobRuns: []string{runId3},
	}

	submit, compressedSubmit := submitMsg(
		t,
		&armadaevents.ObjectMeta{
			Labels:      map[string]string{armadaJobPreemptibleLabel: "false"},
			Annotations: map[string]string{configuration.PoolAnnotation: "test-pool"},
		},
		&v1.PodSpec{
			NodeSelector: map[string]string{nodeIdName: "node-id"},
		},
	)
	defaultLease := &database.JobRunLease{
		RunID:         uuid.NewString(),
		Queue:         "test-queue",
		Pool:          "test-pool",
		JobSet:        "test-jobset",
		UserID:        "test-user",
		Node:          "node-id",
		Groups:        compressedGroups,
		SubmitMessage: compressedSubmit,
	}

	submitWithoutNodeSelector, compressedSubmitNoNodeSelector := submitMsg(t,
		&armadaevents.ObjectMeta{
			Labels:      map[string]string{armadaJobPreemptibleLabel: "false"},
			Annotations: map[string]string{configuration.PoolAnnotation: "test-pool"},
		},
		nil,
	)
	leaseWithoutNode := &database.JobRunLease{
		RunID:         uuid.NewString(),
		Queue:         "test-queue",
		Pool:          "test-pool",
		JobSet:        "test-jobset",
		UserID:        "test-user",
		Groups:        compressedGroups,
		SubmitMessage: compressedSubmitNoNodeSelector,
	}

	preemptibleSubmit, preemptibleCompressedSubmit := submitMsg(
		t,
		&armadaevents.ObjectMeta{
			Labels:      map[string]string{armadaJobPreemptibleLabel: "true"},
			Annotations: map[string]string{configuration.PoolAnnotation: "test-pool"},
		},

		&v1.PodSpec{
			PriorityClassName: armadaPreemptiblePriorityClassName,
			NodeSelector:      map[string]string{nodeIdName: "node-id"},
		},
	)
	preemptibleLease := &database.JobRunLease{
		RunID:         uuid.NewString(),
		Queue:         "test-queue",
		Pool:          "test-pool",
		JobSet:        "test-jobset",
		UserID:        "test-user",
		Node:          "node-id",
		Groups:        compressedGroups,
		SubmitMessage: preemptibleCompressedSubmit,
	}

	tolerations := []v1.Toleration{
		{
			Key:    "whale",
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		},
	}
	leaseWithOverlay := &database.JobRunLease{
		RunID:  uuid.NewString(),
		Queue:  "test-queue",
		Pool:   "test-pool",
		JobSet: "test-jobset",
		UserID: "test-user",
		Node:   "node-id",
		Groups: compressedGroups,
		// We use the submit message without tolerations here, because the
		// run-level tolerations are stored in PodRequirementsOverlay.
		SubmitMessage: compressedSubmit,
		PodRequirementsOverlay: protoutil.MustMarshall(
			&schedulerobjects.PodRequirements{
				Tolerations: armadaslices.Map(tolerations, func(t v1.Toleration) *v1.Toleration {
					return &t
				}),
				Annotations: map[string]string{configuration.PoolAnnotation: "test-pool", "runtime_gang_cardinality": "3"},
			},
		),
	}
	submitWithOverlay, _ := submitMsg(
		t,
		&armadaevents.ObjectMeta{
			Annotations: map[string]string{configuration.PoolAnnotation: "test-pool", "runtime_gang_cardinality": "3"},
			Labels:      map[string]string{armadaJobPreemptibleLabel: "false"},
		},
		&v1.PodSpec{
			NodeSelector: map[string]string{nodeIdName: "node-id"},
			Tolerations:  tolerations,
		},
	)
	submitWithOverlay.JobId = submit.JobId

	tests := map[string]struct {
		request          *executorapi.LeaseRequest
		runsToCancel     []string
		leases           []*database.JobRunLease
		expectedExecutor *schedulerobjects.Executor
		expectedMsgs     []*executorapi.LeaseStreamMessage
	}{
		"lease and cancel": {
			request:          defaultRequest,
			runsToCancel:     []string{runId2},
			leases:           []*database.JobRunLease{defaultLease},
			expectedExecutor: defaultExpectedExecutor,
			expectedMsgs: []*executorapi.LeaseStreamMessage{
				{
					Event: &executorapi.LeaseStreamMessage_CancelRuns{CancelRuns: &executorapi.CancelRuns{
						JobRunIdsToCancel: []string{runId2},
					}},
				},
				{
					Event: &executorapi.LeaseStreamMessage_Lease{Lease: &executorapi.JobRunLease{
						JobRunId: defaultLease.RunID,
						Queue:    defaultLease.Queue,
						Jobset:   defaultLease.JobSet,
						User:     defaultLease.UserID,
						Groups:   groups,
						Job:      submit,
					}},
				},
				{
					Event: &executorapi.LeaseStreamMessage_End{End: &executorapi.EndMarker{}},
				},
			},
		},
		"no node selector when missing in lease": {
			request:          defaultRequest,
			leases:           []*database.JobRunLease{leaseWithoutNode},
			expectedExecutor: defaultExpectedExecutor,
			expectedMsgs: []*executorapi.LeaseStreamMessage{
				{
					Event: &executorapi.LeaseStreamMessage_Lease{Lease: &executorapi.JobRunLease{
						JobRunId: leaseWithoutNode.RunID,
						Queue:    leaseWithoutNode.Queue,
						Jobset:   leaseWithoutNode.JobSet,
						User:     leaseWithoutNode.UserID,
						Groups:   groups,
						Job:      submitWithoutNodeSelector,
					}},
				},
				{
					Event: &executorapi.LeaseStreamMessage_End{End: &executorapi.EndMarker{}},
				},
			},
		},
		"run with PodRequirementsOverlay": {
			request:          defaultRequest,
			leases:           []*database.JobRunLease{leaseWithOverlay},
			expectedExecutor: defaultExpectedExecutor,
			expectedMsgs: []*executorapi.LeaseStreamMessage{
				{
					Event: &executorapi.LeaseStreamMessage_Lease{Lease: &executorapi.JobRunLease{
						JobRunId: leaseWithOverlay.RunID,
						Queue:    leaseWithOverlay.Queue,
						Jobset:   leaseWithOverlay.JobSet,
						User:     leaseWithOverlay.UserID,
						Groups:   groups,
						Job:      submitWithOverlay,
					}},
				},
				{
					Event: &executorapi.LeaseStreamMessage_End{End: &executorapi.EndMarker{}},
				},
			},
		},
		"preemptible job lease": {
			request:          defaultRequest,
			leases:           []*database.JobRunLease{preemptibleLease},
			expectedExecutor: defaultExpectedExecutor,
			expectedMsgs: []*executorapi.LeaseStreamMessage{
				{
					Event: &executorapi.LeaseStreamMessage_Lease{Lease: &executorapi.JobRunLease{
						JobRunId: preemptibleLease.RunID,
						Queue:    preemptibleLease.Queue,
						Jobset:   preemptibleLease.JobSet,
						User:     preemptibleLease.UserID,
						Groups:   groups,
						Job:      preemptibleSubmit,
					}},
				},
				{
					Event: &executorapi.LeaseStreamMessage_End{End: &executorapi.EndMarker{}},
				},
			},
		},
		"do nothing": {
			request:          defaultRequest,
			expectedExecutor: defaultExpectedExecutor,
			expectedMsgs: []*executorapi.LeaseStreamMessage{
				{
					Event: &executorapi.LeaseStreamMessage_End{End: &executorapi.EndMarker{}},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctrl := gomock.NewController(t)
			mockPulsarPublisher := mocks.NewMockPublisher[*armadaevents.EventSequence](ctrl)
			mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
			mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			mockStream := schedulermocks.NewMockExecutorApi_LeaseJobRunsServer[any, any](ctrl)
			mockAuthorizer := servermocks.NewMockActionAuthorizer(ctrl)

			runIds, err := runIdsFromLeaseRequest(tc.request)
			require.NoError(t, err)

			// set up mocks
			mockStream.EXPECT().Context().Return(ctx).AnyTimes()
			mockStream.EXPECT().Recv().Return(tc.request, nil).Times(1)
			mockExecutorRepository.EXPECT().StoreExecutor(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx *armadacontext.Context, executor *schedulerobjects.Executor) error {
				assert.Equal(t, tc.expectedExecutor, executor)
				return nil
			}).Times(1)
			mockJobRepository.EXPECT().FindInactiveRuns(gomock.Any(), schedulermocks.SliceMatcher{Expected: runIds}).Return(tc.runsToCancel, nil).Times(1)
			mockJobRepository.EXPECT().FetchJobRunLeases(gomock.Any(), tc.request.ExecutorId, maxJobsPerCall, runIds).Return(tc.leases, nil).Times(1)
			mockAuthorizer.EXPECT().AuthorizeAction(gomock.Any(), permission.Permission(permissions.ExecuteJobs)).Return(nil).Times(1)

			// capture all sent messages
			var capturedEvents []*executorapi.LeaseStreamMessage
			mockStream.EXPECT().Send(gomock.Any()).
				Do(func(msg *executorapi.LeaseStreamMessage) {
					capturedEvents = append(capturedEvents, msg)
				}).AnyTimes()

			server, err := NewExecutorApi(
				mockPulsarPublisher,
				mockJobRepository,
				mockExecutorRepository,
				[]int32{1000, 2000},
				testResourceNames(),
				"kubernetes.io/hostname",
				nil,
				priorityClasses,
				mockAuthorizer,
			)
			require.NoError(t, err)
			server.clock = testClock

			err = server.LeaseJobRuns(mockStream)
			require.NoError(t, err)
			testutil.AssertProtoEqual(t, tc.expectedMsgs, capturedEvents)
			cancel()
		})
	}
}

func TestExecutorApi_LeaseJobRuns_Unauthorised(t *testing.T) {
	request := &executorapi.LeaseRequest{
		ExecutorId: "test-executor",
		Pool:       "test-pool",
		Nodes: []*executorapi.NodeInfo{
			{
				Name:          "test-node",
				RunIdsByState: map[string]api.JobState{},
				NodeType:      "node-type-1",
			},
		},
		UnassignedJobRunIds: []string{},
		MaxJobsToLease:      uint32(100),
	}

	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	ctrl := gomock.NewController(t)
	mockPulsarPublisher := mocks.NewMockPublisher[*armadaevents.EventSequence](ctrl)
	mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
	mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
	mockStream := schedulermocks.NewMockExecutorApi_LeaseJobRunsServer[any, any](ctrl)
	mockAuthorizer := servermocks.NewMockActionAuthorizer(ctrl)

	// set up mocks
	mockStream.EXPECT().Context().Return(ctx).AnyTimes()
	mockStream.EXPECT().Recv().Return(request, nil).Times(1)
	mockAuthorizer.EXPECT().AuthorizeAction(gomock.Any(), permission.Permission(permissions.ExecuteJobs)).Return(&armadaerrors.ErrUnauthorized{Message: "authorised"}).Times(1)
	// capture all sent messages
	var capturedEvents []*executorapi.LeaseStreamMessage
	mockStream.EXPECT().Send(gomock.Any()).
		Do(func(msg *executorapi.LeaseStreamMessage) {
			capturedEvents = append(capturedEvents, msg)
		}).AnyTimes()

	server, err := NewExecutorApi(
		mockPulsarPublisher,
		mockJobRepository,
		mockExecutorRepository,
		[]int32{1000, 2000},
		testResourceNames(),
		"kubernetes.io/hostname",
		nil,
		priorityClasses,
		mockAuthorizer,
	)

	require.NoError(t, err)

	err = server.LeaseJobRuns(mockStream)
	assert.Error(t, err)
	assert.Empty(t, capturedEvents)

	statusErr, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, statusErr.Code())
}

func TestAddNodeSelector(t *testing.T) {
	withNodeSelector := &armadaevents.PodSpecWithAvoidList{
		PodSpec: &v1.PodSpec{
			NodeSelector: map[string]string{nodeIdName: "node-1"},
		},
	}

	tests := map[string]struct {
		input    *armadaevents.PodSpecWithAvoidList
		expected *armadaevents.PodSpecWithAvoidList
		key      string
		value    string
	}{
		"Sets node selector": {
			input:    &armadaevents.PodSpecWithAvoidList{PodSpec: &v1.PodSpec{}},
			expected: withNodeSelector,
			key:      nodeIdName,
			value:    "node-1",
		},
		"input is nil": {
			input:    nil,
			expected: nil,
			key:      nodeIdName,
			value:    "node-1",
		},
		"podspec is nil": {
			input:    &armadaevents.PodSpecWithAvoidList{},
			expected: &armadaevents.PodSpecWithAvoidList{},
			key:      nodeIdName,
			value:    "node-1",
		},
		"key is not set": {
			input:    &armadaevents.PodSpecWithAvoidList{PodSpec: &v1.PodSpec{}},
			expected: &armadaevents.PodSpecWithAvoidList{PodSpec: &v1.PodSpec{}},
			key:      "",
			value:    "node-1",
		},
		"value is not set": {
			input:    &armadaevents.PodSpecWithAvoidList{PodSpec: &v1.PodSpec{}},
			expected: &armadaevents.PodSpecWithAvoidList{PodSpec: &v1.PodSpec{}},
			key:      nodeIdName,
			value:    "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			addNodeSelector(tc.input, tc.key, tc.value)
			assert.Equal(t, tc.expected, tc.input)
		})
	}
}

func TestExecutorApi_Publish(t *testing.T) {
	tests := map[string]struct {
		sequences []*armadaevents.EventSequence
	}{
		"happy path": {
			sequences: []*armadaevents.EventSequence{
				{
					Queue:      "queue1",
					JobSetName: "jobset1",
					Events: []*armadaevents.EventSequence_Event{
						{
							Event: &armadaevents.EventSequence_Event_JobRunErrors{
								JobRunErrors: &armadaevents.JobRunErrors{},
							},
						},
					},
				},
				{
					Queue:      "queue2",
					JobSetName: "jobset2",
					Events: []*armadaevents.EventSequence_Event{
						{
							Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
								JobRunSucceeded: &armadaevents.JobRunSucceeded{},
							},
						},
					},
				},
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctrl := gomock.NewController(t)
			mockPulsarPublisher := mocks.NewMockPublisher[*armadaevents.EventSequence](ctrl)
			mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
			mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			mockAuthorizer := servermocks.NewMockActionAuthorizer(ctrl)

			// capture all sent messages
			var capturedEvents []*armadaevents.EventSequence
			mockPulsarPublisher.
				EXPECT().
				PublishMessages(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ *armadacontext.Context, sequences ...*armadaevents.EventSequence) error {
					for _, es := range sequences {
						capturedEvents = append(capturedEvents, es)
					}
					return nil
				}).AnyTimes()
			mockAuthorizer.EXPECT().AuthorizeAction(gomock.Any(), permission.Permission(permissions.ExecuteJobs)).Return(nil).Times(1)

			server, err := NewExecutorApi(
				mockPulsarPublisher,
				mockJobRepository,
				mockExecutorRepository,
				[]int32{1000, 2000},
				testResourceNames(),
				"kubernetes.io/hostname",
				nil,
				priorityClasses,
				mockAuthorizer,
			)

			require.NoError(t, err)

			empty, err := server.ReportEvents(ctx, &executorapi.EventList{Events: tc.sequences})
			require.NoError(t, err)
			assert.NotNil(t, empty)
			assert.Equal(t, tc.sequences, capturedEvents)
			cancel()
		})
	}
}

func TestExecutorApi_Publish_Unauthorised(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
	defer cancel()
	ctrl := gomock.NewController(t)
	mockPulsarPublisher := mocks.NewMockPublisher[*armadaevents.EventSequence](ctrl)
	mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
	mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
	mockAuthorizer := servermocks.NewMockActionAuthorizer(ctrl)

	sequences := []*armadaevents.EventSequence{
		{
			Queue:      "queue1",
			JobSetName: "jobset1",
			Events: []*armadaevents.EventSequence_Event{
				{
					Event: &armadaevents.EventSequence_Event_JobRunErrors{
						JobRunErrors: &armadaevents.JobRunErrors{},
					},
				},
			},
		},
	}
	mockAuthorizer.EXPECT().AuthorizeAction(gomock.Any(), permission.Permission(permissions.ExecuteJobs)).Return(&armadaerrors.ErrUnauthorized{Message: "authorised"}).Times(1)

	server, err := NewExecutorApi(
		mockPulsarPublisher,
		mockJobRepository,
		mockExecutorRepository,
		[]int32{1000, 2000},
		testResourceNames(),
		"kubernetes.io/hostname",
		nil,
		priorityClasses,
		mockAuthorizer,
	)

	require.NoError(t, err)

	empty, err := server.ReportEvents(ctx, &executorapi.EventList{Events: sequences})
	assert.Error(t, err)
	assert.Nil(t, empty)

	statusErr, ok := status.FromError(err)
	assert.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, statusErr.Code())
}

func submitMsg(t *testing.T, objectMeta *armadaevents.ObjectMeta, podSpec *v1.PodSpec) (*armadaevents.SubmitJob, []byte) {
	submitMsg := &armadaevents.SubmitJob{
		JobId:      util.NewULID(),
		ObjectMeta: objectMeta,
		MainObject: &armadaevents.KubernetesMainObject{
			Object: &armadaevents.KubernetesMainObject_PodSpec{
				PodSpec: &armadaevents.PodSpecWithAvoidList{
					PodSpec: podSpec,
				},
			},
		},
	}
	bytes, err := proto.Marshal(submitMsg)
	require.NoError(t, err)
	compressor, err := compress.NewZlibCompressor(1024)
	require.NoError(t, err)
	compressed, err := compressor.Compress(bytes)
	require.NoError(t, err)
	return submitMsg, compressed
}

func groups(t *testing.T) ([]string, []byte) {
	groups := []string{"group1", "group2"}
	compressor, err := compress.NewZlibCompressor(1024)
	require.NoError(t, err)
	compressed, err := compress.CompressStringArray(groups, compressor)
	require.NoError(t, err)
	return groups, compressed
}

func testResourceNames() []string {
	return slices.Map(testfixtures.GetTestSupportedResourceTypes(), func(rt schedulerconfig.ResourceType) string { return rt.Name })
}
