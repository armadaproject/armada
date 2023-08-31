package scheduler

import (
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/mocks"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/database"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

const nodeIdName = "kubernetes.io/hostname"

func TestExecutorApi_LeaseJobRuns(t *testing.T) {
	const maxJobsPerCall = uint(100)
	testClock := clock.NewFakeClock(time.Now())
	runId1 := uuid.New()
	runId2 := uuid.New()
	runId3 := uuid.New()
	groups, compressedGroups := groups(t)
	defaultRequest := &executorapi.LeaseRequest{
		ExecutorId: "test-executor",
		Pool:       "test-pool",
		Nodes: []*api.NodeInfo{
			{
				Name: "test-node",
				RunIdsByState: map[string]api.JobState{
					runId1.String(): api.JobState_RUNNING,
					runId2.String(): api.JobState_RUNNING,
				},
				NodeType: "node-type-1",
			},
		},
		UnassignedJobRunIds: []armadaevents.Uuid{*armadaevents.ProtoUuidFromUuid(runId3)},
	}
	defaultExpectedExecutor := &schedulerobjects.Executor{
		Id:   "test-executor",
		Pool: "test-pool",
		Nodes: []*schedulerobjects.Node{
			{
				Id:                          "test-executor-test-node",
				Name:                        "test-node",
				Executor:                    "test-executor",
				TotalResources:              schedulerobjects.ResourceList{},
				StateByJobRunId:             map[string]schedulerobjects.JobRunState{runId1.String(): schedulerobjects.JobRunState_RUNNING, runId2.String(): schedulerobjects.JobRunState_RUNNING},
				NonArmadaAllocatedResources: map[int32]schedulerobjects.ResourceList{},
				AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
					1000: {
						Resources: nil,
					},
					2000: {
						Resources: nil,
					},
				},
				LastSeen:             testClock.Now().UTC(),
				ResourceUsageByQueue: map[string]*schedulerobjects.ResourceList{},
				ReportingNodeType:    "node-type-1",
			},
		},
		MinimumJobSize:    schedulerobjects.ResourceList{},
		LastUpdateTime:    testClock.Now().UTC(),
		UnassignedJobRuns: []string{runId3.String()},
	}

	submit, compressedSubmit := submitMsg(t, "node-id")
	defaultLease := &database.JobRunLease{
		RunID:         uuid.New(),
		Queue:         "test-queue",
		JobSet:        "test-jobset",
		UserID:        "test-user",
		Node:          "node-id",
		Groups:        compressedGroups,
		SubmitMessage: compressedSubmit,
	}

	submitWithoutNodeSelector, compressedSubmitNoNodeSelector := submitMsg(t, "")
	leaseWithoutNode := &database.JobRunLease{
		RunID:         uuid.New(),
		Queue:         "test-queue",
		JobSet:        "test-jobset",
		UserID:        "test-user",
		Groups:        compressedGroups,
		SubmitMessage: compressedSubmitNoNodeSelector,
	}

	tests := map[string]struct {
		request          *executorapi.LeaseRequest
		runsToCancel     []uuid.UUID
		leases           []*database.JobRunLease
		expectedExecutor *schedulerobjects.Executor
		expectedMsgs     []*executorapi.LeaseStreamMessage
	}{
		"lease and cancel": {
			request:          defaultRequest,
			runsToCancel:     []uuid.UUID{runId2},
			leases:           []*database.JobRunLease{defaultLease},
			expectedExecutor: defaultExpectedExecutor,
			expectedMsgs: []*executorapi.LeaseStreamMessage{
				{
					Event: &executorapi.LeaseStreamMessage_CancelRuns{CancelRuns: &executorapi.CancelRuns{
						JobRunIdsToCancel: []*armadaevents.Uuid{armadaevents.ProtoUuidFromUuid(runId2)},
					}},
				},
				{
					Event: &executorapi.LeaseStreamMessage_Lease{Lease: &executorapi.JobRunLease{
						JobRunId: armadaevents.ProtoUuidFromUuid(defaultLease.RunID),
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
						JobRunId: armadaevents.ProtoUuidFromUuid(leaseWithoutNode.RunID),
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ctrl := gomock.NewController(t)
			mockPulsarProducer := mocks.NewMockProducer(ctrl)
			mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
			mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			mockLegacyExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			mockStream := schedulermocks.NewMockExecutorApi_LeaseJobRunsServer(ctrl)

			runIds, err := runIdsFromLeaseRequest(tc.request)
			require.NoError(t, err)

			// set up mocks
			mockStream.EXPECT().Context().Return(ctx).AnyTimes()
			mockStream.EXPECT().Recv().Return(tc.request, nil).Times(1)
			mockExecutorRepository.EXPECT().StoreExecutor(ctx, gomock.Any()).DoAndReturn(func(ctx *context.ArmadaContext, executor *schedulerobjects.Executor) error {
				assert.Equal(t, tc.expectedExecutor, executor)
				return nil
			}).Times(1)
			mockLegacyExecutorRepository.EXPECT().StoreExecutor(ctx, gomock.Any()).DoAndReturn(func(ctx *context.ArmadaContext, executor *schedulerobjects.Executor) error {
				assert.Equal(t, tc.expectedExecutor, executor)
				return nil
			}).Times(1)
			mockJobRepository.EXPECT().FindInactiveRuns(gomock.Any(), schedulermocks.SliceMatcher[uuid.UUID]{Expected: runIds}).Return(tc.runsToCancel, nil).Times(1)
			mockJobRepository.EXPECT().FetchJobRunLeases(gomock.Any(), tc.request.ExecutorId, maxJobsPerCall, runIds).Return(tc.leases, nil).Times(1)

			// capture all sent messages
			var capturedEvents []*executorapi.LeaseStreamMessage
			mockStream.EXPECT().Send(gomock.Any()).
				Do(func(msg *executorapi.LeaseStreamMessage) {
					capturedEvents = append(capturedEvents, msg)
				}).AnyTimes()

			server, err := NewExecutorApi(
				mockPulsarProducer,
				mockJobRepository,
				mockExecutorRepository,
				mockLegacyExecutorRepository,
				[]int32{1000, 2000},
				maxJobsPerCall,
				"kubernetes.io/hostname",
				nil,
				4*1024*1024,
			)
			require.NoError(t, err)
			server.clock = testClock

			err = server.LeaseJobRuns(mockStream)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedMsgs, capturedEvents)
			cancel()
		})
	}
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ctrl := gomock.NewController(t)
			mockPulsarProducer := mocks.NewMockProducer(ctrl)
			mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
			mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			mockLegacyExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)

			// capture all sent messages
			var capturedEvents []*armadaevents.EventSequence
			mockPulsarProducer.
				EXPECT().
				SendAsync(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ *context.ArmadaContext, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
					es := &armadaevents.EventSequence{}
					err := proto.Unmarshal(msg.Payload, es)
					require.NoError(t, err)
					capturedEvents = append(capturedEvents, es)
					callback(pulsarutils.NewMessageId(1), msg, nil)
				}).AnyTimes()

			server, err := NewExecutorApi(
				mockPulsarProducer,
				mockJobRepository,
				mockExecutorRepository,
				mockLegacyExecutorRepository,
				[]int32{1000, 2000},
				100,
				"kubernetes.io/hostname",
				nil,
				4*1024*1024,
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

func submitMsg(t *testing.T, nodeName string) (*armadaevents.SubmitJob, []byte) {
	podSpec := &v1.PodSpec{}
	if nodeName != "" {
		podSpec.NodeSelector = map[string]string{}
		podSpec.NodeSelector[nodeIdName] = nodeName
	}
	submitMsg := &armadaevents.SubmitJob{
		JobId: armadaevents.ProtoUuidFromUuid(uuid.New()),
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
