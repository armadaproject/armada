package scheduler

import (
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/scheduler/database"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func TestExecutorApi_LeaseJobRuns(t *testing.T) {
	const maxJobsPerCall = uint(100)
	testClock := clock.NewFakeClock(time.Now())
	runId1 := uuid.New()
	runId2 := uuid.New()
	runId3 := uuid.New()
	submit, compressedSubmit := submitMsg(t)
	groups, compressedGroups := groups(t)
	defaultRequest := &executorapi.LeaseRequest{
		ExecutorId: "test-executor",
		Pool:       "test-pool",
		Nodes: []*api.NodeInfo{
			{
				Name:   "test-node",
				RunIds: []string{runId1.String(), runId2.String()},
			},
		},
		UnassignedJobRunIds: []armadaevents.Uuid{*armadaevents.ProtoUuidFromUuid(runId3)},
	}
	defaultExpectedExecutor := &schedulerobjects.Executor{
		Id:   "test-executor",
		Pool: "test-pool",
		Nodes: []*schedulerobjects.Node{
			{
				Id:             "test-executor-test-node",
				TotalResources: schedulerobjects.ResourceList{},
				JobRuns:        []string{runId1.String(), runId2.String()},
				AllocatableByPriorityAndResource: map[int32]schedulerobjects.ResourceList{
					1000: {
						Resources: map[string]resource.Quantity{},
					},
					2000: {
						Resources: map[string]resource.Quantity{},
					},
				},
				LastSeen: testClock.Now().UTC(),
			},
		},
		MinimumJobSize:    schedulerobjects.ResourceList{},
		LastUpdateTime:    testClock.Now().UTC(),
		UnassignedJobRuns: []string{runId3.String()},
	}

	defaultLease := &database.JobRunLease{
		RunID:         uuid.New(),
		Queue:         "test-queue",
		JobSet:        "test-jobset",
		UserID:        "test-user",
		Groups:        compressedGroups,
		SubmitMessage: compressedSubmit,
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
			mockPulsarProducer := schedulermocks.NewMockProducer(ctrl)
			mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
			mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)
			mockStream := schedulermocks.NewMockExecutorApi_LeaseJobRunsServer(ctrl)

			runIds, err := extractRunIds(tc.request)
			require.NoError(t, err)

			// set up mocks
			mockStream.EXPECT().Context().Return(ctx).AnyTimes()
			mockStream.EXPECT().Recv().Return(tc.request, nil).Times(1)
			mockExecutorRepository.EXPECT().StoreExecutor(ctx, gomock.Any()).DoAndReturn(func(ctx context.Context, executor *schedulerobjects.Executor) error {
				assert.Equal(t, tc.expectedExecutor, executor)
				return nil
			}).Times(1)
			mockJobRepository.EXPECT().FindInactiveRuns(gomock.Any(), runIds).Return(tc.runsToCancel, nil).Times(1)
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
				[]int32{1000, 2000},
				maxJobsPerCall,
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
			mockPulsarProducer := schedulermocks.NewMockProducer(ctrl)
			mockJobRepository := schedulermocks.NewMockJobRepository(ctrl)
			mockExecutorRepository := schedulermocks.NewMockExecutorRepository(ctrl)

			// capture all sent messages
			var capturedEvents []*armadaevents.EventSequence
			mockPulsarProducer.
				EXPECT().
				SendAsync(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
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
				[]int32{1000, 2000},
				100,
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

func submitMsg(t *testing.T) (*armadaevents.SubmitJob, []byte) {
	submitMsg := &armadaevents.SubmitJob{
		JobId: armadaevents.ProtoUuidFromUuid(uuid.New()),
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
