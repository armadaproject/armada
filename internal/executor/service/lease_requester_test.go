package service

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/context"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/mocks"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

var (
	lease1                = createJobRunLease("queue-1", "set-1")
	lease2                = createJobRunLease("queue-2", "set-1")
	id1                   = armadaevents.ProtoUuidFromUuid(uuid.New())
	id2                   = armadaevents.ProtoUuidFromUuid(uuid.New())
	id3                   = armadaevents.ProtoUuidFromUuid(uuid.New())
	defaultMinimumJobSize = armadaresource.ComputeResources{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("640Ki"),
	}
	defaultClusterIdentity = fake.NewFakeClusterIdentity("cluster-id", "cluster-pool")
	endMarker              = &executorapi.LeaseStreamMessage{
		Event: &executorapi.LeaseStreamMessage_End{
			End: &executorapi.EndMarker{},
		},
	}
)

func TestLeaseJobRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tests := map[string]struct {
		leaseMessages        []*executorapi.JobRunLease
		cancelMessages       [][]*armadaevents.Uuid
		preemptMessages      [][]*armadaevents.Uuid
		expectedLeases       []*executorapi.JobRunLease
		expectedIdsToCancel  []*armadaevents.Uuid
		expectedIdsToPreempt []*armadaevents.Uuid
	}{
		"Lease Messages": {
			leaseMessages:        []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases:       []*executorapi.JobRunLease{lease1, lease2},
			expectedIdsToCancel:  []*armadaevents.Uuid{},
			expectedIdsToPreempt: []*armadaevents.Uuid{},
		},
		"Cancel Messages": {
			cancelMessages:       [][]*armadaevents.Uuid{{id1, id2}, {id3}},
			expectedLeases:       []*executorapi.JobRunLease{},
			expectedIdsToCancel:  []*armadaevents.Uuid{id1, id2, id3},
			expectedIdsToPreempt: []*armadaevents.Uuid{},
		},
		"Preempt Messages": {
			preemptMessages:      [][]*armadaevents.Uuid{{id1, id2}, {id3}},
			expectedLeases:       []*executorapi.JobRunLease{},
			expectedIdsToCancel:  []*armadaevents.Uuid{},
			expectedIdsToPreempt: []*armadaevents.Uuid{id1, id2, id3},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobRequester, mockExecutorApiClient, mockStream := setup(t)
			mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil)
			mockStream.EXPECT().Send(gomock.Any()).Return(nil)
			setStreamExpectations(mockStream, tc.leaseMessages, tc.cancelMessages, tc.preemptMessages)
			mockStream.EXPECT().Recv().Return(endMarker, nil)

			response, err := jobRequester.LeaseJobRuns(ctx, &LeaseRequest{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedLeases, response.LeasedRuns)
			assert.Equal(t, tc.expectedIdsToCancel, response.RunIdsToCancel)
			assert.Equal(t, tc.expectedIdsToPreempt, response.RunIdsToPreempt)
		})
	}
}

func TestLeaseJobRuns_Send(t *testing.T) {
	shortCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leaseRequest := &LeaseRequest{
		AvailableResource: armadaresource.ComputeResources{
			"cpu":    resource.MustParse("2"),
			"memory": resource.MustParse("2Gi"),
		},
		Nodes: []*api.NodeInfo{
			{
				Name:          "node-1",
				RunIdsByState: map[string]api.JobState{"id1": api.JobState_RUNNING},
			},
		},
		UnassignedJobRunIds: []armadaevents.Uuid{*id1},
	}

	expectedRequest := &executorapi.LeaseRequest{
		ExecutorId:          defaultClusterIdentity.GetClusterId(),
		Pool:                defaultClusterIdentity.GetClusterPool(),
		Resources:           leaseRequest.AvailableResource,
		MinimumJobSize:      defaultMinimumJobSize,
		Nodes:               leaseRequest.Nodes,
		UnassignedJobRunIds: leaseRequest.UnassignedJobRunIds,
	}

	jobRequester, mockExecutorApiClient, mockStream := setup(t)
	mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil)
	mockStream.EXPECT().Send(expectedRequest).Return(nil)
	mockStream.EXPECT().Recv().Return(endMarker, nil)

	_, err := jobRequester.LeaseJobRuns(shortCtx, leaseRequest)
	assert.NoError(t, err)
}

func TestLeaseJobRuns_HandlesNoEndMarkerMessage(t *testing.T) {
	leaseMessages := []*executorapi.JobRunLease{lease1, lease2}
	shortCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	jobRequester, mockExecutorApiClient, mockStream := setup(t)
	mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil)
	mockStream.EXPECT().Send(gomock.Any()).Return(nil)
	setStreamExpectations(mockStream, leaseMessages, nil, nil)
	// No end marker, hang. Should
	mockStream.EXPECT().Recv().Do(func() {
		time.Sleep(time.Millisecond * 400)
	})

	response, err := jobRequester.LeaseJobRuns(shortCtx, &LeaseRequest{})
	// Timeout on context expiry
	assert.NoError(t, err)
	// Still receive leases that were received prior to the timeout
	assert.Equal(t, leaseMessages, response.LeasedRuns)
}

func TestLeaseJobRuns_Error(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tests := map[string]struct {
		streamError        bool
		sendError          bool
		recvError          bool
		recvEndOfFileError bool
		shouldError        bool
		leaseMessages      []*executorapi.JobRunLease
		expectedLeases     []*executorapi.JobRunLease
	}{
		"StreamError": {
			sendError:      true,
			shouldError:    true,
			leaseMessages:  []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases: nil,
		},
		"SendError": {
			sendError:      true,
			shouldError:    true,
			leaseMessages:  []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases: nil,
		},
		"RecvError": {
			recvError:      true,
			shouldError:    true,
			leaseMessages:  []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases: nil,
		},
		"RecvEOF": {
			recvEndOfFileError: true,
			shouldError:        false,
			leaseMessages:      []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases:     []*executorapi.JobRunLease{lease1, lease2},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobRequester, mockExecutorApiClient, mockStream := setup(t)
			if tc.streamError {
				mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("stream error")).AnyTimes()
			} else {
				mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil).AnyTimes()
			}

			if tc.sendError {
				mockStream.EXPECT().Send(gomock.Any()).Return(fmt.Errorf("send error")).AnyTimes()
			} else {
				mockStream.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
			}

			if tc.recvError || tc.recvEndOfFileError {
				if tc.recvError {
					mockStream.EXPECT().Recv().Return(nil, fmt.Errorf("recv error")).AnyTimes()
				}
				if tc.recvEndOfFileError {
					setStreamExpectations(mockStream, tc.leaseMessages, nil, nil)
					mockStream.EXPECT().Recv().Return(nil, io.EOF).AnyTimes()
				}
			}

			response, err := jobRequester.LeaseJobRuns(ctx, &LeaseRequest{})
			if tc.shouldError {
				assert.Error(t, err)
				assert.Nil(t, response)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedLeases, response.LeasedRuns)
			}
		})
	}
}

func setup(t *testing.T) (*JobLeaseRequester, *mocks.MockExecutorApiClient, *mocks.MockExecutorApi_LeaseJobRunsClient) {
	ctrl := gomock.NewController(t)
	mockExecutorApiClient := mocks.NewMockExecutorApiClient(ctrl)
	mockStream := mocks.NewMockExecutorApi_LeaseJobRunsClient(ctrl)
	jobLeaseRequester := NewJobLeaseRequester(mockExecutorApiClient, defaultClusterIdentity, defaultMinimumJobSize)

	return jobLeaseRequester, mockExecutorApiClient, mockStream
}

func setStreamExpectations(stream *mocks.MockExecutorApi_LeaseJobRunsClient,
	leaseMessages []*executorapi.JobRunLease,
	cancelMessages [][]*armadaevents.Uuid,
	preemptMessages [][]*armadaevents.Uuid,
) {
	for _, lease := range leaseMessages {
		message := &executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_Lease{
				Lease: lease,
			},
		}
		stream.EXPECT().Recv().Return(message, nil)
	}
	for _, cancelIds := range cancelMessages {
		message := &executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_CancelRuns{
				CancelRuns: &executorapi.CancelRuns{
					JobRunIdsToCancel: cancelIds,
				},
			},
		}
		stream.EXPECT().Recv().Return(message, nil)
	}
	for _, preemptIds := range preemptMessages {
		message := &executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_PreemptRuns{
				PreemptRuns: &executorapi.PreemptRuns{
					JobRunIdsToPreempt: preemptIds,
				},
			},
		}
		stream.EXPECT().Recv().Return(message, nil)
	}
}

func createJobRunLease(queue string, jobSet string) *executorapi.JobRunLease {
	return &executorapi.JobRunLease{
		JobRunId: armadaevents.ProtoUuidFromUuid(uuid.New()),
		Queue:    queue,
		Jobset:   jobSet,
		User:     "user",
		Groups:   []string{"group-1", "group-2"},
	}
}
