package service

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/mocks"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/executorapi"
)

var (
	lease1                 = createJobRunLease("queue-1", "set-1")
	lease2                 = createJobRunLease("queue-2", "set-1")
	id1                    = uuid.NewString()
	id2                    = uuid.NewString()
	id3                    = uuid.NewString()
	defaultClusterIdentity = fake.NewFakeClusterIdentity("cluster-id", "cluster-pool")
	endMarker              = &executorapi.LeaseStreamMessage{
		Event: &executorapi.LeaseStreamMessage_End{
			End: &executorapi.EndMarker{},
		},
	}
)

func TestLeaseJobRuns(t *testing.T) {
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	tests := map[string]struct {
		leaseMessages        []*executorapi.JobRunLease
		cancelMessages       [][]string
		preemptMessages      [][]string
		expectedLeases       []*executorapi.JobRunLease
		expectedIdsToCancel  []string
		expectedIdsToPreempt []string
	}{
		"Lease Messages": {
			leaseMessages:        []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases:       []*executorapi.JobRunLease{lease1, lease2},
			expectedIdsToCancel:  []string{},
			expectedIdsToPreempt: []string{},
		},
		"Cancel Messages": {
			cancelMessages:       [][]string{{id1, id2}, {id3}},
			expectedLeases:       []*executorapi.JobRunLease{},
			expectedIdsToCancel:  []string{id1, id2, id3},
			expectedIdsToPreempt: []string{},
		},
		"Preempt Messages": {
			preemptMessages:      [][]string{{id1, id2}, {id3}},
			expectedLeases:       []*executorapi.JobRunLease{},
			expectedIdsToCancel:  []string{},
			expectedIdsToPreempt: []string{id1, id2, id3},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobRequester, mockExecutorApiClient, mockStream := setup(t)
			mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil)
			mockStream.EXPECT().Send(gomock.Any()).Return(nil)
			setStreamExpectations(mockStream, tc.leaseMessages, tc.cancelMessages, tc.preemptMessages)
			mockStream.EXPECT().Recv().Return(endMarker, nil)
			mockStream.EXPECT().Recv().Return(nil, io.EOF)

			response, err := jobRequester.LeaseJobRuns(ctx, &LeaseRequest{})
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedLeases, response.LeasedRuns)
			assert.Equal(t, tc.expectedIdsToCancel, response.RunIdsToCancel)
			assert.Equal(t, tc.expectedIdsToPreempt, response.RunIdsToPreempt)
		})
	}
}

func TestLeaseJobRuns_Send(t *testing.T) {
	shortCtx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()

	leaseRequest := &LeaseRequest{
		AvailableResource: armadaresource.ComputeResources{
			"cpu":    resource.MustParse("2"),
			"memory": resource.MustParse("2Gi"),
		},
		Nodes: []*executorapi.NodeInfo{
			{
				Name:          "node-1",
				RunIdsByState: map[string]api.JobState{"id1": api.JobState_RUNNING},
			},
		},
		UnassignedJobRunIds: []string{id1},
		MaxJobsToLease:      uint32(5),
	}

	expectedRequest := &executorapi.LeaseRequest{
		ExecutorId:             defaultClusterIdentity.GetClusterId(),
		Pool:                   defaultClusterIdentity.GetClusterPool(),
		Resources:              leaseRequest.AvailableResource.ToProtoMap(),
		Nodes:                  leaseRequest.Nodes,
		UnassignedJobRunIdsStr: leaseRequest.UnassignedJobRunIds,
		MaxJobsToLease:         leaseRequest.MaxJobsToLease,
	}

	jobRequester, mockExecutorApiClient, mockStream := setup(t)
	mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil)
	mockStream.EXPECT().Send(expectedRequest).Return(nil)
	mockStream.EXPECT().Recv().Return(endMarker, nil)
	mockStream.EXPECT().Recv().Return(nil, io.EOF)

	_, err := jobRequester.LeaseJobRuns(shortCtx, leaseRequest)
	assert.NoError(t, err)
}

func TestLeaseJobRuns_ReceiveError(t *testing.T) {
	endStreamMarkerTimeoutErr := fmt.Errorf("end of stream marker timeout")
	closeStreamErr := fmt.Errorf("close stream timeout")
	receiveErr := fmt.Errorf("recv error")
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	tests := map[string]struct {
		recvError      bool
		endOfStreamErr bool
		closeStreamErr bool
		shouldError    bool
		expectedError  error
		leaseMessages  []*executorapi.JobRunLease
		expectedLeases []*executorapi.JobRunLease
	}{
		"Happy Path": {
			shouldError:    false,
			leaseMessages:  []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases: []*executorapi.JobRunLease{lease1, lease2},
		},
		"RecvError": {
			recvError:      true,
			shouldError:    true,
			expectedError:  receiveErr,
			leaseMessages:  []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases: nil,
		},
		"Timeout - end stream marker": {
			endOfStreamErr: true,
			shouldError:    true,
			expectedError:  endStreamMarkerTimeoutErr,
			leaseMessages:  []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases: nil,
		},
		"Close stream error": {
			closeStreamErr: true,
			shouldError:    false,
			expectedError:  closeStreamErr,
			leaseMessages:  []*executorapi.JobRunLease{lease1, lease2},
			expectedLeases: []*executorapi.JobRunLease{lease1, lease2},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobRequester, mockExecutorApiClient, mockStream := setup(t)
			mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil)
			mockStream.EXPECT().Send(gomock.Any()).Return(nil)

			if tc.recvError {
				mockStream.EXPECT().Recv().Return(nil, receiveErr).AnyTimes()
			} else {
				setStreamExpectations(mockStream, tc.leaseMessages, nil, nil)

				if tc.endOfStreamErr {
					mockStream.EXPECT().Recv().Return(nil, endStreamMarkerTimeoutErr)
				} else {
					mockStream.EXPECT().Recv().Return(endMarker, nil)

					if tc.closeStreamErr {
						mockStream.EXPECT().Recv().Return(nil, closeStreamErr)
					} else {
						mockStream.EXPECT().Recv().Return(nil, io.EOF)
					}
				}
			}

			response, err := jobRequester.LeaseJobRuns(ctx, &LeaseRequest{})
			if tc.shouldError {
				assert.Error(t, err)
				assert.Nil(t, response)
				assert.Contains(t, err.Error(), tc.expectedError.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedLeases, response.LeasedRuns)
			}
		})
	}
}

func TestLeaseJobRuns_SendError(t *testing.T) {
	streamError := fmt.Errorf("stream error")
	sendError := fmt.Errorf("send error")
	ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 30*time.Second)
	defer cancel()
	tests := map[string]struct {
		streamError   bool
		sendError     bool
		expectedError error
	}{
		"StreamError": {
			streamError:   true,
			expectedError: streamError,
		},
		"SendError": {
			sendError:     true,
			expectedError: sendError,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobRequester, mockExecutorApiClient, mockStream := setup(t)
			if tc.streamError {
				mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, streamError).AnyTimes()
			} else {
				mockExecutorApiClient.EXPECT().LeaseJobRuns(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockStream, nil).AnyTimes()
			}

			if tc.sendError {
				mockStream.EXPECT().Send(gomock.Any()).Return(sendError).AnyTimes()
			} else {
				mockStream.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
			}

			mockStream.EXPECT().Recv().Times(0)

			response, err := jobRequester.LeaseJobRuns(ctx, &LeaseRequest{})
			assert.Error(t, err)
			assert.Nil(t, response)
			assert.Contains(t, err.Error(), tc.expectedError.Error())
		})
	}
}

func setup(t *testing.T) (*JobLeaseRequester, *mocks.MockExecutorApiClient, *mocks.MockExecutorApi_LeaseJobRunsClient) {
	ctrl := gomock.NewController(t)
	mockExecutorApiClient := mocks.NewMockExecutorApiClient(ctrl)
	mockStream := mocks.NewMockExecutorApi_LeaseJobRunsClient(ctrl)
	jobLeaseRequester := NewJobLeaseRequester(mockExecutorApiClient, defaultClusterIdentity)

	return jobLeaseRequester, mockExecutorApiClient, mockStream
}

func setStreamExpectations(stream *mocks.MockExecutorApi_LeaseJobRunsClient,
	leaseMessages []*executorapi.JobRunLease,
	cancelMessages [][]string,
	preemptMessages [][]string,
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
					JobRunIdsToCancelStr: cancelIds,
				},
			},
		}
		stream.EXPECT().Recv().Return(message, nil)
	}
	for _, preemptIds := range preemptMessages {
		message := &executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_PreemptRuns{
				PreemptRuns: &executorapi.PreemptRuns{
					JobRunIdsToPreemptStr: preemptIds,
				},
			},
		}
		stream.EXPECT().Recv().Return(message, nil)
	}
}

func createJobRunLease(queue string, jobSet string) *executorapi.JobRunLease {
	return &executorapi.JobRunLease{
		JobRunId: uuid.NewString(),
		Queue:    queue,
		Jobset:   jobSet,
		User:     "user",
		Groups:   []string{"group-1", "group-2"},
	}
}
