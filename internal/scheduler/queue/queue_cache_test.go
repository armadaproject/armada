package queue

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/pkg/api"
)

func TestFetch(t *testing.T) {
	tests := map[string]struct {
		queues      []*api.Queue
		streamError bool
	}{
		"No Queues": {
			queues: []*api.Queue{},
		},
		"One Queue": {
			queues: []*api.Queue{{Name: "testQueue1"}},
		},
		"Two Queues": {
			queues: []*api.Queue{
				{Name: "testQueue1"},
				{Name: "testQueue2"},
			},
		},
		"Immediate Stream Error": {
			queues:      []*api.Queue{},
			streamError: true,
		},
		"Steam Error Mid-Stream": {
			queues:      []*api.Queue{{Name: "testQueue1"}},
			streamError: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			ctrl := gomock.NewController(t)
			mockApiClient := schedulermocks.NewMockSubmitClient(ctrl)
			mockStream := schedulermocks.NewMockSubmit_GetQueuesClient(ctrl)
			for _, queue := range tc.queues {
				mockStream.
					EXPECT().
					Recv().
					Return(
						&api.StreamingQueueMessage{
							Event: &api.StreamingQueueMessage_Queue{Queue: queue},
						}, nil)
			}

			if tc.streamError {
				mockStream.
					EXPECT().
					Recv().
					Return(nil, fmt.Errorf("dummy error"))
			} else {
				appEnd := mockStream.
					EXPECT().
					Recv().
					Return(
						&api.StreamingQueueMessage{
							Event: &api.StreamingQueueMessage_End{},
						}, nil)
				grpcEnd := mockStream.
					EXPECT().
					Recv().
					Return(nil, io.EOF)

				gomock.InOrder(
					appEnd,
					grpcEnd,
				)
			}

			mockApiClient.EXPECT().GetQueues(ctx, gomock.Any()).Return(mockStream, nil).Times(1)

			cache := NewQueueCache(mockApiClient, 1*time.Millisecond)
			fetchErr := cache.fetchQueues(ctx)
			queues, getErr := cache.GetAll(ctx)

			if tc.streamError {
				assert.Error(t, fetchErr)
				assert.Error(t, getErr)
			} else {
				assert.NoError(t, fetchErr)
				assert.NoError(t, getErr)
				assert.Equal(t, tc.queues, queues)
			}

			ctrl.Finish()
			cancel()
		})
	}
}
