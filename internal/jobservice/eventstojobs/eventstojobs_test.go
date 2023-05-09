package eventstojobs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/eventstojobs"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/pkg/api"
)

type MockEventClient struct{
	eventStreamMessage *api.EventStreamMessage
	err error
}

func (m MockEventClient) Recv() (*api.EventStreamMessage, error) {
	msg := m.eventStreamMessage
	if msg == nil {
		msg = &api.EventStreamMessage{
			Id: "msgID",
			Message: &api.EventMessage{},
		}
	}
	return msg, m.err
}

func (m MockEventClient) CloseSend() error {
	return nil
}

func (m MockEventClient) Context() context.Context{
	return context.Background()
}

func (m MockEventClient) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (m MockEventClient) Trailer() metadata.MD {
	return metadata.MD{}
}

func (m MockEventClient) SendMsg(msg interface{}) error {
	return nil
}

func (m MockEventClient) RecvMsg(msg interface{}) error {
	return nil
}

func Test_SubscribeToJobSetId(t *testing.T) {
	tests := []struct {
		name                 string
		jobEventMessageFn    func(context.Context, *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error)
		isJobSetSubscribedFn func(context.Context, string, string) (bool, string, error)
		ttlSecs              int64
		wantErr              bool
		wantSubscriptionErr  bool
	}{
		{
			name:    "it exits with error after expiration even if messages are received",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error) {
				return MockEventClient{}, nil
			},
			isJobSetSubscribedFn: func(context.Context, string, string) (bool, string, error) {
				return true, "", nil
			},
			wantErr: true,
		},
		{
			name:    "it exits with error if client errors and sets subscription error",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error) {
				return MockEventClient{err: errors.New("some error")}, nil
			},
			isJobSetSubscribedFn: func(context.Context, string, string) (bool, string, error) {
				return true, "", nil
			},
			wantErr:             true,
			wantSubscriptionErr: true,
		},
		{
			name:    "it exits without error when job unsubscribes",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error) {
				return MockEventClient{}, nil
			},
			isJobSetSubscribedFn: func(context.Context, string, string) (bool, string, error) {
				return false, "", nil
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockJobEventReader := events.JobEventReaderMock{
				GetJobEventMessageFunc: tt.jobEventMessageFn,
				CloseFunc:              func() {},
			}

			mockJobRepo := repository.JobTableUpdaterMock{
				IsJobSetSubscribedFunc:                    tt.isJobSetSubscribedFn,
				SubscribeJobSetFunc:                       func(context.Context, string, string, string) error { return nil },
				AddMessageIdAndClearSubscriptionErrorFunc: func(context.Context, string, string, string) error { return nil },
				SetSubscriptionErrorFunc:                  func(context.Context, string, string, string, string) error { return nil },
				UnsubscribeJobSetFunc:                     func(context.Context, string, string) (int64, error) { return 0, nil },
			}

			service := eventstojobs.NewEventsToJobService(
				"somestring",
				"someJobSetId",
				&mockJobEventReader,
				&mockJobRepo,
			)
			result := service.SubscribeToJobSetId(context.Background(), tt.ttlSecs, "")
			if tt.wantErr {
				assert.Error(t, result)
			} else {
				assert.Nil(t, result)
			}
			if tt.wantSubscriptionErr {
				assert.True(t, len(mockJobRepo.SetSubscriptionErrorCalls()) > 0)
				assert.Equal(t, 0, len(mockJobRepo.AddMessageIdAndClearSubscriptionErrorCalls()))
			} else {
				assert.Equal(t, 0, len(mockJobRepo.SetSubscriptionErrorCalls()))
				assert.True(t, len(mockJobRepo.AddMessageIdAndClearSubscriptionErrorCalls()) > 0)
			}
		})
	}
}
