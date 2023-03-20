package eventstojobs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/eventstojobs"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/pkg/api"
)

func Test_SubscribeToJobSetId(t *testing.T) {
	tests := []struct {
		name                 string
		jobEventMessageFn    func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error)
		isJobSetSubscribedFn func(string, string) (bool, string, error)
		ttlSecs              int64
		wantErr              bool
		wantSubscriptionErr  bool
	}{
		{
			name:    "it exits with error after expiration even if messages are received",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error) {
				return &api.EventStreamMessage{Message: &api.EventMessage{}}, nil
			},
			isJobSetSubscribedFn: func(string, string) (bool, string, error) {
				return true, "", nil
			},
			wantErr: true,
		},
		{
			name:    "it exits with error if client errors and sets subscription error",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error) {
				return &api.EventStreamMessage{Message: &api.EventMessage{}}, errors.New("some error")
			},
			isJobSetSubscribedFn: func(string, string) (bool, string, error) {
				return true, "", nil
			},
			wantErr:             true,
			wantSubscriptionErr: true,
		},
		{
			name:    "it exits without error when job unsubscribes",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error) {
				return &api.EventStreamMessage{Message: &api.EventMessage{}}, nil
			},
			isJobSetSubscribedFn: func(string, string) (bool, string, error) {
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
				SubscribeJobSetFunc:                       func(string, string, string) error { return nil },
				AddMessageIdAndClearSubscriptionErrorFunc: func(string, string, string) error { return nil },
				SetSubscriptionErrorFunc:                  func(string, string, string, string) error { return nil },
				UnsubscribeJobSetFunc:                     func(string, string) (int64, error) { return 0, nil },
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
				assert.Equal(t, 0, len(mockJobRepo.ClearSubscriptionErrorCalls()))
			} else {
				assert.Equal(t, 0, len(mockJobRepo.SetSubscriptionErrorCalls()))
				assert.True(t, len(mockJobRepo.ClearSubscriptionErrorCalls()) > 0)
			}
		})
	}
}
