package eventstojobs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/jobservice/events"
	"github.com/G-Research/armada/internal/jobservice/eventstojobs"
	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
)

func Test_SubscribeToJobSetId(t *testing.T) {
	tests := []struct {
		name                 string
		jobEventMessageFn    func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error)
		isJobSetSubscribedFn func(string, string) bool
		ttlSecs              int64
		wantErr              bool
	}{
		{
			name:    "it exits with error after expiration even if messages are received",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error) {
				return &api.EventStreamMessage{Message: &api.EventMessage{}}, nil
			},
			isJobSetSubscribedFn: func(string, string) bool {
				return true
			},
			wantErr: true,
		},
		{
			name:    "it exits with error if client errors",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error) {
				return &api.EventStreamMessage{Message: &api.EventMessage{}}, errors.New("some error")
			},
			isJobSetSubscribedFn: func(string, string) bool {
				return true
			},
			wantErr: true,
		},
		{
			name:    "it exits without error when job unsubscribes",
			ttlSecs: int64(1),
			jobEventMessageFn: func(context.Context, *api.JobSetRequest) (*api.EventStreamMessage, error) {
				return &api.EventStreamMessage{Message: &api.EventMessage{}}, nil
			},
			isJobSetSubscribedFn: func(string, string) bool {
				return false
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
				IsJobSetSubscribedFunc: tt.isJobSetSubscribedFn,
				SubscribeJobSetFunc:    func(string, string) {},
			}

			service := eventstojobs.NewEventsToJobService(
				"somestring",
				"someJobSetId",
				"someJobId",
				&mockJobEventReader,
				&mockJobRepo,
			)
			result := service.SubscribeToJobSetId(context.Background(), tt.ttlSecs)
			if tt.wantErr {
				assert.Error(t, result)
			} else {
				assert.Nil(t, result)
			}
		})
	}
}
