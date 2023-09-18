package eventstojobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/pkg/api"
)

type MockEventClient struct {
	eventStreamMessage *api.EventStreamMessage
	messagesSent       int
	err                error
}

func (m *MockEventClient) Recv() (*api.EventStreamMessage, error) {
	msg := m.eventStreamMessage
	if msg == nil {
		msg = &api.EventStreamMessage{
			Id:      "msgID",
			Message: &api.EventMessage{},
		}
		m.eventStreamMessage = msg
		m.messagesSent = 0
	}

	m.messagesSent += 1

	if m.messagesSent > 3 {
		// Sleep a bit to mimick the stream being open but no events
		// currently available.
		time.Sleep(time.Second * 5)
		return nil, io.EOF
	}

	// We only want to return the error once.
	defer func() {
		m.err = nil
	}()

	return msg, m.err
}

func (m *MockEventClient) CloseSend() error {
	return nil
}

func (m *MockEventClient) Context() context.Context {
	return context.Background()
}

func (m *MockEventClient) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (m *MockEventClient) Trailer() metadata.MD {
	return metadata.MD{}
}

func (m *MockEventClient) SendMsg(msg interface{}) error {
	return nil
}

func (m *MockEventClient) RecvMsg(msg interface{}) error {
	return nil
}

func TestJobSetSubscriptionSubscribe(t *testing.T) {
	tests := []struct {
		name                 string
		isJobSetSubscribedFn func(context.Context, string, string) (bool, string, error)
		ttlSecs              time.Duration
		err                  error
		wantErr              bool
		wantSubscriptionErr  bool
	}{
		{
			name:    "no error after expiration if messages are received",
			ttlSecs: time.Second,
			err:     nil,
			isJobSetSubscribedFn: func(context.Context, string, string) (bool, string, error) {
				return true, "", nil
			},
			wantErr: false,
		},
		{
			name:    "client errors and sets subscription error, but can continue on and exit normally",
			ttlSecs: time.Second * 10,
			err:     errors.New("some error"),
			isJobSetSubscribedFn: func(context.Context, string, string) (bool, string, error) {
				return true, "", nil
			},
			wantErr:             false,
			wantSubscriptionErr: true,
		},
		{
			name:    "it exits without error when job unsubscribes",
			ttlSecs: time.Second,
			err:     nil,
			isJobSetSubscribedFn: func(context.Context, string, string) (bool, string, error) {
				return false, "", nil
			},
			wantErr: false,
		},
	}

	subDoneChan := make(chan *repository.JobSetKey, 5)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eventClient := MockEventClient{
				err: tt.err,
			}

			mockJobEventReader := events.JobEventReaderMock{
				GetJobEventMessageFunc: func(context.Context, *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error) {
					return &eventClient, nil
				},
				CloseFunc: func() {},
			}

			mockJobRepo := repository.SQLJobServiceMock{
				IsJobSetSubscribedFunc:                    tt.isJobSetSubscribedFn,
				SubscribeJobSetFunc:                       func(context.Context, string, string, string) error { return nil },
				AddMessageIdAndClearSubscriptionErrorFunc: func(context.Context, string, string, string) error { return nil },
				SetSubscriptionErrorFunc:                  func(context.Context, string, string, string, string) error { return nil },
				UnsubscribeJobSetFunc:                     func(context.Context, string, string) (int64, error) { return 0, nil },
				CheckToUnSubscribeFunc:                    func(context.Context, string, string, time.Duration) (bool, error) { return true, nil },
				GetSubscribedJobSetsFunc: func(context.Context) ([]repository.SubscribedTuple, error) {
					return make([]repository.SubscribedTuple, 0), nil
				},
			}

			subInfo := &repository.SubscribedTuple{
				JobSetKey: repository.JobSetKey{
					Queue:    "testQueue",
					JobSetId: "testID",
				},
			}

			sub := NewJobSetSubscription(
				context.Background(),
				&mockJobEventReader,
				subInfo,
				tt.ttlSecs,
				subDoneChan,
				&mockJobRepo,
			)

			result := sub.Subscribe()

			key := <-subDoneChan
			assert.Equal(t, key.Queue, "testQueue")
			assert.Equal(t, key.JobSetId, "testID")

			if tt.wantErr {
				assert.Error(t, result)
			} else {
				assert.Nil(t, result)
			}
			if tt.wantSubscriptionErr {
				assert.True(t, len(mockJobRepo.SetSubscriptionErrorCalls()) > 0)
				assert.Equal(t, 2, len(mockJobRepo.AddMessageIdAndClearSubscriptionErrorCalls()))
			} else {
				assert.Equal(t, 0, len(mockJobRepo.SetSubscriptionErrorCalls()))
				assert.True(t, len(mockJobRepo.AddMessageIdAndClearSubscriptionErrorCalls()) > 0)
			}
		})
	}
}

// Tests general function of the subscription executor.
func TestJobSetSubscriptionExecutor(t *testing.T) {
	eventClient := MockEventClient{
		err: nil,
	}

	mockJobEventReader := events.JobEventReaderMock{
		GetJobEventMessageFunc: func(context.Context, *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error) {
			return &eventClient, nil
		},
		CloseFunc: func() {},
	}

	ctx := context.Background()

	mockJobRepo := repository.SQLJobServiceMock{
		IsJobSetSubscribedFunc:                    func(context.Context, string, string) (bool, string, error) { return true, "", nil },
		SubscribeJobSetFunc:                       func(context.Context, string, string, string) error { return nil },
		AddMessageIdAndClearSubscriptionErrorFunc: func(context.Context, string, string, string) error { return nil },
		SetSubscriptionErrorFunc:                  func(context.Context, string, string, string, string) error { return nil },
		UnsubscribeJobSetFunc:                     func(context.Context, string, string) (int64, error) { return 0, nil },
		CheckToUnSubscribeFunc:                    func(context.Context, string, string, time.Duration) (bool, error) { return true, nil },
		GetSubscribedJobSetsFunc: func(context.Context) ([]repository.SubscribedTuple, error) {
			return make([]repository.SubscribedTuple, 0), nil
		},
	}

	jobSubChan := make(chan *repository.SubscribedTuple, 10)

	executor := NewJobSetSubscriptionExecutor(
		ctx,
		&mockJobEventReader,
		&mockJobRepo,
		jobSubChan,
		time.Duration(time.Second),
	)

	go executor.Manage()

	for i := 0; i < 5; i++ {
		jobSubChan <- &repository.SubscribedTuple{
			JobSetKey: repository.JobSetKey{
				Queue:    fmt.Sprintf("TestQueue-%d", i),
				JobSetId: fmt.Sprintf("TestJobSetId-%d", i),
			},
		}
	}

	// Wait for all subs to clear.
	sawSubs := false
	numberSeen := 0
	func() {
		watchDog := time.After(time.Second * 10)
		ticker := time.NewTicker(time.Millisecond * 200)
		for {
			select {
			case <-ticker.C:
				numSubs := executor.NumActiveSubscriptions()
				if numSubs == 0 {
					return
				} else if !sawSubs {
					sawSubs = true
					numberSeen = numSubs
				}
			case <-watchDog:
				assert.True(t, false, "Reached time out waiting for subscriptions to clear")
				return
			}
		}
	}()

	assert.True(t, sawSubs, "Never saw the executor handle any subscriptions")
	assert.Equal(t, numberSeen, 5, "Didn't see the expected amount of subscriptions")
}
