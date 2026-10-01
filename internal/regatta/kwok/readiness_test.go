package kwok

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/pkg/api"
)

// fakeEventClient serves a fixed sequence of events from GetJobSetEvents, then blocks until the
// request's context ends (like a real watch with nothing new to deliver) or, if endWithEOF, ends
// the stream.
type fakeEventClient struct {
	api.EventClient
	events     []*api.EventMessage
	endWithEOF bool
}

func (f *fakeEventClient) GetJobSetEvents(ctx context.Context, _ *api.JobSetRequest, _ ...grpc.CallOption) (api.Event_GetJobSetEventsClient, error) {
	return &fakeEventStream{ctx: ctx, events: f.events, endWithEOF: f.endWithEOF}, nil
}

type fakeEventStream struct {
	grpc.ClientStream
	ctx        context.Context
	events     []*api.EventMessage
	endWithEOF bool
}

func (s *fakeEventStream) Recv() (*api.EventStreamMessage, error) {
	if len(s.events) == 0 {
		if s.endWithEOF {
			return nil, io.EOF
		}
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
	msg := s.events[0]
	s.events = s.events[1:]
	return &api.EventStreamMessage{Message: msg}, nil
}

func queued(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Queued{Queued: &api.JobQueuedEvent{JobId: jobId}}}
}

func leased(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Leased{Leased: &api.JobLeasedEvent{JobId: jobId}}}
}

func running(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Running{Running: &api.JobRunningEvent{JobId: jobId}}}
}

func succeeded(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Succeeded{Succeeded: &api.JobSucceededEvent{JobId: jobId}}}
}

func failed(jobId, reason string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Failed{Failed: &api.JobFailedEvent{JobId: jobId, Reason: reason}}}
}

func cancelled(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Cancelled{Cancelled: &api.JobCancelledEvent{JobId: jobId}}}
}

func TestAwaitCanaryRunning(t *testing.T) {
	tests := map[string]struct {
		events      []*api.EventMessage
		timeout     time.Duration
		wantRunning bool
		wantFailure string
		wantLast    string
	}{
		"running after queued and leased": {
			events:      []*api.EventMessage{queued("job"), leased("job"), running("job")},
			timeout:     time.Second,
			wantRunning: true,
			wantLast:    "JobRunningEvent",
		},
		"succeeded counts as having run": {
			events:      []*api.EventMessage{queued("job"), succeeded("job")},
			timeout:     time.Second,
			wantRunning: true,
			wantLast:    "JobSucceededEvent",
		},
		"other jobs' events in the same job set are ignored": {
			events:      []*api.EventMessage{running("other"), queued("job"), failed("other", "boom"), running("job")},
			timeout:     time.Second,
			wantRunning: true,
			wantLast:    "JobRunningEvent",
		},
		"still queued at the timeout": {
			events:   []*api.EventMessage{queued("job")},
			timeout:  50 * time.Millisecond,
			wantLast: "JobQueuedEvent",
		},
		"leased but not yet running at the timeout": {
			events:   []*api.EventMessage{queued("job"), leased("job")},
			timeout:  50 * time.Millisecond,
			wantLast: "JobLeasedEvent",
		},
		"no events at the timeout": {
			timeout: 50 * time.Millisecond,
		},
		"failed is terminal": {
			events:      []*api.EventMessage{queued("job"), failed("job", "image pull")},
			timeout:     time.Second,
			wantFailure: "failed: image pull",
			wantLast:    "JobFailedEvent",
		},
		"cancelled is terminal": {
			events:      []*api.EventMessage{queued("job"), cancelled("job")},
			timeout:     time.Second,
			wantFailure: "was cancelled",
			wantLast:    "JobCancelledEvent",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			outcome, err := awaitCanaryRunning(context.Background(), &fakeEventClient{events: tc.events}, "jobset", "job", tc.timeout)
			require.NoError(t, err)
			require.Equal(t, tc.wantRunning, outcome.running)
			require.Equal(t, tc.wantFailure, outcome.failure)
			require.Equal(t, tc.wantLast, outcome.last)
		})
	}
}

func TestAwaitCanaryRunning_ParentContextCancelledIsNotAnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outcome, err := awaitCanaryRunning(ctx, &fakeEventClient{events: []*api.EventMessage{queued("job")}}, "jobset", "job", time.Minute)
	// The cancelled context may end the wait before or after the queued event is read.
	require.NoError(t, err)
	require.False(t, outcome.running)
}

func TestAwaitCanaryRunning_StreamErrorBeforeTimeoutIsReturned(t *testing.T) {
	_, err := awaitCanaryRunning(context.Background(), &fakeEventClient{events: []*api.EventMessage{queued("job")}, endWithEOF: true}, "jobset", "job", time.Second)
	require.Error(t, err)
	require.True(t, errors.Is(err, io.EOF))
}

func TestCanaryJobSpec_NodeSelector(t *testing.T) {
	annotationOnly := canaryJobSpec("target-a", false).PodSpec.NodeSelector
	require.Equal(t, map[string]string{NodeAnnotation: NodeAnnotationOK}, annotationOnly)

	selectsTarget := canaryJobSpec("target-a", true).PodSpec.NodeSelector
	require.Equal(t, map[string]string{NodeAnnotation: NodeAnnotationOK, TargetLabel: "target-a"}, selectsTarget)
}
