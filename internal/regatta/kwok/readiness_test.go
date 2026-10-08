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
			outcome, err := awaitCanaryRunning(context.Background(), &fakeEventClient{events: tc.events}, "queue", "jobset", "job", tc.timeout)
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
	outcome, err := awaitCanaryRunning(ctx, &fakeEventClient{events: []*api.EventMessage{queued("job")}}, "queue", "jobset", "job", time.Minute)
	// The cancelled context may end the wait before or after the queued event is read.
	require.NoError(t, err)
	require.False(t, outcome.running)
}

func TestAwaitCanaryRunning_StreamErrorBeforeTimeoutIsReturned(t *testing.T) {
	_, err := awaitCanaryRunning(context.Background(), &fakeEventClient{events: []*api.EventMessage{queued("job")}, endWithEOF: true}, "queue", "jobset", "job", time.Second)
	require.Error(t, err)
	require.True(t, errors.Is(err, io.EOF))
}

func TestCanaryJobSpec_NodeSelector(t *testing.T) {
	annotationOnly := canaryJobSpec("target-a", false).PodSpec.NodeSelector
	require.Equal(t, map[string]string{NodeAnnotation: NodeAnnotationOK}, annotationOnly)

	selectsTarget := canaryJobSpec("target-a", true).PodSpec.NodeSelector
	require.Equal(t, map[string]string{NodeAnnotation: NodeAnnotationOK, TargetLabel: "target-a"}, selectsTarget)
}

func TestWaitRemaining(t *testing.T) {
	t.Run("waits out the rest of the budget after an early finish", func(t *testing.T) {
		start := time.Now()
		require.NoError(t, waitRemaining(context.Background(), start, 60*time.Millisecond))
		require.GreaterOrEqual(t, time.Since(start), 60*time.Millisecond)
	})

	t.Run("returns at once when the budget is already spent", func(t *testing.T) {
		begin := time.Now()
		require.NoError(t, waitRemaining(context.Background(), begin.Add(-time.Minute), 10*time.Millisecond))
		require.Less(t, time.Since(begin), 50*time.Millisecond)
	})

	t.Run("stops early when the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		begin := time.Now()
		require.ErrorIs(t, waitRemaining(ctx, begin, time.Minute), context.Canceled)
		require.Less(t, time.Since(begin), time.Second)
	})
}

const sampleJobReport = `default:
	Most recent scheduling round that affected job 01m3ypyf3j8h63b7z1x69t6nmg:
		Time:                       2026-10-02 11:25:22.46128 -0500 CDT m=+906.826707460
		Job ID:                     01m3ypyf3j8h63b7z1x69t6nmg
		UnschedulableReason:        none
		Node:                       cpu-cluster-kwok-node-cpu-only-143
		Number of nodes in cluster: 494
		Excluded nodes:
		 295: node does not match pod NodeSelector: required label armadaproject.io/regatta-target = cpu-cluster, but node has gpu-cluster
		 2: node does not match pod NodeSelector: label kwok.x-k8s.io/node not set
`

func TestParseSchedulerReport(t *testing.T) {
	view := parseSchedulerReport(sampleJobReport)
	require.Equal(t, "494", view.nodes)
	require.Equal(t, []string{
		"295: node does not match pod NodeSelector: required label armadaproject.io/regatta-target = cpu-cluster, but node has gpu-cluster",
		"2: node does not match pod NodeSelector: label kwok.x-k8s.io/node not set",
	}, view.excluded)

	require.Equal(t, schedulerView{}, parseSchedulerReport(""), "no report")
	none := parseSchedulerReport("default:\n\tMost recent scheduling round that affected job x: none\n")
	require.Equal(t, schedulerView{}, none, "a job the scheduler has not seen reports none")
	require.Empty(t, parseSchedulerReport("Number of nodes in cluster: 3\nExcluded nodes: none\n").excluded)
}

func TestTimedOutFailure(t *testing.T) {
	view := parseSchedulerReport(sampleJobReport)
	f := timedOutFailure("01m3", 5*time.Second, "JobLeasedEvent", view)

	require.Equal(t, "canary job 01m3, Number of nodes in cluster: 494", f.brief, "the per-attempt line stays short")
	require.Contains(t, f.detail, "not running within 5s, last event JobLeasedEvent")
	require.Contains(t, f.detail, "Number of nodes in cluster: 494")
	require.Contains(t, f.detail, "295: node does not match pod NodeSelector", "the final error keeps the exclusion reasons")

	noReport := timedOutFailure("01m3", 5*time.Second, "JobQueuedEvent", schedulerView{})
	require.Equal(t, "canary job 01m3, last event JobQueuedEvent", noReport.brief, "falls back to the last event without a report")
	require.Equal(t, "canary job 01m3 not running within 5s, last event JobQueuedEvent", noReport.detail)
}

func TestWithTimeout_AnUnreachableServerCannotBlockTheCallForever(t *testing.T) {
	start := time.Now()
	err := withTimeout(50*time.Millisecond, func(ctx context.Context) error {
		<-ctx.Done() // a call that waits for a server that never answers
		return ctx.Err()
	})

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestWithTimeout_ReturnsTheCallsOwnResult(t *testing.T) {
	sentinel := errors.New("denied")
	require.Equal(t, sentinel, withTimeout(time.Minute, func(context.Context) error { return sentinel }))
	require.NoError(t, withTimeout(time.Minute, func(context.Context) error { return nil }))
}
