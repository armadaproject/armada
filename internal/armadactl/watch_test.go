package armadactl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/domain"
)

func TestRequestorFromEvent(t *testing.T) {
	requestor := "alice"
	for name, event := range map[string]api.Event{
		"preempting event":     &api.JobPreemptingEvent{Requestor: requestor},
		"cancelling event":     &api.JobCancellingEvent{Requestor: requestor},
		"cancelled event":      &api.JobCancelledEvent{Requestor: requestor},
		"reprioritizing event": &api.JobReprioritizingEvent{Requestor: requestor},
		"reprioritized event":  &api.JobReprioritizedEvent{Requestor: requestor},
	} {
		t.Run("returns requestor for "+name, func(t *testing.T) {
			assert.Equal(t, requestor, requestorFromEvent(event))
		})
	}

	t.Run("returns empty for events without requestor", func(t *testing.T) {
		e := &api.JobSucceededEvent{}
		assert.Equal(t, "", requestorFromEvent(e))
	})
}

func TestPrintSummaryShowsRequestorAsUser(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &App{Out: buf}
	event := &api.JobPreemptingEvent{
		JobId:     "job-1",
		Created:   protoutil.ToTimestamp(time.Unix(0, 0)),
		Requestor: "alice",
	}

	app.printSummary(domain.NewWatchContext(), event)

	assert.True(t, strings.Contains(buf.String(), "user: alice"), "expected output to contain user label, got %q", buf.String())
	assert.False(t, strings.Contains(buf.String(), "actor: alice"), "expected output not to contain old actor label, got %q", buf.String())
}

func TestKubectlLogsHint(t *testing.T) {
	const jobId = "01hcq4x7d2k9m3n5p8r0s1t2v4"
	knownJob := &domain.JobInfo{ClusterId: "cluster-1", Job: &api.Job{Namespace: "ns"}}
	tests := []struct {
		name    string
		event   *api.JobFailedEvent
		jobInfo *domain.JobInfo
		want    string
	}{
		{
			name:  "a failed event with a pod name gives a command for that pod",
			event: &api.JobFailedEvent{JobId: jobId, ClusterId: "cluster-1", PodNamespace: "ns", PodName: "armada-run-1"},
			want:  "kubectl --context cluster-1 -n ns logs armada-run-1",
		},
		{
			name:    "a failed event without a pod name gives a command for the job-scoped pod name",
			event:   &api.JobFailedEvent{JobId: jobId},
			jobInfo: knownJob,
			want:    "kubectl --context cluster-1 -n ns logs armada-" + jobId + "-0",
		},
		{
			name:  "a failed event without a pod name for a job that the watch does not know gives no command",
			event: &api.JobFailedEvent{JobId: jobId},
			want:  "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, kubectlLogsHint(tc.event, tc.jobInfo))
		})
	}
}

// fakeEventClient serves the events of a single job set. A request that does not watch gets the history only. A
// request that watches gets the history followed by the live events, after which the stream ends and the live events
// become part of the history. The events in recordedAfterFirstRequest become part of the history once the first
// request has been served.
type fakeEventClient struct {
	api.EventClient
	history                   []*api.EventMessage
	live                      []*api.EventMessage
	recordedAfterFirstRequest []*api.EventMessage
	requests                  []*api.JobSetRequest
}

func (c *fakeEventClient) GetJobSetEvents(_ context.Context, in *api.JobSetRequest, _ ...grpc.CallOption) (api.Event_GetJobSetEventsClient, error) {
	c.requests = append(c.requests, in)
	messages := append([]*api.EventMessage{}, c.history...)
	if in.Watch {
		messages = append(messages, c.live...)
		c.history = append(c.history, c.live...)
		c.live = nil
	}
	if len(c.requests) == 1 {
		c.history = append(c.history, c.recordedAfterFirstRequest...)
	}
	return &fakeEventStream{messages: messages}, nil
}

type fakeEventStream struct {
	grpc.ClientStream
	messages []*api.EventMessage
	next     int
}

func (s *fakeEventStream) Recv() (*api.EventStreamMessage, error) {
	if s.next >= len(s.messages) {
		return nil, io.EOF
	}
	message := s.messages[s.next]
	s.next++
	return &api.EventStreamMessage{Id: fmt.Sprint(s.next), Message: message}, nil
}

func submitted(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Submitted{Submitted: &api.JobSubmittedEvent{JobId: jobId, Job: &api.Job{Id: jobId}, Created: protoutil.ToTimestamp(time.Unix(0, 0))}}}
}

func running(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Running{Running: &api.JobRunningEvent{JobId: jobId, Created: protoutil.ToTimestamp(time.Unix(0, 0))}}}
}

func succeeded(jobId string) *api.EventMessage {
	return &api.EventMessage{Events: &api.EventMessage_Succeeded{Succeeded: &api.JobSucceededEvent{JobId: jobId, Created: protoutil.ToTimestamp(time.Unix(0, 0))}}}
}

func printedJobEvents(output string) []string {
	var events []string
	for _, line := range strings.Split(output, "\n") {
		if i := strings.Index(line, "Event, job id: "); i >= 0 {
			events = append(events, strings.Split(line[strings.LastIndex(line[:i], " ")+1:], " pod:")[0])
		}
	}
	return events
}

func TestWatch_ExitOnInactive(t *testing.T) {
	tests := map[string]struct {
		history          []*api.EventMessage
		live             []*api.EventMessage
		recordedLater    []*api.EventMessage
		expectedEvents   []string
		expectedRequests []bool // the Watch field of each request
	}{
		"exits at the end of the history if the job set is inactive": {
			history:          []*api.EventMessage{submitted("1"), running("1"), succeeded("1")},
			live:             []*api.EventMessage{submitted("2")},
			expectedEvents:   []string{"JobSubmittedEvent, job id: 1", "JobRunningEvent, job id: 1", "JobSucceededEvent, job id: 1"},
			expectedRequests: []bool{false},
		},
		"does not exit at a moment of inactivity in the history if the job set is active now": {
			history: []*api.EventMessage{submitted("1"), succeeded("1"), submitted("2"), running("2")},
			expectedEvents: []string{
				"JobSubmittedEvent, job id: 1", "JobSucceededEvent, job id: 1",
				"JobSubmittedEvent, job id: 2", "JobRunningEvent, job id: 2",
			},
			expectedRequests: []bool{false, true},
		},
		"exits when the job set becomes inactive after an active history": {
			history: []*api.EventMessage{submitted("1"), succeeded("1"), submitted("2"), running("2")},
			live:    []*api.EventMessage{succeeded("2")},
			expectedEvents: []string{
				"JobSubmittedEvent, job id: 1", "JobSucceededEvent, job id: 1",
				"JobSubmittedEvent, job id: 2", "JobRunningEvent, job id: 2",
				"JobSucceededEvent, job id: 2",
			},
			expectedRequests: []bool{false, true, false},
		},
		"does not exit at a moment of inactivity that was recorded after the history was read": {
			history:       []*api.EventMessage{submitted("1"), running("1")},
			recordedLater: []*api.EventMessage{succeeded("1"), submitted("2"), running("2")},
			expectedEvents: []string{
				"JobSubmittedEvent, job id: 1", "JobRunningEvent, job id: 1",
				"JobSucceededEvent, job id: 1", "JobSubmittedEvent, job id: 2", "JobRunningEvent, job id: 2",
			},
			expectedRequests: []bool{false, true, false, true},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			app := &App{Out: buf}
			fake := &fakeEventClient{history: tc.history, live: tc.live, recordedAfterFirstRequest: tc.recordedLater}

			err := app.watch(fake, "queue", "job-set", false, true, false, false)
			assert.NoError(t, err)

			assert.Equal(t, tc.expectedEvents, printedJobEvents(buf.String()))
			var requests []bool
			for _, r := range fake.requests {
				requests = append(requests, r.Watch)
			}
			assert.Equal(t, tc.expectedRequests, requests)
		})
	}
}

func TestWatch_WithoutExitOnInactiveWatchesTheWholeJobSet(t *testing.T) {
	buf := new(bytes.Buffer)
	app := &App{Out: buf}
	fake := &fakeEventClient{
		history: []*api.EventMessage{submitted("1"), succeeded("1")},
		live:    []*api.EventMessage{submitted("2")},
	}

	assert.NoError(t, app.watch(fake, "queue", "job-set", false, false, false, false))

	assert.Equal(t,
		[]string{"JobSubmittedEvent, job id: 1", "JobSucceededEvent, job id: 1", "JobSubmittedEvent, job id: 2"},
		printedJobEvents(buf.String()))
	assert.Len(t, fake.requests, 1)
	assert.True(t, fake.requests[0].Watch)
}
