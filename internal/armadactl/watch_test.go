package armadactl

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
	tests := []struct {
		name  string
		event *api.JobFailedEvent
		want  string
	}{
		{
			name:  "a failed run with a pod gives a command for that pod",
			event: &api.JobFailedEvent{ClusterId: "cluster-1", PodNamespace: "ns", PodName: "armada-run-1"},
			want:  "kubectl --context cluster-1 -n ns logs armada-run-1",
		},
		{
			name:  "a failed run without a pod gives no command",
			event: &api.JobFailedEvent{ClusterId: "cluster-1", PodNamespace: "ns"},
			want:  "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, kubectlLogsHint(tc.event))
		})
	}
}
