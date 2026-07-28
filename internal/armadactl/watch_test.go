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
	for name, event := range map[string]api.Event{
		"preempting event":     &api.JobPreemptingEvent{Requestor: "alice"},
		"cancelling event":     &api.JobCancellingEvent{Requestor: "alice"},
		"cancelled event":      &api.JobCancelledEvent{Requestor: "alice"},
		"reprioritizing event": &api.JobReprioritizingEvent{Requestor: "alice"},
		"reprioritized event":  &api.JobReprioritizedEvent{Requestor: "alice"},
	} {
		t.Run("returns requestor for "+name, func(t *testing.T) {
			assert.Equal(t, "alice", requestorFromEvent(event))
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
