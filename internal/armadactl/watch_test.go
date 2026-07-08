package armadactl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api"
)

func TestRequestorFromEvent(t *testing.T) {
	t.Run("returns requestor for preempting event", func(t *testing.T) {
		e := &api.JobPreemptingEvent{Requestor: "alice"}
		assert.Equal(t, "alice", requestorFromEvent(e))
	})

	t.Run("returns empty for events without requestor", func(t *testing.T) {
		e := &api.JobSucceededEvent{}
		assert.Equal(t, "", requestorFromEvent(e))
	})
}
