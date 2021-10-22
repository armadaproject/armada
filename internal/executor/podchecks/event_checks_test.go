package podchecks

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"

	config "github.com/G-Research/armada/internal/executor/configuration/podchecks"
)

func Test_getAction_WhenNoEvents_AndNoChecks_ReturnsWait(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{})
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneEvent_AndOneMatchingFailCheck_ReturnsFail(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "pvc", Type: "Warning"}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Warning"}})
	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_getAction_WhenOneEvent_MatchFailsDueToRegexp_ReturnsWait(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "something else", Type: "Warning"}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Warning"}})
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneEvent_MatchFailsDueToType_ReturnsWait(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "pvc", Type: "Warning"}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Normal"}})
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenTwoEvents_AndTwoChecks_MostDrasticActionWins(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionRetry, Regexp: "image", Type: "Warning"}, {Action: config.ActionFail, Regexp: "pvc", Type: "Warning"}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Warning"}, {Message: "Failed to pull image!", Type: "Warning"}})
	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_newEventChecks_InvalidRegexp_ReturnsError(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "[", Type: config.EventTypeNormal}})
	assert.Nil(t, ec)
	assert.NotNil(t, err)
}

func Test_newEventChecks_InvalidType_ReturnsError(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: ".*", Type: "wrong"}})
	assert.Nil(t, ec)
	assert.NotNil(t, err)
}

func Test_newEventChecks_InvalidAction_ReturnsError(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: "wrong", Regexp: ".*", Type: config.EventTypeNormal}})
	assert.Nil(t, ec)
	assert.NotNil(t, err)
}
