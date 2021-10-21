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

	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "pvc"}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!"}})
	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_getAction_WhenTwoEvents_AndTwoChecks_FirstCheckNotFirstEventWins(t *testing.T) {

	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionRetry, Regexp: "image"}, {Action: config.ActionFail, Regexp: "pvc"}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!"}, {Message: "Failed to pull image!"}})
	assert.Equal(t, ActionRetry, action)
	assert.NotEmpty(t, message)
}

func Test_newEventChecks_InvalidRegexp_ReturnsError(t *testing.T) {

	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "["}})
	assert.Nil(t, ec)
	assert.NotNil(t, err)
}
