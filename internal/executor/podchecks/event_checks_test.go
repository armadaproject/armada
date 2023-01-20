package podchecks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"

	config "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

func Test_getAction_WhenNoEvents_AndNoChecks_ReturnsWait(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{}, time.Minute)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneEvent_WithMatchingFailCheck_ReturnsFail(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "pvc", Type: "Warning", GracePeriod: time.Minute}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Warning"}}, time.Minute*2)
	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_getAction_WhenOneEvent_MatchFailsDueToRegexp_ReturnsWait(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "something else", Type: "Warning", GracePeriod: time.Minute}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Warning"}}, time.Minute*2)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneEvent_MatchFailsDueToType_ReturnsWait(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "pvc", Type: "Warning", GracePeriod: time.Minute}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Normal"}}, time.Minute*2)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenOneEvent_MatchFailsDueToGracePeriod_ReturnsWait(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{{Action: config.ActionFail, Regexp: "pvc", Type: "Warning", GracePeriod: time.Minute}})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to mount pvc!", Type: "Warning"}}, time.Minute/2)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenTwoEvents_AndTwoChecks_MostDrasticActionWins(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{Action: config.ActionRetry, Regexp: "image", Type: "Warning", GracePeriod: time.Minute},
		{Action: config.ActionFail, Regexp: "pvc", Type: "Warning", GracePeriod: time.Minute},
	})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{
		{Message: "Failed to mount pvc!", Type: "Warning"},
		{Message: "Failed to pull image!", Type: "Warning"},
	}, time.Minute*2)

	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_getAction_WhenEventMatchesTwoRules_AndFirstRuleStillInGracePeriod_ButSecondIsNotInGracePeriod_IgnoresBothRules(t *testing.T) {
	// When a rule matches an event, but doesn't fire as still in grace period, we don't match later rules.
	ec, err := newEventChecks([]config.EventCheck{
		{Regexp: "image", Type: "Warning", GracePeriod: time.Minute * 2, Action: config.ActionRetry},
		{Regexp: ".*", Type: "Warning", GracePeriod: time.Second, Action: config.ActionFail},
	})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Failed to pull image", Type: "Warning"}}, time.Minute)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)
}

func Test_getAction_WhenTwoRules_AndFirstRuleRegexDoesNotMatch_AppliesSecondRule(t *testing.T) {
	ec, err := newEventChecks([]config.EventCheck{
		{Regexp: "image", Type: "Warning", GracePeriod: time.Minute, Action: config.ActionRetry},
		{Regexp: ".*", Type: "Warning", GracePeriod: time.Minute, Action: config.ActionFail},
	})
	assert.Nil(t, err)

	action, message := ec.getAction("my-pod", []*v1.Event{{Message: "Secret missing", Type: "Warning"}}, time.Minute*2)
	assert.Equal(t, ActionFail, action)
	assert.NotEmpty(t, message)
}

func Test_getAction_WhenOneEvent_NegativeRegex_Works(t *testing.T) {
	ec, err := newEventChecks(
		[]config.EventCheck{{Action: config.ActionRetry, Regexp: "nodes are available", Type: "Warning", Inverse: true, GracePeriod: time.Minute}},
	)
	assert.Nil(t, err)

	action, message := ec.getAction(
		"my-pod",
		[]*v1.Event{
			{
				Message: "0/3 nodes are available: 1 node(s) had taint {node-role.kubernetes.io/master: }, that the pod didn't tolerate, 2 Insufficient cpu.",
				Type:    "Warning",
			},
		},
		time.Minute*2,
	)
	assert.Equal(t, ActionWait, action)
	assert.Empty(t, message)

	action, message = ec.getAction("my-pod", []*v1.Event{{Message: "Some other error message", Type: "Warning"}}, time.Minute*2)
	assert.Equal(t, ActionRetry, action)
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
