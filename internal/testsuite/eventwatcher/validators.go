package eventwatcher

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/api"
)

func assertEvent(expected *api.EventMessage, actual *api.EventMessage) error {
	switch e := expected.Events.(type) {
	case *api.EventMessage_Failed:
		v := actual.Events.(*api.EventMessage_Failed)
		return assertEventFailed(e, v)
	default:
		return nil
	}
}

func assertEventFailed(expected *api.EventMessage_Failed, actual *api.EventMessage_Failed) error {
	if expected.Failed.GetReason() == "" {
		return nil
	}
	if actual == nil {
		return errors.Errorf("unexpected nil event 'actual'")
	}
	if expected.Failed.GetReason() != actual.Failed.GetReason() {
		return errors.Errorf(
			"error asserting failure reason: expected %s, got %s",
			expected.Failed.GetReason(), actual.Failed.GetReason(),
		)
	}
	return nil
}
