package eventwatcher

import (
	"github.com/pkg/errors"

	"github.com/G-Research/armada/pkg/api"
)

func assertEvent(expected *api.EventMessage, actual *api.EventMessage) error {
	switch e := actual.Events.(type) {
	case *api.EventMessage_Failed:
		v := expected.Events.(*api.EventMessage_Failed)
		return assertEventFailed(e, v)
	default:
		return nil
	}
}

func assertEventFailed(expected *api.EventMessage_Failed, actual *api.EventMessage_Failed) error {
	if len(expected.Failed.GetReason()) > 0 && expected.Failed.GetReason() != actual.Failed.GetReason() {
		return errors.Errorf(
			"error asserting failure reason: expected %s, got %s",
			expected.Failed.GetReason(), actual.Failed.GetReason(),
		)
	}
	return nil
}
