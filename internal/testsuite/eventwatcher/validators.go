package eventwatcher

import (
	"github.com/pkg/errors"

	"github.com/G-Research/armada/pkg/api"
)

func validateEvent(actual *api.EventMessage, expected *api.EventMessage) error {
	switch actual.Events.(type) {
	case *api.EventMessage_Failed:
		return validateFailedEvent(actual, expected)
	default:
		return nil
	}
}

func validateFailedEvent(actual *api.EventMessage, expected *api.EventMessage) error {
	receivedFailedEvent, ok := actual.Events.(*api.EventMessage_Failed)
	if !ok {
		return errors.New("error casting actual event as Failed event")
	}
	expectedFailedEvent, ok := expected.Events.(*api.EventMessage_Failed)
	if !ok {
		return errors.New("error casting expected event as Failed event")
	}
	// only check messages if there is an expected message to check
	if len(expectedFailedEvent.Failed.GetReason()) > 0 {
		if expectedFailedEvent.Failed.GetReason() != receivedFailedEvent.Failed.GetReason() {
			return errors.Errorf(
				"error asserting failure reason: expected %s, got %s",
				expectedFailedEvent.Failed.GetReason(), receivedFailedEvent.Failed.GetReason(),
			)
		}
	}

	return nil
}
