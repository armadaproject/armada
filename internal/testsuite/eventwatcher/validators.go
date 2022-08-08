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
	if expectedFailedEvent.Failed.Reason != receivedFailedEvent.Failed.Reason {
		return errors.Errorf(
			"error asserting failure reason: expected %s, got %s",
			expectedFailedEvent.Failed.Reason, receivedFailedEvent.Failed.Reason,
		)
	}

	return nil
}
