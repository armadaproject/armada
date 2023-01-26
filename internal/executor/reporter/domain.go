package reporter

import "github.com/armadaproject/armada/pkg/api"

type EventMessage struct {
	Event    api.Event
	JobRunId string
}
