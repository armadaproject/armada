package reporter

import (
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type EventMessage struct {
	Event    *armadaevents.EventSequence
	JobRunId string
}
