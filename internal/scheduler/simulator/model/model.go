package model

import (
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type StateTransition struct {
	Jobs          []*jobdb.Job
	EventSequence *armadaevents.EventSequence
}
