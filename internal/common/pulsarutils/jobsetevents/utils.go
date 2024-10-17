package jobsetevents

import (
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pulsarutils/utils"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func NewPreProcessor(maxEventsPerMessage int, maxAllowedMessageSize uint) utils.PreProcessor[*armadaevents.EventSequence] {
	return func(events []*armadaevents.EventSequence) ([]*armadaevents.EventSequence, error) {
		sequences := eventutil.CompactEventSequences(events)
		sequences = eventutil.LimitSequencesEventMessageCount(sequences, maxEventsPerMessage)
		return eventutil.LimitSequencesByteSize(sequences, maxAllowedMessageSize, true)
	}
}

func RetrieveKey(event *armadaevents.EventSequence) string {
	return event.JobSetName
}
