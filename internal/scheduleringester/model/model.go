package model

import "github.com/apache/pulsar-client-go/pulsar"

// DbOperationsWithMessageIds bundles a sequence of schedulerdb ops with the ids of all Pulsar
// messages that were consumed to produce it.
type DbOperationsWithMessageIds struct {
	Ops        []DbOperation
	MessageIds []pulsar.MessageID
}

func (d *DbOperationsWithMessageIds) GetMessageIDs() []pulsar.MessageID {
	return d.MessageIds
}
