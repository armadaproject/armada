package armadaevents

import "github.com/apache/pulsar-client-go/pulsar"

// PULSAR_MESSAGE_TYPE_PROPERTY is the key of a property included with Pulsar messages.
// It's value indicates if the message is a control message or a utilisation message.
//
// We need this because, we currently submit legacy utilisation messages directly to the log
// (i.e., not as part of an EventSequence).
//
// Hence, consumers can distinguish between control and utilisation messages by inspecting this property.
// If the property isn't included, the message should be interpreted as a control message.
const PULSAR_MESSAGE_TYPE_PROPERTY = "type"

const (
	PULSAR_UTILISATION_MESSAGE = "utilisation"
	PULSAR_CONTROL_MESSAGE     = "control"
)

// IsControlMessage returns true if msg is a control message, and false otherwise.
func IsControlMessage(msg pulsar.Message) bool {
	if messageType, ok := msg.Properties()[PULSAR_MESSAGE_TYPE_PROPERTY]; ok {
		return messageType == PULSAR_CONTROL_MESSAGE
	}
	return true
}
