package pulsarrequestid

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/armadaproject/armada/internal/common/requestid"
)

// FromMessage attempts to extract a request id embedded in a Pulsar message.
// The second return value is true if the operation was successful.
func FromMessage(msg pulsar.Message) (string, bool) {
	properties := msg.Properties()
	if properties == nil {
		return "", false
	}
	id, ok := properties[requestid.MetadataKey]
	return id, ok
}

// FromMessageOrMissing attempts to extract a request id embedded in a Pulsar message,
// or "missing" if none could be found.
func FromMessageOrMissing(msg pulsar.Message) (requestId string) {
	requestId = "missing"
	properties := msg.Properties()
	if properties == nil {
		return
	}
	if id, ok := properties[requestid.MetadataKey]; ok {
		requestId = id
	}
	return
}

// AddToMessage adds the provided request id in-place to the provided Pulsar message.
func AddToMessage(msg *pulsar.ProducerMessage, id string) {
	if msg.Properties == nil {
		msg.Properties = make(map[string]string)
	}
	msg.Properties[requestid.MetadataKey] = id
	return
}
