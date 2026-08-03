package pulsarutils

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

// MessageIdsToStrings converts Pulsar message ids to their string representation,
// suitable for embedding in JSON payloads or message properties.
func MessageIdsToStrings(ids []pulsar.MessageID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}
