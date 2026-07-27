package reporter

import (
	"bytes"
	"unicode/utf8"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubectl/pkg/describe"
)

// maxDebugMessageSize caps the rendered debug message to prevent Pulsar message overflow.
const maxDebugMessageSize = 10 * 1024

// CreateDebugMessage renders the supplied Kubernetes pod events into a human-readable
// debug string, capped at maxDebugMessageSize (keeping the most recent events on truncation).
func CreateDebugMessage(podEvents []*v1.Event) string {
	events := make([]v1.Event, 0, len(podEvents))
	for _, e := range podEvents {
		events = append(events, *e)
	}

	eventList := v1.EventList{Items: events}
	writer := bytes.Buffer{}
	prefixWriter := describe.NewPrefixWriter(&writer)

	describe.DescribeEvents(&eventList, prefixWriter)

	message := writer.String()
	if len(message) > maxDebugMessageSize {
		tail := message[len(message)-maxDebugMessageSize:]
		// Slicing by byte offset can land mid-rune; advance to the next rune boundary so the
		// result is always valid UTF-8 (Postgres/JSON encoding and the UI reject invalid UTF-8).
		for len(tail) > 0 && !utf8.RuneStart(tail[0]) {
			tail = tail[1:]
		}
		message = "[truncated]..." + tail
	}
	return message
}
