package reporter

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCreateDebugMessage(t *testing.T) {
	largeMessage := ""
	for i := 0; i < 500; i++ {
		largeMessage += "Long message that will cause truncation. "
	}

	tests := []struct {
		name             string
		events           []*v1.Event
		expectTruncation bool
	}{
		{
			name: "small message passes through",
			events: []*v1.Event{{
				ObjectMeta:     metav1.ObjectMeta{Name: "event1", Namespace: "default"},
				InvolvedObject: v1.ObjectReference{Kind: "Pod", Name: "test-pod", Namespace: "default"},
				Reason:         "FailedScheduling",
				Message:        "Small message",
				Type:           "Warning",
			}},
			expectTruncation: false,
		},
		{
			name: "large message gets truncated",
			events: []*v1.Event{{
				ObjectMeta:     metav1.ObjectMeta{Name: "event1", Namespace: "default"},
				InvolvedObject: v1.ObjectReference{Kind: "Pod", Name: "test-pod", Namespace: "default"},
				Reason:         "FailedScheduling",
				Message:        largeMessage,
				Type:           "Warning",
			}},
			expectTruncation: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateDebugMessage(tt.events)

			if tt.expectTruncation {
				assert.LessOrEqual(t, len(result), maxDebugMessageSize+len("[truncated]..."))
				assert.Contains(t, result, "[truncated]")
				assert.True(t, strings.HasPrefix(strings.TrimSpace(result), "[truncated]"))
			} else {
				assert.NotContains(t, result, "[truncated]")
				assert.Contains(t, result, tt.events[0].Message)
			}
		})
	}
}

func TestCreateDebugMessage_TruncationProducesValidUtf8(t *testing.T) {
	// '€' is 3 bytes. Vary the message length by a byte at a time so the byte-offset truncation
	// point sweeps across rune boundaries - at least one offset lands mid-rune, exercising the
	// rune-boundary correction. A naive byte slice yields invalid UTF-8 for several of these.
	for pad := 0; pad < 6; pad++ {
		largeMessage := strings.Repeat("€", maxDebugMessageSize) + strings.Repeat("a", pad)

		result := CreateDebugMessage([]*v1.Event{{
			ObjectMeta:     metav1.ObjectMeta{Name: "event1", Namespace: "default"},
			InvolvedObject: v1.ObjectReference{Kind: "Pod", Name: "test-pod", Namespace: "default"},
			Reason:         "FailedScheduling",
			Message:        largeMessage,
			Type:           "Warning",
		}})

		assert.Contains(t, result, "[truncated]")
		assert.Truef(t, utf8.ValidString(result), "truncated debug message must be valid UTF-8 (pad=%d)", pad)
	}
}
