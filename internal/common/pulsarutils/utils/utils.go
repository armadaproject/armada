package utils

import (
	"github.com/armadaproject/armada/internal/common/ingest/utils"
)

// PreProcessor applies any pre-processing to events before they're published
type PreProcessor[T utils.ArmadaEvent] func([]T) ([]T, error)

// KeyRetriever retrieves the pulsar message key given the event being published
type KeyRetriever[T utils.ArmadaEvent] func(T) string
