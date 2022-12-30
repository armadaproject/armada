package processor

import (
	"context"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/server"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/pkg/api"
)

type StreamEventStore struct {
	PulsarSubmitServer *server.PulsarSubmitServer
}

func NewEventStore() *StreamEventStore {
	return &StreamEventStore{}
}

func (n *StreamEventStore) ReportEvents(messages []*api.EventMessage) error {
	if len(messages) == 0 {
		return nil
	}

	// Publish to Pulsar if enabled.
	if n.PulsarSubmitServer != nil {
		logger := log.StandardLogger().WithField("service", "StreamEventStore")
		err := n.PulsarSubmitServer.SubmitApiEvents(context.Background(), messages)
		if err != nil {
			logging.WithStacktrace(logger, err).Error("failed to submit API event to Pulsar")
		}
	}

	return nil
}
