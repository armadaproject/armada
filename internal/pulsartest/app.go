package pulsartest

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/pulsarutils"
)

type App struct {
	Producer pulsar.Producer
}

type Params struct {
	Pulsar configuration.PulsarConfig
}

func New(params Params) (*App, error) {
	serverId := uuid.New()

	compressionType, err := pulsarutils.ParsePulsarCompressionType(params.Pulsar.CompressionType)
	if err != nil {
		return nil, err
	}
	compressionLevel, err := pulsarutils.ParsePulsarCompressionLevel(params.Pulsar.CompressionLevel)
	if err != nil {
		return nil, err
	}

	pulsarClient, err := pulsarutils.NewPulsarClient(&params.Pulsar)
	if err != nil {
		return nil, err
	}

	producerName := fmt.Sprintf("pulsartest-%s", serverId)
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Name:             producerName,
		CompressionType:  compressionType,
		CompressionLevel: compressionLevel,
		Topic:            params.Pulsar.JobsetEventsTopic,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error creating pulsar producer %s", producerName)
	}

	app := &App{
		Producer: producer,
	}
	return app, nil
}
