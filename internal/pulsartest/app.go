package pulsartest

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
)

type App struct {
	Producer pulsar.Producer
	Reader   pulsar.Reader
}

type Params struct {
	Pulsar configuration.PulsarConfig
}

func New(params Params, cmdType string) (*App, error) {
	serverId := uuid.New()

	pulsarClient, err := pulsarutils.NewPulsarClient(&params.Pulsar)
	if err != nil {
		return nil, err
	}

	var producer pulsar.Producer
	var reader pulsar.Reader

	if cmdType == "submit" {
		producerName := fmt.Sprintf("pulsartest-%s", serverId)
		producer, err = pulsarClient.CreateProducer(pulsar.ProducerOptions{
			Name:  producerName,
			Topic: params.Pulsar.JobsetEventsTopic,
		})

		if err != nil {
			return nil, errors.Wrapf(err, "error creating pulsar producer %s", producerName)
		}

	} else if cmdType == "watch" {
		reader, err = pulsarClient.CreateReader(pulsar.ReaderOptions{
			Topic:          params.Pulsar.JobsetEventsTopic,
			StartMessageID: pulsar.EarliestMessageID(),
		})

		if err != nil {
			return nil, errors.Wrapf(err, "error creating pulsar reader")
		}

	} else {
		return nil, errors.New("cmdType must be either 'submit' or 'watch'")
	}

	app := &App{
		Producer: producer,
		Reader:   reader,
	}
	return app, nil
}
