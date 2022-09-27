package pulsartest

import (
	"fmt"
	"io"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/server"
	"github.com/G-Research/armada/internal/pulsarutils"
)

type App struct {
	// Parameters passed to the CLI by the user.
	Params *Params
	// Out is used to write the output. Default to standard out,
	// but can be overridden in tests to make assertions on the applications's output.
	Out io.Writer

	// TODO this can be reduced to just a Pulsar Producer object, since we
	// won't be using the methods on this struct.
	PS *server.PulsarSubmitServer
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

	serverPulsarProducerName := fmt.Sprintf("armada-server-%s", serverId)
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Name:             serverPulsarProducerName,
		CompressionType:  compressionType,
		CompressionLevel: compressionLevel,
		Topic:            params.Pulsar.JobsetEventsTopic,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error creating pulsar producer %s", serverPulsarProducerName)
	}

	ps := &server.PulsarSubmitServer{
		Producer: producer,
	}

	app := &App{
		Params: &Params{},
		Out:    os.Stdout,
		PS:     ps,
	}
	return app, nil
}
