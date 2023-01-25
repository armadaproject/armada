package pulsartest

import (
	"context"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/requestid"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Submit a job, represented by a file, to the Pulsar server.
func (a *App) Submit(path string) error {
	eventYaml, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	es := &armadaevents.EventSequence{}

	if err = UnmarshalEventSubmission(eventYaml, es); err != nil {
		return err
	}

	log.Infof("submitting event sequence: %+v\n", es)

	// synchronously send request with event sequence
	payload, err := proto.Marshal(es)
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.Background()
	requestId := requestid.FromContextOrMissing(ctx)

	_, err = a.Producer.Send(
		ctx,
		&pulsar.ProducerMessage{
			Payload: payload,
			Properties: map[string]string{
				requestid.MetadataKey:                     requestId,
				armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
			},
			Key: es.JobSetName,
		},
	)

	return err
}
