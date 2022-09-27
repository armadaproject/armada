package pulsartest

import (
	"context"
	"os"

	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Submit a job, represented by a file, to the Pulsar server.
// If dry-run is true, the job file is validated but not submitted.
func (a *App) Submit(path string, dryRun bool) error {
	eventYaml, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	es := &armadaevents.EventSequence{}

	if err = UnmarshalEventSubmission(eventYaml, es); err != nil {
		return err
	}

	if dryRun {
		return nil
	}

	log.Infof("submitting event sequence: %+v\n", es)

	// synchronously send request with event sequence
	payload, err := proto.Marshal(es)
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := context.Background()
	requestId := requestid.FromContextOrMissing(ctx)

	_, err = a.PS.Producer.Send(
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

	if err != nil {
		return err
	}

	err = a.PS.Producer.Flush()
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
