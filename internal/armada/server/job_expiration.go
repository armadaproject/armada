package server

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type PulsarJobExpirer struct {
	Consumer      pulsar.Consumer
	JobRepository repository.JobRepository
}

func (srv *PulsarJobExpirer) Run(ctx *armadacontext.Context) error {
	eventChan := srv.Consumer.Chan()
	for {
		select {
		case <-ctx.Done():
			log.Infof("Context expired, stopping job details expiration loop")
			return nil
		case msg, ok := <-eventChan:
			if !ok {
				log.Infof("Channel closing, stopping job details expiration loop")
				return nil
			}
			// Unmarshal and validate the message.
			sequence, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
			if err == nil {
				errExpiring := srv.handlePulsarSchedulerEventSequence(ctx, sequence)
				if errExpiring != nil {
					logging.WithStacktrace(ctx, err).Warnf("Could not expire PulsarJobDetails; ignoring")
				}
			} else {
				logging.WithStacktrace(ctx, err).Warnf("Could not unmarshall event sequenbce; ignoring")
			}
			srv.ack(ctx, msg)
		}
	}
}

func (srv *PulsarJobExpirer) handlePulsarSchedulerEventSequence(ctx *armadacontext.Context, sequence *armadaevents.EventSequence) error {
	idsOfJobsToExpireMappingFor := make([]string, 0)
	for _, event := range sequence.GetEvents() {
		var jobId string
		var err error
		switch e := event.Event.(type) {
		case *armadaevents.EventSequence_Event_JobSucceeded:
			jobId, err = armadaevents.UlidStringFromProtoUuid(e.JobSucceeded.JobId)
		case *armadaevents.EventSequence_Event_JobErrors:
			if ok := armadaslices.AnyFunc(e.JobErrors.Errors, func(e *armadaevents.Error) bool { return e.Terminal }); ok {
				jobId, err = armadaevents.UlidStringFromProtoUuid(e.JobErrors.JobId)
			}
		case *armadaevents.EventSequence_Event_CancelledJob:
			jobId, err = armadaevents.UlidStringFromProtoUuid(e.CancelledJob.JobId)
		default:
			// Non-terminal event
			continue
		}
		if err != nil {
			logging.WithStacktrace(ctx, err).Warnf("failed to determine jobId from event of type %T; ignoring", event.Event)
			continue
		}
		idsOfJobsToExpireMappingFor = append(idsOfJobsToExpireMappingFor, jobId)
	}
	return srv.JobRepository.ExpirePulsarSchedulerJobDetails(ctx, idsOfJobsToExpireMappingFor)
}

func (srv *PulsarJobExpirer) ack(ctx *armadacontext.Context, msg pulsar.Message) {
	util.RetryUntilSuccess(
		ctx,
		func() error {
			return srv.Consumer.Ack(msg)
		},
		func(err error) {
			log.WithError(err).Warnf("Error acking pulsar message")
			time.Sleep(time.Second)
		},
	)
}
