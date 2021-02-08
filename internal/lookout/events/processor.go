package events

import (
	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/stan.go"
	stanPb "github.com/nats-io/stan.go/pb"
	log "github.com/sirupsen/logrus"

	stanUtil "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/pkg/api"
)

type EventProcessor struct {
	connection *stanUtil.DurableConnection
	subject    string
	group      string
	recorder   repository.JobRecorder
}

func NewEventProcessor(connection *stanUtil.DurableConnection, repository repository.JobRecorder, subject string, group string) *EventProcessor {
	return &EventProcessor{connection: connection, recorder: repository, subject: subject, group: group}
}

func (p *EventProcessor) Start() {
	err := p.connection.QueueSubscribe(p.subject, p.group,
		p.handleMessage,
		stan.SetManualAckMode(),
		stan.StartAt(stanPb.StartPosition_LastReceived),
		stan.DurableName(p.group))

	if err != nil {
		panic(err)
	}
}

func (p *EventProcessor) handleMessage(msg *stan.Msg) {
	// TODO: batching???
	eventMessage := &api.EventMessage{}
	err := proto.Unmarshal(msg.Data, eventMessage)
	if err != nil {
		log.Errorf("Error while unmarshaling nats message: %v", err)
	} else {
		event, err := api.UnwrapEvent(eventMessage)
		if err != nil {
			log.Errorf("Error while unwrapping event message: %v", err)
			return
		}
		err = p.processEvent(event)
		if err != nil {
			log.Errorf("Error while reporting event from nats: %v (event: %v)", err, eventMessage)
			return
		}
	}
	err = msg.Ack()
	if err != nil {
		log.Errorf("Error while ack nats message: %v", err)
	}
}

func (p *EventProcessor) processEvent(event api.Event) error {
	switch typed := event.(type) {
	case *api.JobSubmittedEvent:
		return p.recorder.RecordJob(&typed.Job)

	case *api.JobQueuedEvent:
		// this event just attest saving job to redis

	case *api.JobPendingEvent:
		return p.recorder.RecordJobPending(typed)

	case *api.JobRunningEvent:
		return p.recorder.RecordJobRunning(typed)

	case *api.JobSucceededEvent:
		return p.recorder.RecordJobSucceeded(typed)

	case *api.JobFailedEvent:
		return p.recorder.RecordJobFailed(typed)

	case *api.JobLeasedEvent:
	case *api.JobLeaseReturnedEvent:
	case *api.JobLeaseExpiredEvent:
		// TODO record leasing as messages?

	case *api.JobUnableToScheduleEvent:
		return p.recorder.RecordJobUnableToSchedule(typed)

	case *api.JobReprioritizedEvent:
		return p.recorder.RecordJobPriorityChange(typed)

	case *api.JobCancellingEvent: // noop
	case *api.JobCancelledEvent:
		// job marked for cancellation
		return p.recorder.MarkCancelled(typed)

	case *api.JobTerminatedEvent:
		// not used currently

	case *api.JobUtilisationEvent:
		// TODO
	}

	return nil
}
