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
	repository repository.JobRepository
}

func NewEventProcessor(connection *stanUtil.DurableConnection, repository repository.JobRepository, subject string, group string) *EventProcessor {
	return &EventProcessor{connection: connection, repository: repository, subject: subject, group: group}
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
			log.Errorf("Error while reporting event from nats: %v", err)
			return
		}
	}
	err = msg.Ack()
	if err != nil {
		log.Errorf("Error while ack nats message: %v", err)
	}
}

func (p *EventProcessor) processEvent(event api.Event) error {
	//switch typed := event.(type) {
	//case *api.JobSubmittedEvent:
	//
	//}
	return nil
}
