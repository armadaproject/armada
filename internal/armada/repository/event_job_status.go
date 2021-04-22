package repository

import (
	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/stan.go"
	stanPb "github.com/nats-io/stan.go/pb"
	log "github.com/sirupsen/logrus"

	stanUtil "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/pkg/api"
)

type NatsEventJobStatusProcessor struct {
	connection    *stanUtil.DurableConnection
	jobRepository JobRepository
	subject       string
	group         string
}

func NewNatsEventJobStatusProcessor(connection *stanUtil.DurableConnection, jobRepository JobRepository, subject string, group string) *NatsEventJobStatusProcessor {
	return &NatsEventJobStatusProcessor{connection: connection, jobRepository: jobRepository, subject: subject, group: group}
}

func (p *NatsEventJobStatusProcessor) Start() {
	err := p.connection.QueueSubscribe(p.subject, p.group,
		p.handleMessage,
		stan.SetManualAckMode(),
		stan.StartAt(stanPb.StartPosition_LastReceived),
		stan.DurableName(p.group))

	if err != nil {
		panic(err)
	}
}

func (p *NatsEventJobStatusProcessor) handleMessage(msg *stan.Msg) {
	// TODO: batching???
	event, err := unmarshal(msg)

	if err != nil {
		log.Errorf("Error while unmarshalling nats message: %v", err)
	} else {
		switch event := event.(type) {
		case *api.JobRunningEvent:
			err = p.jobRepository.UpdateStartTime(event.JobId, event.ClusterId, event.Created)
			if err != nil {
				log.Errorf("Error while updating job start time: %v", err)
				return
			}
		}
	}
	err = msg.Ack()
	if err != nil {
		log.Errorf("Error while ack nats message: %v", err)
	}
}

func unmarshal(msg *stan.Msg) (api.Event, error) {
	eventMessage := &api.EventMessage{}
	err := proto.Unmarshal(msg.Data, eventMessage)
	if err != nil {
		return nil, err
	}
	return api.UnwrapEvent(eventMessage)
}
