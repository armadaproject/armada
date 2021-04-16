package repository

import (
	"github.com/gogo/protobuf/proto"
	stan "github.com/nats-io/stan.go"
	stanPb "github.com/nats-io/stan.go/pb"
	log "github.com/sirupsen/logrus"

	stanUtil "github.com/G-Research/armada/internal/common/stan-util"
	"github.com/G-Research/armada/pkg/api"
)

type NatsEventStore struct {
	connection *stanUtil.DurableConnection
	subject    string
}

func NewNatsEventStore(connection *stanUtil.DurableConnection, subject string) *NatsEventStore {
	return &NatsEventStore{connection: connection, subject: subject}
}

func (n *NatsEventStore) ReportEvents(messages []*api.EventMessage) error {
	if len(messages) == 0 {
		return nil
	}
	errors := make(chan error, len(messages))
	for _, m := range messages {
		messageData, e := proto.Marshal(m)
		if e != nil {
			log.Errorf("Error while marshaling event: %v", e)
			return e
		}
		_, e = n.connection.PublishAsync(n.subject, messageData, func(subj string, err error) {
			if err != nil {
				log.Errorf("Error while publishing event to queue: %v", err)
			}
			errors <- err
		})
		if e != nil {
			log.Errorf("Error while sending event to queue: %v", e)
			return e
		}
	}

	waiting := len(messages)
	var lastError error
	select {
	case e := <-errors:
		waiting--
		if e != nil {
			lastError = e
		}
		if waiting == 0 {
			return lastError
		}
	}
	return nil
}

type NatsEventRedisProcessor struct {
	connection *stanUtil.DurableConnection
	repository EventStore
	subject    string
	group      string
}

func NewNatsEventRedisProcessor(connection *stanUtil.DurableConnection, repository EventStore, subject string, group string) *NatsEventRedisProcessor {
	return &NatsEventRedisProcessor{connection: connection, repository: repository, subject: subject, group: group}
}

func (p *NatsEventRedisProcessor) Start() {
	err := p.connection.QueueSubscribe(p.subject, p.group,
		p.handleMessage,
		stan.SetManualAckMode(),
		stan.StartAt(stanPb.StartPosition_LastReceived),
		stan.DurableName(p.group))

	if err != nil {
		panic(err)
	}
}

func (p *NatsEventRedisProcessor) handleMessage(msg *stan.Msg) {
	// TODO: batching???
	eventMessage := &api.EventMessage{}
	err := proto.Unmarshal(msg.Data, eventMessage)
	if err != nil {
		log.Errorf("Error while unmarshaling nats message: %v", err)
	} else {
		err := p.repository.ReportEvents([]*api.EventMessage{eventMessage})
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
