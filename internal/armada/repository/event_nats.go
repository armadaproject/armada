package repository

import (
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/eventstream"
	"github.com/G-Research/armada/pkg/api"
)

type StreamEventStore struct {
	stream eventstream.EventStream
}

func NewEventStore(stream eventstream.EventStream) *StreamEventStore {
	return &StreamEventStore{stream: stream}
}

func (n *StreamEventStore) ReportEvents(messages []*api.EventMessage) error {
	if len(messages) == 0 {
		return nil
	}
	errs := n.stream.Publish(messages)
	if len(errs) > 0 {
		return fmt.Errorf("errors when publishing events: %v", errs)
	}
	return nil
}

type RedisEventProcessor struct {
	stream     eventstream.EventStream
	repository EventStore
}

func NewEventRedisProcessor(stream eventstream.EventStream, repository EventStore) *RedisEventProcessor {
	return &RedisEventProcessor{stream: stream, repository: repository}
}

func (p *RedisEventProcessor) Start() {
	err := p.stream.Subscribe(p.handleMessage)

	if err != nil {
		panic(err)
	}
}

func (p *RedisEventProcessor) handleMessage(eventMessage *api.EventMessage) error {
	// TODO: batching???
	err := p.repository.ReportEvents([]*api.EventMessage{eventMessage})
	if err != nil {
		log.Errorf("error while reporting event in redis: %v", err)
		return err
	}
	return nil
}
