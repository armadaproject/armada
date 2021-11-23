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
	queue      string
	stream     eventstream.EventStream
	batcher    eventstream.EventBatcher
}

func NewEventRedisProcessor(
	queue string,
	repository EventStore,
	stream eventstream.EventStream,
	batcher eventstream.EventBatcher,
) *RedisEventProcessor {
	batcher.Register(repository.ReportEvents)
	return &RedisEventProcessor{
		queue:   queue,
		stream:  stream,
		batcher: batcher,
	}
}

func (p *RedisEventProcessor) Start() {
	err := p.stream.Subscribe(p.queue, p.handleMessage)

	if err != nil {
		panic(err)
	}
}

func (p *RedisEventProcessor) handleMessage(eventMessage *api.EventMessage) error {
	err := p.batcher.Report(eventMessage)
	if err != nil {
		log.Errorf("error while reporting event in redis: %v", err)
		return err
	}
	return nil
}
