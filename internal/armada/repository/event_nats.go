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
	repository EventStore
	stream     eventstream.EventStream
	batcher    eventstream.EventBatcher
}

func NewEventRedisProcessor(
	queue string,
	repository EventStore,
	stream eventstream.EventStream,
	batcher eventstream.EventBatcher,
) *RedisEventProcessor {
	processor := &RedisEventProcessor{
		queue:      queue,
		repository: repository,
		stream:     stream,
		batcher:    batcher,
	}
	processor.batcher.Register(processor.handleBatch)
	return processor
}

func (p *RedisEventProcessor) Start() {
	err := p.stream.Subscribe(p.queue, p.handleMessage)

	if err != nil {
		panic(err)
	}
}

func (p *RedisEventProcessor) handleMessage(message *eventstream.Message) error {
	err := p.batcher.Report(message)
	if err != nil {
		log.Errorf("error while reporting event in redis: %v", err)
		return err
	}
	return nil
}

func (p *RedisEventProcessor) handleBatch(batch []*eventstream.Message) error {
	events := make([]*api.EventMessage, len(batch), len(batch))
	for i, msg := range batch {
		events[i] = msg.EventMessage
	}

	err := p.repository.ReportEvents(events)
	if err != nil {
		return fmt.Errorf("error while reporting events to event store for %d events: %v", len(events), err)
	}

	for _, msg := range batch {
		err = msg.Ack()
		if err != nil {
			log.Errorf("error while acknowledging event: %v", err)
		}
	}
	return nil
}
