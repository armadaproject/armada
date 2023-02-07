package pulsarutils

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type MockMessageId struct {
	pulsar.MessageID
	id int
}

type MockPulsarMessage struct {
	pulsar.Message
	messageId   pulsar.MessageID
	payload     []byte
	publishTime time.Time
	properties  map[string]string
}

func NewMessageId(id int) pulsar.MessageID {
	return MockMessageId{id: id}
}

func NewConsumerMessageId(id int) *ConsumerMessageId {
	return &ConsumerMessageId{
		MessageId:  MockMessageId{id: id},
		ConsumerId: id,
	}
}

func EmptyPulsarMessage(id int, publishTime time.Time) MockPulsarMessage {
	return MockPulsarMessage{
		messageId:   NewMessageId(id),
		publishTime: publishTime,
	}
}

func NewPulsarMessage(id int, publishTime time.Time, payload []byte) MockPulsarMessage {
	return MockPulsarMessage{
		messageId:   NewMessageId(id),
		publishTime: publishTime,
		payload:     payload,
	}
}

func (m MockPulsarMessage) ID() pulsar.MessageID {
	return m.messageId
}

func (m MockPulsarMessage) Payload() []byte {
	return m.payload
}

func (m MockPulsarMessage) PublishTime() time.Time {
	return m.publishTime
}

func (m MockPulsarMessage) Properties() map[string]string {
	return m.properties
}
