package eventstream

import (
	"fmt"
	"github.com/G-Research/armada/pkg/api"
	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
)

type JetstreamEventClient struct {
	subject      string
	queue        string
	consumerOpts []jsm.ConsumerOption
	conn         *nats.Conn
	manager      *jsm.Manager
	stream       *jsm.Stream
	consumer     *jsm.Consumer
}

func NewJetstreamClient(
	subject string,
	queue string,
	conn *nats.Conn,
	manager *jsm.Manager,
	stream *jsm.Stream,
	consumerOpts []jsm.ConsumerOption) (*JetstreamEventClient, error) {
	return &JetstreamEventClient{
		subject: 	  subject,
		queue:        queue,
		conn:         conn,
		manager:      manager,
		stream:       stream,
		consumerOpts: consumerOpts,
		consumer:     nil,
	}, nil
}

func (c *JetstreamEventClient) Publish(events []*api.EventMessage) []error {
	var errors []error
	for _, event := range events {
		data, err := proto.Marshal(event)
		if err != nil {
			errors = append(errors, fmt.Errorf("error while marshalling event: %v", err))
		}
		err = c.conn.Publish(c.subject, data)
		if err != nil {
			errors = append(errors, fmt.Errorf("error when publishing to subject %q: %v", c.subject, err))
		}
	}
	return errors
}

func (c *JetstreamEventClient) Subscribe(callback func(event *api.EventMessage) error) error {
	if c.consumer == nil {
		inbox := nats.NewInbox()

		opts := make([]jsm.ConsumerOption, len(c.consumerOpts))
		copy(opts, c.consumerOpts)
		opts = append(opts, jsm.DeliverySubject(inbox))
		opts = append(opts, jsm.DeliverGroup(c.queue))

		consumer, err := c.manager.NewConsumer(c.stream.Name(), opts...)
		if err != nil {
			return fmt.Errorf("error when creating consumer for subject %q: %v", c.subject, err)
		}
		c.consumer = consumer
	}

	_, err := c.conn.QueueSubscribe(c.consumer.DeliverySubject(), c.queue, func(msg *nats.Msg) {
		event := &api.EventMessage{}
		err := proto.Unmarshal(msg.Data, event)
		if err != nil {
			log.Errorf("failed to unmarsal event: %v", err)
		}
		err = callback(event)
		if err != nil {
			log.Errorf("queue subscribe callback error: %v", err)
		}
		err = msg.Ack()
		if err != nil {
			log.Errorf("error when acknowledging message: %v", err)
		}
	})
	if err != nil {
		return fmt.Errorf("error when trying to queue subscribe: %v", err)
	}

	return nil
}

func (c *JetstreamEventClient) Close() error {
	if c.conn != nil {
		c.conn.Close()
	}
	return nil
}
