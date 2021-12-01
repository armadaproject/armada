package eventstream

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/pkg/api"
)

type JetstreamEventStream struct {
	subject      string
	consumerOpts []jsm.ConsumerOption
	conn         *nats.Conn
	manager      *jsm.Manager
	stream       *jsm.Stream
	consumers    []*jsm.Consumer

	mutex sync.RWMutex
}

func NewJetstreamEventStream(
	opts *configuration.JetstreamConfig,
	consumerOpts ...jsm.ConsumerOption) (*JetstreamEventStream, error) {
	natsConn, err := nats.Connect(strings.Join(opts.Servers, ","))
	if err != nil {
		return nil, err
	}
	manager, err := jsm.New(natsConn, jsm.WithTimeout(opts.ConnTimeout))
	if err != nil {
		return nil, err
	}

	streamOptions := []jsm.StreamOption{
		jsm.Subjects(opts.Subject),
		jsm.MaxAge(time.Duration(opts.MaxAgeDays) * 24 * time.Hour),
		jsm.Replicas(opts.Replicas),
	}
	if opts.InMemory {
		streamOptions = append(streamOptions, jsm.MemoryStorage())
	} else {
		streamOptions = append(streamOptions, jsm.FileStorage())
	}
	stream, err := manager.LoadOrNewStream(
		opts.StreamName,
		streamOptions...)
	if err != nil {
		return nil, err
	}

	return &JetstreamEventStream{
		subject:      opts.Subject,
		conn:         natsConn,
		manager:      manager,
		stream:       stream,
		consumerOpts: consumerOpts,
		consumers:    []*jsm.Consumer{},
	}, nil
}

func (c *JetstreamEventStream) Publish(events []*api.EventMessage) []error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var errs []error
	for _, event := range events {
		data, err := proto.Marshal(event)
		if err != nil {
			errs = append(errs, fmt.Errorf("error while marshalling event: %v", err))
		}
		err = c.conn.Publish(c.subject, data)
		if err != nil {
			errs = append(errs, fmt.Errorf("error when publishing to subject %q: %v", c.subject, err))
		}
	}
	return errs
}

func (c *JetstreamEventStream) Subscribe(queue string, callback func(event *Message) error) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	inbox := nats.NewInbox()

	opts := append(
		c.consumerOpts,
		jsm.FilterStreamBySubject(c.subject),
		jsm.DeliverySubject(inbox),
		jsm.DeliverGroup(queue))

	consumer, err := c.manager.NewConsumer(c.stream.Name(), opts...)
	if err != nil {
		return fmt.Errorf("error when creating consumer for subject %q: %v", c.subject, err)
	}
	c.consumers = append(c.consumers, consumer)

	_, err = c.conn.QueueSubscribe(consumer.DeliverySubject(), queue, func(msg *nats.Msg) {
		event := &api.EventMessage{}
		err := proto.Unmarshal(msg.Data, event)
		if err != nil {
			log.Errorf("failed to unmarsal event: %v", err)
		}

		ackFn := func() error {
			return msg.Ack()
		}
		err = callback(&Message{
			EventMessage: event,
			Ack:          ackFn,
		})
		if err != nil {
			log.Errorf("queue subscribe callback error: %v", err)
		}
		if err != nil {
			log.Errorf("error when acknowledging message: %v", err)
		}
	})

	if err != nil {
		return fmt.Errorf("error when trying to queue subscribe: %v", err)
	}
	return nil
}

func (c *JetstreamEventStream) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, consumer := range c.consumers {
		err := consumer.Delete()
		if err != nil {
			return err
		}
	}
	if c.conn != nil {
		c.conn.Close()
	}
	return nil
}

func (c *JetstreamEventStream) Check() error {
	if !c.conn.IsConnected() {
		return errors.New("not connected to NATS")
	}
	return nil
}
