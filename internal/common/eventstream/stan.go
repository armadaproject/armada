package eventstream

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/pkg/api"
)

type StanEventStream struct {
	subject             string
	stanClient          StanClient
	subscriptionOptions []stan.SubscriptionOption
}

func NewStanEventStream(subject string, stanClient StanClient, subscriptionOptions ...stan.SubscriptionOption) *StanEventStream {
	// as underlying NATS connection reconnects automatically, there is no need to renew it
	// keeping one NATS connection around will make message ack work better during STAN connection lost event
	return &StanEventStream{
		subject:             subject,
		stanClient:          stanClient,
		subscriptionOptions: subscriptionOptions,
	}
}

func (stream *StanEventStream) Publish(events []*api.EventMessage) []error {
	if len(events) == 0 {
		return nil
	}
	var errs []error

	wg := &sync.WaitGroup{}
	wg.Add(len(events))
	errorChan := make(chan error, len(events))
	for _, m := range events {
		messageData, err := proto.Marshal(m)
		if err != nil {
			errs = append(errs, fmt.Errorf("error while marshaling event: %v", err))
		}
		_, err = stream.stanClient.PublishAsync(stream.subject, messageData, func(subj string, err error) {
			if err != nil {
				errorChan <- fmt.Errorf("error while publishing event to queue: %v", err)
			} else {
				errorChan <- nil
			}
			wg.Done()
		})
		if err != nil {
			log.Errorf("error while sending event to queue: %v", err)
			errs = append(errs, err)
		}
	}

	err := waitTimeout(wg, 10*time.Second)
	if err != nil {
		errs = append(errs, err)
	}

	for i := 0; i < len(events); i++ {
		err, timeoutErr := getErrorChanTimeout(errorChan, 10*time.Second)
		if timeoutErr != nil {
			errs = append(errs, timeoutErr)
			break
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout when waiting for ack from stan on publish")
	}
}

func getErrorChanTimeout(channel chan error, timeout time.Duration) (error, error) {
	c := make(chan error)
	go func() {
		defer close(c)
		val := <-channel
		c <- val
	}()
	select {
	case val := <-c:
		return val, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout when getting ack from callback on publish")
	}
}

func (stream *StanEventStream) Subscribe(queue string, callback func(event *Message) error) error {
	opts := append(stream.subscriptionOptions, stan.DurableName(queue))
	return stream.stanClient.QueueSubscribe(
		stream.subject,
		queue,
		func(msg *stan.Msg) {
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
				log.Errorf("stan queue subscribe callback error: %v", err)
			}
			if err != nil {
				log.Errorf("stan error when acknowledging message: %v", err)
			}
		},
		opts...)
}

func (stream *StanEventStream) Close() error {
	return stream.stanClient.Close()
}

type StanClient interface {
	PublishAsync(subject string, data []byte, ah stan.AckHandler) (string, error)
	QueueSubscribe(subject, queue string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) error
	Close() error
}

type StanClientConnection struct {
	mutex sync.RWMutex

	options       []stan.Option
	clientID      string
	stanClusterID string

	subscriptions []func(conn stan.Conn) error

	currentConn stan.Conn
	nc          *nats.Conn
}

func NewStanClientConnection(
	stanClusterID, clientID string, servers []string,
	options ...stan.Option) (*StanClientConnection, error) {
	// as underlying NATS connection reconnects automatically, there is no need to renew it
	// keeping one NATS connection around will make message ack work better during STAN connection lost event
	nc, err := nats.Connect(
		strings.Join(servers, ","),
		nats.Name(clientID),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1))

	if err != nil {
		return nil, err
	}

	conn := &StanClientConnection{
		stanClusterID: stanClusterID,
		clientID:      clientID,
		nc:            nc,
	}
	conn.options = append(options, stan.SetConnectionLostHandler(conn.onConnectionLost), stan.NatsConn(nc))
	err = conn.reconnect()
	return conn, err
}

func (c *StanClientConnection) PublishAsync(subject string, data []byte, ah stan.AckHandler) (string, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.currentConn.PublishAsync(subject, data, ah)
}

func (c *StanClientConnection) QueueSubscribe(subject, qgroup string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	s := func(conn stan.Conn) error {
		_, err := conn.QueueSubscribe(subject, qgroup, cb, opts...)
		return err
	}
	c.subscriptions = append(c.subscriptions, s)

	return s(c.currentConn)
}

func (c *StanClientConnection) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.currentConn.Close()
	c.nc.Close()
	return err
}

func (c *StanClientConnection) Check() error {
	currentConn := c.currentConn
	if currentConn == nil {
		return errors.New("no NATS connection")
	}

	natsConn := currentConn.NatsConn()
	if natsConn == nil {
		return errors.New("no NATS connection")
	}

	if !natsConn.IsConnected() {
		return errors.New("not connected to NATS")
	}

	return nil
}

func (c *StanClientConnection) onConnectionLost(_ stan.Conn, e error) {
	// this callback is started in new go routine, it can take all the time needed
	for {
		err := c.reconnect()
		if err == nil {
			return
		}
		log.Errorf("Error while reconnecting to STAN: %v", err)
		time.Sleep(1 * time.Second)
	}
}

func (c *StanClientConnection) reconnect() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// close any previous connection, just in case it was still open
	if c.currentConn != nil {
		c.closeConnection()
	}

	// reconnect
	newConnection, err := stan.Connect(c.stanClusterID, c.clientID, c.options...)
	c.currentConn = newConnection
	if err != nil {
		log.Errorf("Error while connecting to STAN: %v", err)
		return err
	}

	// resubscribe
	for _, s := range c.subscriptions {
		err := s(c.currentConn)
		if err != nil {
			// on any subscription error consider connection unsuccessful
			log.Errorf("Error while resubscribing to STAN: %v", err)
			c.closeConnection()
			return err
		}
	}

	return nil
}

func (c *StanClientConnection) closeConnection() {
	err := c.currentConn.Close()
	if err != nil {
		log.Errorf("Error while closing STAN connection: %v", err)
	}
}
