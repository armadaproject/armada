package stan_util

import (
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	log "github.com/sirupsen/logrus"
)

type DurableConnection struct {
	mutex sync.RWMutex

	options       []stan.Option
	clientID      string
	stanClusterID string

	subscriptions []func(conn stan.Conn) error

	currentConn stan.Conn
	nc          *nats.Conn
}

func DurableConnect(stanClusterID, clientID, urls string, options ...stan.Option) (*DurableConnection, error) {
	// as underlying NATS connection reconnects automatically, there is no need to renew it
	// keeping one NATS connection around will make message ack work better during STAN connection lost event
	nc, err := nats.Connect(urls,
		nats.Name(clientID),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1))

	if err != nil {
		return nil, err
	}

	conn := &DurableConnection{
		stanClusterID: stanClusterID,
		clientID:      clientID,
		nc:            nc,
	}
	conn.options = append(options, stan.SetConnectionLostHandler(conn.onConnectionLost), stan.NatsConn(nc))
	err = conn.reconnect()
	return conn, err
}

func (c *DurableConnection) PublishAsync(subject string, data []byte, ah stan.AckHandler) (string, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.currentConn.PublishAsync(subject, data, ah)
}

func (c *DurableConnection) QueueSubscribe(subject, qgroup string, cb stan.MsgHandler, opts ...stan.SubscriptionOption) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	s := func(conn stan.Conn) error {
		_, err := conn.QueueSubscribe(subject, qgroup, cb, opts...)
		return err
	}
	c.subscriptions = append(c.subscriptions, s)

	return s(c.currentConn)
}

func (c *DurableConnection) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	err := c.currentConn.Close()
	c.nc.Close()
	return err
}

func (c *DurableConnection) Check() error {
	currentConn := c.currentConn
	if currentConn == nil {
		return errors.New("No NATS connection")
	}

	natsConn := currentConn.NatsConn()
	if natsConn == nil {
		return errors.New("No NATS connection")
	}

	if !natsConn.IsConnected() {
		return errors.New("Not connected to NATS")
	}

	return nil
}

func (c *DurableConnection) onConnectionLost(_ stan.Conn, e error) {
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

func (c *DurableConnection) reconnect() error {
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

func (c *DurableConnection) closeConnection() {
	err := c.currentConn.Close()
	if err != nil {
		log.Errorf("Error while closing STAN connection: %v", err)
	}
}
