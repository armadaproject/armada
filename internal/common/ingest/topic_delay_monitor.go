package ingest

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	pulsarutils "github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/pulsarclient"
	log "github.com/armadaproject/armada/internal/common/logging"
)

const (
	messageNotFound     = "Message not found"
	publishTimeProperty = "publish-time"
)

type TopicProcessingDelayMonitor struct {
	pulsarAdminClient pulsarclient.Client
	pulsarClient      pulsar.Client
	topic             string
	subscriptionName  string
	metrics           *commonmetrics.Metrics
	interval          time.Duration
	partitions        []*pulsarutils.TopicName
}

func NewTopicProcessingDelayMonitor(
	client pulsar.Client, adminClient pulsarclient.Client,
	topic string, subscriptionName string, interval time.Duration, metrics *commonmetrics.Metrics,
) *TopicProcessingDelayMonitor {
	return &TopicProcessingDelayMonitor{
		pulsarClient:      client,
		pulsarAdminClient: adminClient,
		topic:             topic,
		subscriptionName:  subscriptionName,
		metrics:           metrics,
		interval:          interval,
		partitions:        make([]*pulsarutils.TopicName, 0),
	}
}

func (t *TopicProcessingDelayMonitor) Initialise(ctx *armadacontext.Context) error {
	partitions, err := t.pulsarClient.TopicPartitions(t.topic)
	if err != nil {
		return fmt.Errorf("failed to get number of partitions for topic %s - %s", t.topic, err)
	}

	topicNames := make([]*pulsarutils.TopicName, 0, len(partitions))
	for _, partition := range partitions {
		topicName, err := pulsarutils.GetTopicName(partition)
		if err != nil {
			return fmt.Errorf("failed to create topic name for partition %s - %s", partition, err)
		}
		topicNames = append(topicNames, topicName)
	}

	t.partitions = topicNames
	return nil
}

func (t *TopicProcessingDelayMonitor) Run(ctx *armadacontext.Context) error {
	if len(t.partitions) == 0 {
		err := t.Initialise(ctx)
		if err != nil {
			return err
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	for _, topic := range t.partitions {
		go func(topic *pulsarutils.TopicName) {
			wg.Add(1)
			t.monitorPartitionDelay(ctx, topic)
			wg.Done()
		}(topic)
	}

	wg.Done()
	wg.Wait()
	return nil
}

func (t *TopicProcessingDelayMonitor) monitorPartitionDelay(ctx *armadacontext.Context, partition *pulsarutils.TopicName) {
	// Sleep for 0 -> 10s, so all the routines don't start and hit the pulsar api at once
	jitter := time.Duration(rand.Int63n(int64(time.Second * 10)))
	time.Sleep(jitter)

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	var lastKnownResultTime time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// If no message, default to 0 delay
			// This is common and occurs when the topic is empty (the ingester is keeping up)
			delayMs := float64(0)
			lastestUnackedMessage, err := t.getLatestUnackedMessage(partition)
			if err != nil {
				log.Errorf("failed to get latest unacked message for partition %d - %s", partition.GetPartitionIndex(), err)
				delay := time.Now().UTC().Sub(lastKnownResultTime.UTC())
				delayMs = math.Max(0, float64(delay.Milliseconds()))
			} else if lastestUnackedMessage != nil {
				publishTime, err := getPublishTimeFromMessage(*lastestUnackedMessage)
				if err != nil {
					log.Errorf("failed to get publish time for message %s - %s", lastestUnackedMessage.GetMessageID().String(), err)
					delay := time.Now().UTC().Sub(lastKnownResultTime.UTC())
					delayMs = math.Max(0, float64(delay.Milliseconds()))
				} else {
					delay := time.Now().UTC().Sub(publishTime.UTC())
					delayMs = math.Max(0, float64(delay.Milliseconds()))
					lastKnownResultTime = publishTime.UTC()
				}
			} else {
				lastKnownResultTime = time.Now().UTC()
			}

			t.metrics.RecordPulsarProcessingDelay(t.subscriptionName, partition.GetPartitionIndex(), delayMs)
		}
	}
}

func getPublishTimeFromMessage(message pulsarutils.Message) (time.Time, error) {
	if ptStr, ok := message.Properties[publishTimeProperty]; ok {
		if t, err := time.Parse(time.RFC3339, ptStr); err == nil {
			return t, nil
		} else {
			return time.Time{}, fmt.Errorf("failed to parse publish-time: %s", err)
		}
	} else {
		return time.Time{}, fmt.Errorf("no publish-time property present")
	}
}

func (t *TopicProcessingDelayMonitor) getLatestUnackedMessage(partition *pulsarutils.TopicName) (*pulsarutils.Message, error) {
	messages, err := t.pulsarAdminClient.Subscriptions().PeekMessages(*partition, t.subscriptionName, 1)
	if err != nil {
		var restErr rest.Error
		if errors.As(err, &restErr) {
			if restErr.Code == 404 && restErr.Reason == messageNotFound {
				// No unacked messages, return
				return nil, nil
			}
		}

		return nil, fmt.Errorf("failed to peek messages - %s", err)
	}

	if len(messages) != 0 {
		return messages[0], nil
	}
	return nil, nil
}
