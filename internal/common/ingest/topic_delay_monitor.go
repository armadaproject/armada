package ingest

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	pulsarutils "github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	log "github.com/armadaproject/armada/internal/common/logging"
)

const messageNotFound = "Message not found"
const publishTimeProperty = "publish-time"

type TopicProcessingDelayMonitor struct {
	pulsarAdminClient pulsaradmin.Client
	pulsarClient      pulsar.Client
	topic             string
	subscriptionName  string
	metrics           *commonmetrics.Metrics
}

func NewTopicProcessingDelayMonitor(
	client pulsar.Client, adminClient pulsaradmin.Client,
	topic string, subscriptionName string, metrics *commonmetrics.Metrics) *TopicProcessingDelayMonitor {
	return &TopicProcessingDelayMonitor{
		pulsarClient:      client,
		pulsarAdminClient: adminClient,
		topic:             topic,
		subscriptionName:  subscriptionName,
		metrics:           metrics,
	}
}

func (t *TopicProcessingDelayMonitor) Run(ctx *armadacontext.Context) error {
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

	wg := &sync.WaitGroup{}
	wg.Add(1)

	for _, topic := range topicNames {
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
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastestUnackedMessage, err := t.getLatestUnackedMessage(partition)
			if err != nil {
				log.Errorf("failed to get latest unacked message for partition %d - %s", partition.GetPartitionIndex(), err)
				continue
			}

			delayMs := float64(0)
			if lastestUnackedMessage != nil {
				publishTime, err := getPublishTimeFromMessage(*lastestUnackedMessage)
				if err != nil {
					log.Errorf("failed to get publish time for message %s - %s", lastestUnackedMessage.GetMessageID().String(), err)
					continue
				}

				delay := time.Now().UTC().Sub(publishTime.UTC())
				delayMs = math.Max(0, float64(delay.Milliseconds()))
			}

			t.metrics.RecordPulsarProcessingDelay(t.subscriptionName, partition.GetPartitionIndex(), delayMs)
		}
	}
}

func getPublishTimeFromMessage(message pulsarutils.Message) (time.Time, error) {
	publishTime := time.Time{}
	if ptStr, ok := message.Properties[publishTimeProperty]; ok {
		if t, err := time.Parse(time.RFC3339, ptStr); err == nil {
			publishTime = t
		} else {
			return time.Time{}, fmt.Errorf("failed to parse publish-time: %s", err)
		}
	} else {
		return time.Time{}, fmt.Errorf("no publish-time property present")
	}

	return publishTime, nil
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
