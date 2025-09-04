package ingest

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	pulsarutils "github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/pkg/errors"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/ingest/pulsarclient"
	log "github.com/armadaproject/armada/internal/common/logging"
)

type PartitionInfoFetcher interface {
	TopicPartitions(topic string) ([]string, error)
}

type PulsarDelayRecorder interface {
	RecordPulsarProcessingDelay(subscriptionName string, partition int, delayInMs float64)
}

const (
	messageNotFound     = "Message not found"
	publishTimeProperty = "publish-time"
)

type TopicProcessingDelayMonitor struct {
	pulsarAdminClient    pulsarclient.Client
	partitionInfoFetcher PartitionInfoFetcher
	topic                string
	subscriptionName     string
	metrics              PulsarDelayRecorder
	interval             time.Duration
	partitions           []*pulsarutils.TopicName
	clock                clock.WithTicker
	jitterFunc           func() time.Duration
}

func NewTopicProcessingDelayMonitor(
	partitionInfoFetcher PartitionInfoFetcher, adminClient pulsarclient.Client,
	topic string, subscriptionName string, interval time.Duration, metrics PulsarDelayRecorder,
) *TopicProcessingDelayMonitor {
	return &TopicProcessingDelayMonitor{
		partitionInfoFetcher: partitionInfoFetcher,
		pulsarAdminClient:    adminClient,
		topic:                topic,
		subscriptionName:     subscriptionName,
		metrics:              metrics,
		interval:             interval,
		partitions:           make([]*pulsarutils.TopicName, 0),
		clock:                clock.RealClock{},
		jitterFunc: func() time.Duration {
			return time.Duration(rand.Int63n(int64(time.Second * 10)))
		},
	}
}

func (t *TopicProcessingDelayMonitor) Initialise(ctx *armadacontext.Context) error {
	partitions, err := t.partitionInfoFetcher.TopicPartitions(t.topic)
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
		wg.Add(1)
		go func(topic *pulsarutils.TopicName) {
			t.monitorPartitionDelay(ctx, topic)
			wg.Done()
		}(topic)
	}

	wg.Done()
	wg.Wait()
	return nil
}

func (t *TopicProcessingDelayMonitor) monitorPartitionDelay(ctx *armadacontext.Context, partition *pulsarutils.TopicName) {
	// Sleep for some jitter, so all the routines don't start and hit the pulsar api at once
	time.Sleep(t.jitterFunc())

	ticker := t.clock.NewTicker(t.interval)
	defer ticker.Stop()

	lastKnownResultTime := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			delayMs := float64(0)

			publishTimeOfLatestMessage, err := t.getPublishTimeOfLatestMessageOnPartition(partition)
			if err != nil {
				if lastKnownResultTime.IsZero() {
					log.WithError(err).Errorf("failed to get delay info for partition (%s/%d), no fallback to use", t.subscriptionName, partition.GetPartitionIndex())
					continue
				}
				log.WithError(err).Errorf("failed to get delay info for partition (%s/%d), using fall back value", t.subscriptionName, partition.GetPartitionIndex())
				// On error, fall back to the last know delay of the partition
				// This is a conservative estimate of how far the ingester is behind,
				//  it acts as an upper bound as we know there are no messages with a publish time older than the fallback time
				publishTimeOfLatestMessage = &lastKnownResultTime
			}

			if publishTimeOfLatestMessage != nil {
				delay := t.clock.Now().UTC().Sub(publishTimeOfLatestMessage.UTC())
				delayMs = math.Max(0, float64(delay.Milliseconds()))
				// On successful calculation, update lastKnownResultTime, so we can fallback to the last time we knew the delay of the partition
				lastKnownResultTime = publishTimeOfLatestMessage.UTC()
			} else {
				// This happens when there are no messages, this is common and occurs when the topic is empty (the ingester is keeping up)
				//
				// Update lastKnownResultTime, so we can fallback to the last time we knew the delay of the partition
				lastKnownResultTime = t.clock.Now().UTC()
			}

			t.metrics.RecordPulsarProcessingDelay(t.subscriptionName, partition.GetPartitionIndex(), delayMs)
		}
	}
}

// This function will return the publish time of the latest unacked message on the provided partition
// If there are no messages on the partition, it will return a nil value (and no errors)
func (t *TopicProcessingDelayMonitor) getPublishTimeOfLatestMessageOnPartition(partition *pulsarutils.TopicName) (*time.Time, error) {
	lastestUnackedMessage, err := t.getLatestUnackedMessage(partition)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest unacked message for partition - %s", err)
	}

	if lastestUnackedMessage == nil {
		// No unprocessed messages
		return nil, nil
	}

	publishTime, err := getPublishTimeFromMessage(*lastestUnackedMessage)
	if err != nil {
		return nil, fmt.Errorf("failed to get publish time for message %s - %s", lastestUnackedMessage.GetMessageID().String(), err)
	}

	return &publishTime, nil
}

func getPublishTimeFromMessage(message pulsarutils.Message) (time.Time, error) {
	if ptStr, ok := message.Properties[publishTimeProperty]; ok {
		if t, err := time.Parse(time.RFC3339Nano, ptStr); err == nil {
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
