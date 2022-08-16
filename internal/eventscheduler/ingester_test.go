package eventscheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/severinson/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

// Pulsar configuration. Must be manually reconciled with changes to the test setup or Armada.
const pulsarUrl = "pulsar://localhost:6650"
const pulsarTopic = "persistent://armada/armada/events"
const pulsarSubscription = "e2e-test"
const defaultPulsarTimeout = 30 * time.Second

const numConsumers = 2

func withPulsar(ctx context.Context, options pulsar.ClientOptions, action func(context.Context, pulsar.Client) error) error {
	pulsarClient, err := pulsar.NewClient(options)
	if err != nil {
		return errors.WithStack(err)
	}
	defer pulsarClient.Close()

	return action(ctx, pulsarClient)
}

// func main() {

// 	// Create a KeyShared consumer.
// 	client, _ := pulsar.NewClient(pulsar.ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})
// 	consumer, _ := client.Subscribe(pulsar.ConsumerOptions{
// 		Topic:            "my-topic",
// 		SubscriptionName: "my-subscription",
// 		Type:             pulsar.KeyShared,
// 	})

// 	// Seek a specific partition of a partitioned topic.
// 	partitionIdx := 0 // Assume that this consumer is responsible for the 0-th partition.
// 	messageIdToSeekTo := getMessageIdToSeekTo(partitionIdx)

// 	// TODO: This function doesn't exist. How do I implement it?
// 	consumer.Seek(partitionIdx, messageIdToSeekTo)

// 	// Start consuming
// 	// ...

// 	return
// }

func TestSeekPartitions(t *testing.T) {
	err := withPulsar(context.Background(), pulsar.ClientOptions{
		URL:              pulsarUrl,
		OperationTimeout: defaultPulsarTimeout,
	}, func(ctx context.Context, client pulsar.Client) error {
		producer, err := client.CreateProducer(pulsar.ProducerOptions{
			Topic: pulsarTopic,
		})
		if err != nil {
			return errors.WithStack(err)
		}

		// Publish some messages.
		// messageIds := make([]pulsar.MessageID, 0)
		firstMessageIdByPartition := make(map[int32]pulsar.MessageID)
		lastMessageIdByPartition := make(map[int32]pulsar.MessageID)
		firstKeyByPartition := make(map[int32]string)
		lastKeyByPartition := make(map[int32]string)
		for i := 0; i < 100; i++ {
			key := "foo-" + fmt.Sprint(i)
			messageId, err := producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: make([]byte, 0),
				Key:     key,
			})
			if err != nil {
				return errors.WithStack(err)
			}
			fmt.Println("sent ", messageId.PartitionIdx(), " - ", key)
			// messageIds = append(messageIds, messageId)
			if _, ok := firstMessageIdByPartition[messageId.PartitionIdx()]; !ok {
				firstMessageIdByPartition[messageId.PartitionIdx()] = messageId
				firstKeyByPartition[messageId.PartitionIdx()] = key
			}
			lastMessageIdByPartition[messageId.PartitionIdx()] = messageId
			lastKeyByPartition[messageId.PartitionIdx()] = key
		}

		// Create a consumer and seek it to the first message published for each partition.
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            pulsarTopic,
			SubscriptionName: pulsarSubscription,
			Type:             pulsar.KeyShared,
		})
		if err != nil {
			return errors.WithStack(err)
		}

		messageIdsToSeek := make([]pulsar.MessageID, 0, len(firstMessageIdByPartition))
		for _, messageId := range firstMessageIdByPartition {
			fmt.Println(messageId)
			messageIdsToSeek = append(messageIdsToSeek, messageId)
		}
		consumer, err = SeekPartitions(ctx, client, consumer, pulsarTopic, messageIdsToSeek)
		if err != nil {
			return err
		}

		// Read messages until we've seen all the first values.
		confirmedPartitions := make(map[int32]bool)
		for len(confirmedPartitions) < len(firstKeyByPartition) {
			ctxWithTimeout, _ := context.WithTimeout(ctx, 10*time.Second)
			message, err := consumer.Receive(ctxWithTimeout)
			if err != nil {
				return errors.WithStack(err)
			}
			partitionIdx := message.ID().PartitionIdx()
			fmt.Println("received ", partitionIdx, " - ", message.Key())
			if _, ok := confirmedPartitions[partitionIdx]; !ok {
				if correct, ok := firstKeyByPartition[partitionIdx]; ok {
					if !assert.Equal(t, correct, message.Key()) {
						return nil
					}
				} else {
					return fmt.Errorf("no first value present for partition %d", partitionIdx)
				}
				confirmedPartitions[partitionIdx] = true
			}
		}

		// Seek to the final message id for each partition.
		messageIdsToSeek = make([]pulsar.MessageID, 0, len(lastMessageIdByPartition))
		for _, messageId := range lastMessageIdByPartition {
			messageIdsToSeek = append(messageIdsToSeek, messageId)
		}
		consumer, err = SeekPartitions(ctx, client, consumer, pulsarTopic, messageIdsToSeek)
		if err != nil {
			return err
		}

		// Read messages until we've seen all the last values.
		confirmedPartitions = make(map[int32]bool)
		for len(confirmedPartitions) < len(lastKeyByPartition) {
			ctxWithTimeout, _ := context.WithTimeout(ctx, 10*time.Second)
			message, err := consumer.Receive(ctxWithTimeout)
			if err != nil {
				return errors.WithStack(err)
			}
			partitionIdx := message.ID().PartitionIdx()
			if _, ok := confirmedPartitions[partitionIdx]; !ok {
				if correct, ok := lastKeyByPartition[partitionIdx]; ok {
					if !assert.Equal(t, correct, message.Key()) {
						return nil
					}
				} else {
					return fmt.Errorf("no last value present for partition %d", partitionIdx)
				}
				confirmedPartitions[partitionIdx] = true
			}
		}

		// // Create some consumers.
		// consumers := make([]pulsar.Consumer, numConsumers)
		// for i := 0; i < numConsumers; i++ {
		// 	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		// 		Topic: pulsarTopic,
		// 		SubscriptionName: pulsarSubscription,
		// 		Type: pulsar.KeyShared,
		// 	})
		// 	if err != nil {
		// 		return errors.WithStack(err)
		// 	}
		// 	consumers[i] = consumer
		// }

		//

		return nil
	})
	assert.NoError(t, err)

	// Let's assume a partitioned topic exists.
	// Put some data in. Make sure we have a couple of partitions.
	// Create two consumers.
	// Read messages. Ensure all partitions are covered.
	// Seek to the first messages written.
	// Read to ensure they match.
	// Seek to the last messages written. Seek to ensure they match.
}
