package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/events"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

// Pulsar configuration. Must be manually reconciled with changes to the test setup or Armada.
const pulsarUrl = "pulsar://localhost:6650"
const pulsarTopic = "armada"
const pulsarSubscription = "e2e-test-topic"
const armadaUrl = "localhost:50051"
const armadaQueueName = "e2e-test-queue"

// Test publishing and receiving a message to/from Pulsar.
func TestPublishReceive(t *testing.T) {
	err := withSetup(func(ctx context.Context, _ api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("hello"),
		})
		if err != nil {
			return err
		}

		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		msg, err := consumer.Receive(ctxWithTimeout)
		if err != nil {
			return err
		}
		assert.Equal(t, "hello", string(msg.Payload()))

		return nil
	})
	assert.NoError(t, err)
}

// Test that submitting a job to Armada results in the correct sequence of Pulsar message being produced.
func TestSubmitJobTransitions(t *testing.T) {
	err := withSetup(func(ctx context.Context, client api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error {
		ctxWithTimeout, _ := context.WithTimeout(context.Background(), time.Second)
		req := createJobSubmitRequest("personal-anonymous") // Namespace created by test setup
		_, err := client.SubmitJobs(ctxWithTimeout, req)
		if err != nil {
			return err
		}

		numEventsExpected := 5
		sequence, err := receiveJobSetSequence(ctx, consumer, armadaQueueName, req.JobSetId, numEventsExpected, 10*time.Second)
		if err != nil {
			return err
		}
		if ok := assert.Equal(t, numEventsExpected, len(sequence.Events)); !ok {
			return nil
		}
		if ok := assert.IsType(t, &events.EventSequence_Event_SubmitJob{}, sequence.Events[0].Event); !ok {
			return nil
		}
		if ok := assert.IsType(t, &events.EventSequence_Event_JobRunLeased{}, sequence.Events[1].Event); !ok {
			return nil
		}
		if ok := assert.IsType(t, &events.EventSequence_Event_JobRunAssigned{}, sequence.Events[2].Event); !ok {
			return nil
		}
		if ok := assert.IsType(t, &events.EventSequence_Event_JobRunRunning{}, sequence.Events[3].Event); !ok {
			return nil
		}
		if ok := assert.IsType(t, &events.EventSequence_Event_JobRunSucceeded{}, sequence.Events[4].Event); !ok {
			return nil
		}
		// TODO: We should also have a job succeeded message here.
		fmt.Printf("Received %d events\n", len(sequence.Events))

		return nil
	})
	assert.NoError(t, err)
}

// receiveJobSetSequence receives messages from Pulsar, discarding any messages not for queue and jobSetName.
// The events contained in the remaining messages are collected in a single sequence, which is returned.
func receiveJobSetSequence(ctx context.Context, consumer pulsar.Consumer, queue string, jobSetName string, numEventsExpected int, timeout time.Duration) (result *events.EventSequence, err error) {
	result = &events.EventSequence{
		Queue:      queue,
		JobSetName: jobSetName,
		Events:     make([]*events.EventSequence_Event, 0),
	}
	for len(result.Events) < numEventsExpected {
		ctxWithTimeout, _ := context.WithTimeout(ctx, timeout)
		var msg pulsar.Message
		msg, err = consumer.Receive(ctxWithTimeout)
		if err == context.DeadlineExceeded {
			err = nil // Timeout is expected; ignore.
			return
		} else if err != nil {
			fmt.Println("Pulsar receive error", err)
			continue
			// return
		}
		consumer.Ack(msg)

		sequence := &events.EventSequence{}
		err = proto.Unmarshal(msg.Payload(), sequence)
		if err != nil {
			fmt.Println("Sequence unmarshalling error", err)
			continue
		}
		fmt.Printf("Received sequence %s\n", sequence)

		if sequence.Queue != queue || sequence.JobSetName != jobSetName {
			fmt.Println("Skipping sequence")
			continue
		}

		result.Events = append(result.Events, sequence.Events...)
	}
	return
}

// Create a job submit request for testing.
func createJobSubmitRequest(namespace string) *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	return &api.JobSubmitRequest{
		Queue:    armadaQueueName,
		JobSetId: util.NewULID(),
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Namespace: namespace,
				Priority:  0,
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "container1",
							Image: "alpine:3.10",
							Args:  []string{"sleep", "5s"},
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
								Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
							},
						},
					},
				},
			},
		},
	}
}

// Run action with an Armada submit client and a Pulsar producer and consumer.
func withSetup(action func(ctx context.Context, submitClient api.SubmitClient, producer pulsar.Producer, consumer pulsar.Consumer) error) error {

	// Connection to the Armada API. To submit API requests.
	conn, err := client.CreateApiConnection(&client.ApiConnectionDetails{ArmadaUrl: armadaUrl})
	defer conn.Close()
	submitClient := api.NewSubmitClient(conn)

	// Recreate the queue to make sure it's empty.
	err = client.DeleteQueue(submitClient, armadaQueueName)
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		// Queue didn't exist, which is fine; do nothing.
	} else if err != nil {
		return err
	}
	err = client.CreateQueue(submitClient, &api.Queue{Name: armadaQueueName, PriorityFactor: 1})
	if err != nil {
		return err
	}

	// Connection to Pulsar. To check that the correct sequence of messages are produced.
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarUrl,
	})
	if err != nil {
		return err
	}
	defer pulsarClient.Close()

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{
		Topic: pulsarTopic,
	})
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := pulsarClient.Subscribe(pulsar.ConsumerOptions{
		Topic:            pulsarTopic,
		SubscriptionName: pulsarSubscription,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Skip any messages already published to Pulsar.
	err = consumer.SeekByTime(time.Now())
	if err != nil {
		return err
	}

	return action(context.Background(), submitClient, producer, consumer)
}
