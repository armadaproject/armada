package main

import (
	"context"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"time"
)
import log "github.com/sirupsen/logrus"

const jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"

var jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)

// Cancelled
var jobCancelled = &armadaevents.EventSequence_Event{
	Event: &armadaevents.EventSequence_Event_CancelledJob{
		CancelledJob: &armadaevents.CancelledJob{
			JobId: jobIdProto,
		},
	},
}

func main() {

	es := &armadaevents.EventSequence{
		Queue:      "test-queue",
		JobSetName: "test-jobset",
		UserId:     "chrisma",
		Events: []*armadaevents.EventSequence_Event{
			jobCancelled,
		},
	}

	payload, err := proto.Marshal(es)
	if err != nil {
		log.Fatal(err)
	}

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "jobset-events",
	})
	if err != nil {
		log.Fatal(err)
	}
	for {
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload: payload,
			Key:     "Jobset1",
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				log.Warnf("Error sending message +%v", err)
			}
		})
		time.Sleep(1 * time.Second)
	}
}
