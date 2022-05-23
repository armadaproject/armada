package main

import (
	ctx "context"
	"github.com/G-Research/armada/pkg/api"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		logrus.Fatal(err)
	}
	defer conn.Close()

	//queue := api.NewSubmitClient(conn)
	//_, err = queue.CreateQueue(ctx.Background(), &api.Queue{
	//	Name:           "test-queue",
	//	PriorityFactor: 2,
	//})
	//if err != nil {
	//	logrus.Fatal(err)
	//}

	client := api.NewEventClient(conn)
	eventsClient, err := client.GetJobSetEvents(ctx.Background(), &api.JobSetRequest{
		Queue:          "test-queue",
		Id:             "test-jobset",
		Watch:          true,
		ErrorIfMissing: false,
	})
	if err != nil {
		logrus.Fatal(err)
	}
	msgsReceived := 0
	for {
		_, err := eventsClient.Recv()
		if err != nil {
			logrus.Fatal(err.Error())
		} else {
			msgsReceived++
			logrus.Infof("Received %d Messages", msgsReceived)
		}
	}

}
