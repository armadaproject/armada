package queue

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetAllAPI func() ([]*api.Queue, error)

func GetAll(getConnectionDetails client.ConnectionDetails) GetAllAPI {
	return func() ([]*api.Queue, error) {
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		conn, err := client.CreateApiConnection(connectionDetails)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to api because %s", err)
		}
		defer conn.Close()

		client := api.NewSubmitClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		allQueues := make([]*api.Queue, 0)
		queueStream, err := client.GetQueues(ctx, &api.StreamingQueueGetRequest{})
		if err != nil {
			log.Error(err)
			return nil, err
		}

		for {

			msg, e := queueStream.Recv()
			if e != nil {
				if err, ok := status.FromError(e); ok {
					switch err.Code() {
					case codes.NotFound:
						log.Error(err.Message())
						return nil, e
					case codes.PermissionDenied:
						log.Error(err.Message())
						return nil, e
					}
				}
				if e == io.EOF {
					return nil, e
				}
				if !isTransportClosingError(e) {
					log.Error(e)
				}
				break
			}

			event := msg.Event
			switch event.(type) {
			case *api.StreamingQueueMessage_Queue:
				allQueues = append(allQueues, event.(*api.StreamingQueueMessage_Queue).Queue)
			case *api.StreamingQueueMessage_End:
				return allQueues, nil
			}
		}

		return allQueues, nil
	}
}

func isTransportClosingError(e error) bool {
	if err, ok := status.FromError(e); ok {
		switch err.Code() {
		case codes.Unavailable:
			return true
		}
	}
	return false
}
