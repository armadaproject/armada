package client

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/prometheus/common/log"
	"reflect"
	"time"
)

func WatchJobSet(client api.EventClient, jobSetId string) {

	running := 0
	finished := 0
	failed := 0

	r := make(map[string]bool)

	for {
		clientStream, e := client.GetJobSetEvents(context.Background(), &api.JobSetRequest{Id: jobSetId, Watch: true})

		if e != nil {
			log.Error(e)
			time.Sleep(5 * time.Second)
			continue
		}

		for {

			msg, e := clientStream.Recv()
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				continue
			}

			event, e := api.UnwrapEvent(msg.Message)
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				continue
			}

			switch event.(type) {
			case *api.JobRunningEvent:
				r[event.GetJobId()] = true
				running++
			case *api.JobFailedEvent:
				if r[event.GetJobId()] {
					r[event.GetJobId()] = false
					running--
				}
				failed++
			case *api.JobSucceededEvent:
				if r[event.GetJobId()] {
					r[event.GetJobId()] = false
					running--
				}
				finished++
			}

			log.
				With("event", reflect.TypeOf(event)).
				With("jobId", event.GetJobId()).
				Infof("running: %d finished: %d, failed: %d", running, finished, failed)
		}
	}
}
