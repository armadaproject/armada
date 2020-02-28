package client

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/client/domain"
	"github.com/G-Research/armada/internal/common/util"
)

func WatchJobSet(client api.EventClient, queue, jobSetId string, waitForNew bool, context context.Context, onUpdate func(*domain.WatchContext, api.Event) bool) {
	WatchJobSetWithJobIdsFilter(client, queue, jobSetId, waitForNew, []string{}, context, onUpdate)
}

func WatchJobSetWithJobIdsFilter(client api.EventClient, queue, jobSetId string, waitForNew bool, jobIds []string, context context.Context, onUpdate func(*domain.WatchContext, api.Event) bool) {
	state := domain.NewWatchContext()

	jobIdsSet := util.StringListToSet(jobIds)
	filterOnJobId := len(jobIdsSet) > 0
	lastMessageId := ""

	for {
		select {
		case <-context.Done():
			return
		default:
		}

		clientStream, e := client.GetJobSetEvents(context, &api.JobSetRequest{Queue: queue, Id: jobSetId, FromMessageId: lastMessageId, Watch: waitForNew})

		if e != nil {
			log.Error(e)
			time.Sleep(5 * time.Second)
			continue
		}

		receivedThisCall := 0

		for {

			msg, e := clientStream.Recv()
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				break
			}
			receivedThisCall++
			lastMessageId = msg.Id

			event, e := api.UnwrapEvent(msg.Message)
			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				continue
			}

			if filterOnJobId && !jobIdsSet[event.GetJobId()] {
				continue
			}

			state.ProcessEvent(event)

			shouldExit := onUpdate(state, event)
			if shouldExit {
				return
			}
		}

		if receivedThisCall == 0 && !waitForNew {
			return
		}
	}
}
