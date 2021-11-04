package client

import (
	"context"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/domain"
)

func GetJobSetState(client api.EventClient, queue, jobSetId string, context context.Context) *domain.WatchContext {
	latestState := domain.NewWatchContext()
	WatchJobSet(client, queue, jobSetId, false, context, func(state *domain.WatchContext, _ api.Event) bool {
		latestState = state
		return false
	})
	return latestState
}

func WatchJobSet(client api.EventClient, queue, jobSetId string, waitForNew bool, context context.Context, onUpdate func(*domain.WatchContext, api.Event) bool) *domain.WatchContext {
	return WatchJobSetWithJobIdsFilter(client, queue, jobSetId, waitForNew, []string{}, context, onUpdate)
}

func WatchJobSetWithJobIdsFilter(client api.EventClient, queue, jobSetId string, waitForNew bool, jobIds []string, context context.Context, onUpdate func(*domain.WatchContext, api.Event) bool) *domain.WatchContext {
	state := domain.NewWatchContext()

	jobIdsSet := util.StringListToSet(jobIds)
	filterOnJobId := len(jobIdsSet) > 0
	lastMessageId := ""

	for {
		select {
		case <-context.Done():
			return state
		default:
		}

		clientStream, e := client.GetJobSetEvents(context, &api.JobSetRequest{Queue: queue, Id: jobSetId, FromMessageId: lastMessageId, Watch: waitForNew})

		if e != nil {
			log.Error(e)
			time.Sleep(5 * time.Second)
			continue
		}

		for {

			msg, e := clientStream.Recv()
			if e != nil {
				if e == io.EOF {
					return state
				}
				if !isTransportClosingError(e) {
					log.Error(e)
				}
				time.Sleep(5 * time.Second)
				break
			}
			lastMessageId = msg.Id

			event, e := api.UnwrapEvent(msg.Message)
			if e != nil {
				// This can mean that the event type reported from server is unknown to the client
				log.Error(e)
				continue
			}

			if filterOnJobId && !jobIdsSet[event.GetJobId()] {
				continue
			}

			state.ProcessEvent(event)

			shouldExit := onUpdate(state, event)
			if shouldExit {
				return state
			}
		}
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

func WatchJobEvents(ctx context.Context, client api.EventClient, queue, jobSetId string, onUpdate func(*domain.WatchContext, api.Event) bool) *domain.WatchContext {
	state := domain.NewWatchContext()

	lastMessageId := ""

	for {
		select {
		case <-ctx.Done():
			return state
		default:
		}

		clientStream, e := client.Watch(ctx, &api.WatchRequest{Queue: queue, JobSetId: jobSetId, FromId: lastMessageId})

		if e != nil {
			log.Error(e)
			time.Sleep(5 * time.Second)
			continue
		}

		for {

			msg, e := clientStream.Recv()
			if e == io.EOF {
				return state
			}
			if e != nil {
				status, _ := status.FromError(e)
				if status.Code() == codes.Unavailable {
					log.Error(e)
				}
				if status.Code() == codes.NotFound {
					log.Error(e)
					return state
				}
				time.Sleep(5 * time.Second)
				break
			}
			lastMessageId = msg.Id

			event, e := api.UnwrapEvent(msg.Message)
			if e != nil {
				// This can mean that the event type reported from server is unknown to the client
				log.Error(e)
				continue
			}

			state.ProcessEvent(event)

			shouldExit := onUpdate(state, event)
			if shouldExit {
				return state
			}
		}
	}
}
