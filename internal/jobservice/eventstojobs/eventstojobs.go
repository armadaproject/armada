package eventstojobs

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/pkg/api"
)

// Service that subscribes to events and stores JobStatus in the repository.
type EventsToJobService struct {
	queue          string
	jobSetId       string
	eventClient    events.JobEventReader
	jobServiceRepo repository.JobTableUpdater
}

func NewEventsToJobService(
	queue string,
	jobSetId string,
	eventClient events.JobEventReader,
	jobServiceRepository repository.JobTableUpdater,
) *EventsToJobService {
	return &EventsToJobService{
		queue:          queue,
		jobSetId:       jobSetId,
		eventClient:    eventClient,
		jobServiceRepo: jobServiceRepository,
	}
}

// Subscribes to a JobSet from jobsetid. Will retry until there is a successful exit, up to the TTL
func (evToJS *EventsToJobService) SubscribeToJobSetId(ctx context.Context, timeout int64, fromMessageId string) error {
	ctx, cancel := context.WithCancel(ctx)
	g, _ := errgroup.WithContext(ctx)
	expiresAt := time.Now().Add(time.Duration(timeout) * time.Second)

	log.Infof("SubscribeToJobSetId start for %s/%s with messageId %s", evToJS.queue, evToJS.jobSetId, fromMessageId)

	g.Go(func() error {
		defer cancel()

		// Once we unsubscribed from the job-set, we need to close the GRPC connection.
		// According to GRPC official docs, you can only end a client stream by either canceling the context or closing the connection
		// // This will log an error to the jobservice log saying that the connection was used.
		ticker := time.NewTicker(time.Duration(timeout) * time.Second)
		for {
			select {
			case <-ctx.Done():
				return errors.Errorf("context done")
			case t := <-ticker.C:
				jobSetFound, _, err := evToJS.jobServiceRepo.IsJobSetSubscribed(ctx, evToJS.queue, evToJS.jobSetId)
				if err != nil {
					return errors.Errorf("unsubscribe jobsets: %v", err)
				}
				if !jobSetFound {
					return nil
				}
				if t.After(expiresAt) {
					log.Infof("JobSet %s/%s unsubscribing and messageId is %s", evToJS.queue, evToJS.jobSetId, fromMessageId)
					return errors.Errorf("stream subscription ttl exceeded: %v", timeout)
				}
			}
		}
	})
	g.Go(func() error {
		var err error
		defer func() {
			evToJS.eventClient.Close()
			log.Info("closed the event client connection")
			// cancel the ticker go routine if an error originated here
			if err != nil {
				cancel()
			}
		}()

		log.Infof("GetJobEventMessage for %s/%s with id %s", evToJS.queue, evToJS.jobSetId, fromMessageId)
		stream, err := evToJS.eventClient.GetJobEventMessage(ctx, &api.JobSetRequest{
			Id:            evToJS.jobSetId,
			Queue:         evToJS.queue,
			Watch:         true,
			FromMessageId: fromMessageId,
		})
		if err != nil {
			log.Error("error from GetJobEventMessage", err)
			cancel()
		}

		// this loop will run until the context is canceled
		for {
			select {
			case <-ctx.Done():
				log.Errorf("context is done on %s/%s", evToJS.queue, evToJS.jobSetId)
				return nil
			default:
				requestFields := log.Fields{
					"job_set_id": evToJS.jobSetId,
					"queue":      evToJS.queue,
				}
				msg, err := stream.Recv()
				if err != nil {
					log.WithError(err).Error("could not obtain job set event message, retrying")
					settingSubscribeErr := evToJS.jobServiceRepo.SetSubscriptionError(
						ctx, evToJS.queue, evToJS.jobSetId, err.Error(), fromMessageId)
					if settingSubscribeErr != nil {
						log.WithError(settingSubscribeErr).Error("could not set error field in job set table")
					}
					time.Sleep(5 * time.Second)
					continue
				}
				errClear := evToJS.jobServiceRepo.AddMessageIdAndClearSubscriptionError(
					ctx, evToJS.queue, evToJS.jobSetId, fromMessageId)
				if errClear != nil {
					log.WithError(errClear).Error("could not clear subscription error from job set table")
				}
				currentJobId := api.JobIdFromApiEvent(msg.Message)
				jobStatus := EventsToJobResponse(*msg.Message)
				if jobStatus != nil {
					log.WithFields(requestFields).Debugf("fromMessageId: %s JobId: %s State: %s", fromMessageId,
						currentJobId, jobStatus.GetState().String())
					jobStatus := repository.NewJobStatus(evToJS.queue, evToJS.jobSetId, currentJobId, *jobStatus)
					err := evToJS.jobServiceRepo.UpdateJobServiceDb(ctx, jobStatus)
					if err != nil {
						log.WithError(err).Error("could not update job status, retrying")
						time.Sleep(5 * time.Second)
						continue
					}
				} else {
					log.WithFields(requestFields).Debugf("JobId: %s Message: %v", currentJobId, msg.Message)
				}
				// advance the message id for next loop
				fromMessageId = msg.GetId()
			}
		}
	})
	return g.Wait()
}
