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
	"github.com/armadaproject/armada/pkg/api/jobservice"
)

// Service that subscribes to events and stores JobStatus in the repository.
type EventsToJobService struct {
	queue                string
	jobSetId             string
	eventClient          events.JobEventReader
	jobServiceRepository repository.JobTableUpdater
}

func NewEventsToJobService(
	queue string,
	jobSetId string,
	eventClient events.JobEventReader,
	jobServiceRepository repository.JobTableUpdater,
) *EventsToJobService {
	return &EventsToJobService{
		queue:                queue,
		jobSetId:             jobSetId,
		eventClient:          eventClient,
		jobServiceRepository: jobServiceRepository,
	}
}

// Subscribes to a JobSet from jobsetid. Will retry until there is a successful exit, up to the TTL
func (eventToJobService *EventsToJobService) SubscribeToJobSetId(context context.Context, ttlSecs int64, fromMessageId string) error {
	log.Infof("subscribeToJobSetId start for %s/%s with messageId %s", eventToJobService.queue, eventToJobService.jobSetId, fromMessageId)
	err := eventToJobService.streamCommon(context, ttlSecs, fromMessageId)
	if err != nil {
		log.Infof("subscribeToJobSetId ended for %s/%s with error %v", eventToJobService.queue, eventToJobService.jobSetId, err)
		if err != nil {
			log.Error("unable to unsubscribe database due to ", err)
		}
		return err
	}
	return nil
}

func (eventToJobService *EventsToJobService) streamCommon(inCtx context.Context, timeout int64, fromMessageId string) error {
	ctx, cancel := context.WithCancel(inCtx)
	g, _ := errgroup.WithContext(ctx)
	expiresAt := time.Now().Add(time.Duration(timeout) * time.Second)
	g.Go(func() error {
		defer cancel()

		// Once we unsubscribed from the job-set, we need to close the GRPC connection.
		// According to GRPC official docs, you can only end a client stream by either canceling the context or closing the connection
		// // This will log an error to the jobservice log saying that the connection was used.
		ticker := time.NewTicker(time.Duration(timeout) * time.Second)
		for {
			select {
			case <-ctx.Done():
				return nil
			case t := <-ticker.C:
				jobSetFound, _, err := eventToJobService.jobServiceRepository.IsJobSetSubscribed(inCtx, eventToJobService.queue, eventToJobService.jobSetId)
				if err != nil {
					return errors.Errorf("unsubscribe jobsets: %v", err)
				}
				if !jobSetFound {
					return nil
				}
				if t.After(expiresAt) {
					log.Infof("JobSet %s/%s unsubcribing and messageId is %s", eventToJobService.queue, eventToJobService.jobSetId, fromMessageId)
					log.Infof("stream subscription ttl exceeded: %v", timeout)
					return nil
				}
			}
		}
	})
	g.Go(func() error {
		var err error
		defer func() {
			eventToJobService.eventClient.Close()
			log.Info("closed the event client connection")
			// cancel the ticker go routine if an error originated here
			if err != nil {
				cancel()
			}
		}()
		requestFields := log.Fields{
			"job_set_id": eventToJobService.jobSetId,
			"queue":      eventToJobService.queue,
		}

		// this loop will run until the context is canceled
		for {
			select {
			case <-ctx.Done():
				log.Infof("context is done on %s/%s and we are leaving StreamCommon", eventToJobService.queue, eventToJobService.jobSetId)
				log.Infof("Messageid: %s", fromMessageId)

				errClear := eventToJobService.jobServiceRepository.AddMessageIdAndClearSubscriptionError(
					inCtx, eventToJobService.queue, eventToJobService.jobSetId, fromMessageId)
				if errClear != nil {
					log.WithError(errClear).Error("could not clear subscription error from job set table")
					return errClear
				}
				return nil
			default:

				msg, err := eventToJobService.eventClient.GetJobEventMessage(inCtx, &api.JobSetRequest{
					Id:             eventToJobService.jobSetId,
					Queue:          eventToJobService.queue,
					Watch:          true,
					FromMessageId:  fromMessageId,
					ErrorIfMissing: true,
				})
				if err != nil {
					log.WithError(err).Error("could not obtain job set event message, retrying")
					settingSubscribeErr := eventToJobService.jobServiceRepository.SetSubscriptionError(
						inCtx, eventToJobService.queue, eventToJobService.jobSetId, err.Error(), fromMessageId)
					if settingSubscribeErr != nil {
						log.WithError(settingSubscribeErr).Error("could not set error field in job set table")
					}
					return nil
				}
				errClear := eventToJobService.jobServiceRepository.AddMessageIdAndClearSubscriptionError(
					inCtx, eventToJobService.queue, eventToJobService.jobSetId, fromMessageId)
				if errClear != nil {
					log.WithError(errClear).Error("could not clear subscription error from job set table")
					return nil
				}
				currentJobId := api.JobIdFromApiEvent(msg.Message)
				jobStatus := EventsToJobResponse(*msg.Message)
				if jobStatus != nil {
					if jobStatus.State != jobservice.JobServiceResponse_SUCCEEDED {
						log.WithFields(requestFields).Infof("fromMessageId: %s JobId: %s State: %s", fromMessageId, currentJobId, jobStatus.GetState().String())
					}
					jobStatus := repository.NewJobStatus(eventToJobService.queue, eventToJobService.jobSetId, currentJobId, *jobStatus)
					err := eventToJobService.jobServiceRepository.UpdateJobServiceDb(inCtx, jobStatus)
					if err != nil {
						log.WithError(err).Error("could not update job status, retry on next subscription")
						return nil
					}
				}
				// advance the message id for next loop
				fromMessageId = msg.GetId()
			}
		}
	})
	return g.Wait()
}
