package eventstojobs

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/jobservice/events"
	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
)

// Service that subscribes to events and stores JobStatus in the repository.
type EventsToJobService struct {
	queue                string
	jobSetId             string
	jobId                string
	eventClient          events.JobEventReader
	jobServiceRepository repository.JobTableUpdater
}

func NewEventsToJobService(
	queue string,
	jobSetId string,
	jobId string,
	eventClient events.JobEventReader,
	jobServiceRepository repository.JobTableUpdater,
) *EventsToJobService {
	return &EventsToJobService{
		queue:                queue,
		jobSetId:             jobSetId,
		jobId:                jobId,
		eventClient:          eventClient,
		jobServiceRepository: jobServiceRepository,
	}
}

// Subscribes to a JobSet from jobsetid. Will retry until there is a successful exit, up to the TTL
func (eventToJobService *EventsToJobService) SubscribeToJobSetId(context context.Context, ttlSecs int64) error {
	return eventToJobService.streamCommon(context, ttlSecs)
}

func (eventToJobService *EventsToJobService) streamCommon(ctx context.Context, timeout int64) error {
	var fromMessageId string
	eventToJobService.jobServiceRepository.SubscribeJobSet(eventToJobService.queue, eventToJobService.jobSetId)
	ctx, cancel := context.WithCancel(ctx)
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
				if !eventToJobService.jobServiceRepository.IsJobSetSubscribed(
					eventToJobService.queue,
					eventToJobService.jobSetId) {
					return nil
				}
				if t.After(expiresAt) {
					return errors.Errorf("stream subscription ttl exceeded: %v", timeout)
				}
			}
		}
	})
	g.Go(func() error {
		var err error
		defer func() {
			eventToJobService.eventClient.Close()
			// cancel the ticker go routine if an error originated here
			if err != nil {
				cancel()
			}
		}()

		// this loop will run until the context is canceled or an error is encountered
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
				msg, err := eventToJobService.eventClient.GetJobEventMessage(ctx, &api.JobSetRequest{
					Id:             eventToJobService.jobSetId,
					Queue:          eventToJobService.queue,
					Watch:          true,
					FromMessageId:  fromMessageId,
					ErrorIfMissing: false,
				})
				if err != nil {
					log.WithError(err).Error("could not obtain job set event message, retrying")
					time.Sleep(5 * time.Second)
					continue
				}
				currentJobId := api.JobIdFromApiEvent(msg.Message)
				jobStatus := EventsToJobResponse(*msg.Message)
				if jobStatus != nil {
					jobStatus := repository.NewJobStatus(eventToJobService.queue, eventToJobService.jobSetId, currentJobId, *jobStatus)
					err := eventToJobService.jobServiceRepository.UpdateJobServiceDb(jobStatus)
					if err != nil {
						log.WithError(err).Error("could not update job status, retrying")
						time.Sleep(5 * time.Second)
						continue
					}
				}
				// advance the message id for next loop
				fromMessageId = msg.GetId()
			}
		}
	})
	return g.Wait()
}
