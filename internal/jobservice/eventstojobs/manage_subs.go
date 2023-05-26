package eventstojobs

import (
	"context"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/pkg/api"
	log "github.com/sirupsen/logrus"
)

type JobSetKey struct {
	Queue    string
	JobSetID string
}

type JobSubscription struct {
	JobSetKey

	fromMessageId string

	eventReader events.JobEventReader

	ctx    context.Context
	cancel context.CancelFunc
}

type JobSubscriptionExecutor struct {
	ctx           context.Context
	sqlJobService repository.SQLJobService
	eventReader   events.JobEventReader

	subscriptions map[JobSetKey]*JobSubscription
	mutex         sync.Mutex

	nextDue    <-chan time.Time
	nextDueSub *JobSubscription
}

func NewJobSubscriptionExecutor(ctx context.Context, eventReader events.JobEventReader, sqlJobService repository.SQLJobService) *JobSubscriptionExecutor {
	return &JobSubscriptionExecutor{
		ctx:           ctx,
		eventReader:   eventReader,
		sqlJobService: sqlJobService,
		subscriptions: make(map[JobSetKey]*JobSubscription),
	}
}

// TODO: Some mechanism for new subs to signal this executor (channel anyone?)
// that a new subscription has come in.
func (jse *JobSubscriptionExecutor) Manage() error {
	// TODO: Setup nextDue vars here.

	ticker := time.NewTicker(time.Second)
	// Main subscription loop.
	for {
		select {
		case <-jse.ctx.Done():
			log.Infof("Context is done.")
			return nil
		case <-jse.nextDue:
			log.Info("Due subscription up for review...")
		case <-ticker.C:
			log.Debug("Heartbeat")
		}
	}
}

func (js *JobSubscription) Subscribe() error {
	log.Infof("GetJobEventMessage for %s/%s with id %s", js.Queue, js.JobSetID, js.fromMessageId)
	stream, err := js.eventReader.GetJobEventMessage(js.ctx, &api.JobSetRequest{
		Id:            js.JobSetID,
		Queue:         js.Queue,
		Watch:         true,
		FromMessageId: js.fromMessageId,
	})
	if err != nil {
		log.Error("error from GetJobEventMessage", err)
		js.cancel()
		return err
	}

	// this loop will run until the context is canceled
	for {
		select {
		case <-js.ctx.Done():
			log.Errorf("context is done on %s/%s", js.Queue, js.JobSetID)
			return nil
		default:
			requestFields := log.Fields{
				"job_set_id": js.JobSetID,
				"queue":      js.Queue,
			}
			msg, err := stream.Recv()
			if err != nil {
				log.WithError(err).Error("could not obtain job set event message, retrying")
				settingSubscribeErr := js.jobServiceRepository.SetSubscriptionError(
					js.ctx, js.Queue, js.JobSetID, err.Error(), js.fromMessageId)
				if settingSubscribeErr != nil {
					log.WithError(settingSubscribeErr).Error("could not set error field in job set table")
				}
				time.Sleep(5 * time.Second)
				continue
			}
			errClear := js.jobServiceRepository.AddMessageIdAndClearSubscriptionError(
				js.ctx, js.Queue, js.JobSetID, js.fromMessageId)
			if errClear != nil {
				log.WithError(errClear).Error("could not clear subscription error from job set table")
			}
			currentJobId := api.JobIdFromApiEvent(msg.Message)
			jobStatus := EventsToJobResponse(*msg.Message)
			if jobStatus != nil {
				log.WithFields(requestFields).Debugf("fromMessageId: %s JobId: %s State: %s", js.fromMessageId, currentJobId, jobStatus.GetState().String())
				jobStatus := repository.NewJobStatus(js.Queue, js.JobSetID, currentJobId, *jobStatus)
				err := js.jobServiceRepository.UpdateJobServiceDb(js.ctx, jobStatus)
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

	return nil
}
