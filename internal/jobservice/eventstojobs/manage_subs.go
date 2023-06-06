package eventstojobs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/pkg/api"
	log "github.com/sirupsen/logrus"
)

type JobSetSubscription struct {
	repository.JobSetKey

	fromMessageId string

	sqlJobService repository.SQLJobService
	eventReader   events.JobEventReader

	ctx    context.Context
	cancel context.CancelFunc

	subTimeout time.Duration

	subDoneChan chan<- *repository.JobSetKey
}

type JobSetSubscriptionExecutor struct {
	ctx           context.Context
	sqlJobService repository.SQLJobService
	eventReader   events.JobEventReader

	subscriptions map[repository.JobSetKey]*JobSetSubscription
	mutex         sync.Mutex

	newSubChan  <-chan *repository.JobSetSubscriptionInfo
	subDoneChan chan *repository.JobSetKey

	subTimeout time.Duration
}

func NewJobSetSubscriptionExecutor(ctx context.Context,
	eventReader events.JobEventReader,
	sqlJobService repository.SQLJobService,
	newSubChan <-chan *repository.JobSetSubscriptionInfo,
	subTimeout time.Duration,
) *JobSetSubscriptionExecutor {
	return &JobSetSubscriptionExecutor{
		ctx:           ctx,
		eventReader:   eventReader,
		sqlJobService: sqlJobService,
		subscriptions: make(map[repository.JobSetKey]*JobSetSubscription),
		newSubChan:    newSubChan,
		subDoneChan:   make(chan *repository.JobSetKey),
		subTimeout:    subTimeout,
	}
}

func (jse *JobSetSubscriptionExecutor) Manage() error {
	// Main {un}subscribe loop.
	for {
		select {
		case <-jse.ctx.Done():
			log.Infof("Context is done.")
			return nil
		case newSubInfo := <-jse.newSubChan:
			jse.addSubscription(newSubInfo)
			jse.launchSubscriber(&newSubInfo.JobSetKey)
		case subDoneKey := <-jse.subDoneChan:
			log.Infof("Removing subscription on %s/%s", subDoneKey.Queue, subDoneKey.JobSetId)
			jse.removeSubscription(subDoneKey)
		}
	}
}

func (jse *JobSetSubscriptionExecutor) addSubscription(sub *repository.JobSetSubscriptionInfo) {
	jse.mutex.Lock()
	defer jse.mutex.Unlock()

	_, ok := jse.subscriptions[sub.JobSetKey]
	if ok {
		// TODO: What to do if subscription already exists locally?
		// Restart it? Do nothing?
	} else { // sub doesn't already exist.
		jse.subscriptions[sub.JobSetKey] = NewJobSetSubscription(
			jse.ctx,
			jse.eventReader,
			sub,
			jse.subTimeout,
			jse.subDoneChan,
			jse.sqlJobService)
	}

	err := jse.sqlJobService.SubscribeJobSet(jse.ctx, sub.Queue, sub.JobSetId, sub.FromMessageId)
	if err != nil {
		log.Errorf("Could not add subscription on %s/%s to DB: %s", sub.Queue, sub.JobSetId, err.Error())
	}
}

func (jse *JobSetSubscriptionExecutor) launchSubscriber(key *repository.JobSetKey) error {
	jse.mutex.Lock()
	sub, ok := jse.subscriptions[*key]
	jse.mutex.Unlock()

	if ok {
		go sub.Subscribe()
		return nil
	}
	return fmt.Errorf("No subscription with specified key %s/%s exists!", sub.Queue, sub.JobSetId)
}

func (jse *JobSetSubscriptionExecutor) removeSubscription(key *repository.JobSetKey) error {
	jse.mutex.Lock()
	defer jse.mutex.Unlock()

	sub, ok := jse.subscriptions[*key]
	if ok {
		sub.cancel()
		delete(jse.subscriptions, *key)
	} else {
		log.Errorf("No subscription with specified key %s/%s exists!", key.Queue, key.JobSetId)
	}

	_, err := jse.sqlJobService.UnsubscribeJobSet(jse.ctx, key.Queue, key.JobSetId)
	return err
}

func NewJobSetSubscription(ctx context.Context, eventReader events.JobEventReader, subInfo *repository.JobSetSubscriptionInfo, subTimeout time.Duration, subDoneChan chan<- *repository.JobSetKey, sqlJobService repository.SQLJobService) *JobSetSubscription {
	newCtx, cancel := context.WithCancel(ctx)
	return &JobSetSubscription{
		ctx:           newCtx,
		cancel:        cancel,
		eventReader:   eventReader,
		JobSetKey:     subInfo.JobSetKey,
		fromMessageId: subInfo.FromMessageId,
		subTimeout:    subTimeout,
		subDoneChan:   subDoneChan,
		sqlJobService: sqlJobService,
	}
}

func (js *JobSetSubscription) Subscribe() error {
	log.Debugf("Subscribe() for %s/%s with message id %s", js.Queue, js.JobSetId, js.fromMessageId)

	defer func() {
		js.subDoneChan <- &repository.JobSetKey{
			Queue:    js.Queue,
			JobSetId: js.JobSetId,
		}
	}()

	log.Infof("Calling GetJobEventMessage on %s/%s", js.Queue, js.JobSetId)

	stream, err := js.eventReader.GetJobEventMessage(js.ctx, &api.JobSetRequest{
		Id:            js.JobSetId,
		Queue:         js.Queue,
		Watch:         true,
		FromMessageId: js.fromMessageId,
	})
	if err != nil {
		log.Error("error from GetJobEventMessage", err)
		js.cancel()
		return err
	}

	log.Infof("Got stream on %s/%s/", js.Queue, js.JobSetId)

	// Subscription self-reaping/timeout.
	timeout := time.NewTicker(js.subTimeout)
	go func() {
		select {
		case <-js.ctx.Done():
			return
		case <-timeout.C:
			log.Infof("JobSetSubscription.Subscribe timeout on %s/%s/", js.Queue, js.JobSetId)
			// Stream is created with *our* context, therefore if we cancel, stream.Recv() should bail out too.
			js.cancel()
			return
		}
	}()

	// Nanosecond, zero wait essentially.
	nextRecv := time.After(1 * time.Nanosecond)

	// this loop will run until the context is canceled
	for {
		select {
		case <-js.ctx.Done():
			log.Infof("context is done for JobSetSubscription on %s/%s", js.Queue, js.JobSetId)
			return nil
		case <-nextRecv:
			requestFields := log.Fields{
				"job_set_id": js.JobSetId,
				"queue":      js.Queue,
			}
			msg, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Infof("Reached stream end for JobSetSubscription on %s/%s", js.Queue, js.JobSetId)
					return nil
				} else if strings.Contains(err.Error(), "context canceled") {
					// The select case will handle context being done/canceled.
					continue
				}

				log.WithError(err).Error("could not obtain job set event message, retrying")
				settingSubscribeErr := js.sqlJobService.SetSubscriptionError(
					js.ctx, js.Queue, js.JobSetId, err.Error(), js.fromMessageId)
				if settingSubscribeErr != nil {
					log.WithError(settingSubscribeErr).Error("could not set error field in job set table")
				}
				nextRecv = time.After(5 * time.Second)
				continue
			}

			// We got a good message, so reset our timeout.
			timeout.Reset(js.subTimeout)

			errClear := js.sqlJobService.AddMessageIdAndClearSubscriptionError(
				js.ctx, js.Queue, js.JobSetId, js.fromMessageId)
			if errClear != nil {
				log.WithError(errClear).Error("could not clear subscription error from job set table")
			}
			currentJobId := api.JobIdFromApiEvent(msg.Message)
			jobStatus := EventsToJobResponse(*msg.Message)
			if jobStatus != nil {
				log.WithFields(requestFields).Infof("fromMessageId: %s JobId: %s State: %s", js.fromMessageId, currentJobId, jobStatus.GetState().String())
				jobStatus := repository.NewJobStatus(js.Queue, js.JobSetId, currentJobId, *jobStatus)
				err := js.sqlJobService.UpdateJobServiceDb(js.ctx, jobStatus)
				if err != nil {
					log.WithError(err).Error("could not update job status, retrying")
					nextRecv = time.After(5 * time.Second)
					continue
				}
			} else {
				log.WithFields(requestFields).Debugf("JobId: %s Message: %v", currentJobId, msg.Message)
			}
			// advance the message id for next loop
			js.fromMessageId = msg.GetId()
			// Nanosecond, essentially go again now.
			nextRecv = time.After(1 * time.Nanosecond)
		}
	}
}
