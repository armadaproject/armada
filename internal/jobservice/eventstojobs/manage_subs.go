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
	"golang.org/x/sync/errgroup"
)

type JobSetSubscription struct {
	repository.JobSetKey

	fromMessageId string

	jobUpdater  repository.JobTableUpdater
	eventReader events.JobEventReader

	ctx    context.Context
	cancel context.CancelFunc

	subTimeout time.Duration

	subDoneChan chan<- *repository.JobSetKey
}

type JobSetSubscriptionExecutor struct {
	ctx context.Context

	jobUpdater  repository.JobTableUpdater
	eventReader events.JobEventReader

	subscriptions map[repository.JobSetKey]*JobSetSubscription
	mutex         sync.Mutex

	newSubChan  <-chan *repository.JobSetSubscriptionInfo
	subDoneChan chan *repository.JobSetKey

	subTimeout time.Duration
}

func NewJobSetSubscriptionExecutor(ctx context.Context,
	eventReader events.JobEventReader,
	jobUpdater repository.JobTableUpdater,
	newSubChan <-chan *repository.JobSetSubscriptionInfo,
	subTimeout time.Duration,
) *JobSetSubscriptionExecutor {
	return &JobSetSubscriptionExecutor{
		ctx:           ctx,
		eventReader:   eventReader,
		jobUpdater:    jobUpdater,
		subscriptions: make(map[repository.JobSetKey]*JobSetSubscription),
		newSubChan:    newSubChan,
		subDoneChan:   make(chan *repository.JobSetKey),
		subTimeout:    subTimeout,
	}
}

func (jse *JobSetSubscriptionExecutor) Manage() {
	// Main {un}subscribe loop.
	go jse.ScanForMissingSubscriptions()

	for {
		select {
		case <-jse.ctx.Done():
			log.Infof("Context is done.")
			return
		case newSubInfo := <-jse.newSubChan:
			jse.addSubscription(newSubInfo)
			jse.launchSubscriber(&newSubInfo.JobSetKey)
		case subDoneKey := <-jse.subDoneChan:
			log.Infof("Removing subscription on %s/%s", subDoneKey.Queue, subDoneKey.JobSetId)
			jse.removeSubscription(subDoneKey)
		}
	}
}

func (jse *JobSetSubscriptionExecutor) ScanForMissingSubscriptions() {
	// TODO
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
			jse.jobUpdater)
	}

	err := jse.jobUpdater.SubscribeJobSet(jse.ctx, sub.Queue, sub.JobSetId, sub.FromMessageId)
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

	_, err := jse.jobUpdater.UnsubscribeJobSet(jse.ctx, key.Queue, key.JobSetId)
	return err
}

func (jse *JobSetSubscriptionExecutor) HasSubscription(key *repository.JobSetKey) bool {
	jse.mutex.Lock()
	defer jse.mutex.Unlock()

	_, ok := jse.subscriptions[*key]
	return ok
}

func (jse *JobSetSubscriptionExecutor) NumActiveSubscriptions() int {
	jse.mutex.Lock()
	defer jse.mutex.Unlock()

	return len(jse.subscriptions)
}

func NewJobSetSubscription(ctx context.Context, eventReader events.JobEventReader, subInfo *repository.JobSetSubscriptionInfo, subTimeout time.Duration, subDoneChan chan<- *repository.JobSetKey, jobUpdater repository.JobTableUpdater) *JobSetSubscription {
	newCtx, cancel := context.WithCancel(ctx)
	return &JobSetSubscription{
		ctx:           newCtx,
		cancel:        cancel,
		eventReader:   eventReader,
		JobSetKey:     subInfo.JobSetKey,
		fromMessageId: subInfo.FromMessageId,
		subTimeout:    subTimeout,
		subDoneChan:   subDoneChan,
		jobUpdater:    jobUpdater,
	}
}

func (js *JobSetSubscription) Subscribe() error {
	requestFields := log.Fields{
		"job_set_id":      js.JobSetId,
		"queue":           js.Queue,
		"from_message_id": js.fromMessageId,
	}

	defer func() {
		js.subDoneChan <- &repository.JobSetKey{
			Queue:    js.Queue,
			JobSetId: js.JobSetId,
		}
	}()

	log.WithFields(requestFields).Debugf("Calling GetJobEventMessage")

	stream, err := js.eventReader.GetJobEventMessage(js.ctx, &api.JobSetRequest{
		Id:            js.JobSetId,
		Queue:         js.Queue,
		Watch:         true,
		FromMessageId: js.fromMessageId,
	})
	if err != nil {
		log.WithFields(requestFields).WithError(err).Error("error from GetJobEventMessage")
		js.cancel()
		return err
	}

	log.WithFields(requestFields).Debug("Got stream")

	g, _ := errgroup.WithContext(js.ctx)

	// Subscription status check ticker.
	timeout := time.NewTicker(js.subTimeout)

	g.Go(func() error {
		select {
		case <-js.ctx.Done():
			return nil
		case <-timeout.C:
			log.WithFields(requestFields).Infof("JobSetSubscription.Subscribe checking subscription status")
			// Stream is created with *our* context, therefore if we cancel, stream.Recv() should bail out too.
			jobSetFound, _, err := js.jobUpdater.IsJobSetSubscribed(js.ctx, js.Queue, js.JobSetId)
			if err != nil {
				log.WithFields(requestFields).WithError(err).Error("IsJobSetSubscribed error")
			}
			// We're no longer subscribed.
			if !jobSetFound {
				log.WithFields(requestFields).Info("subscription done")
				js.cancel()
				return nil
			}

		}
		return nil
	})

	// Nanosecond, zero wait essentially.
	g.Go(func() error {
		nextRecv := time.After(1 * time.Nanosecond)

		// this loop will run until the context is canceled
		for {
			select {
			case <-js.ctx.Done():
				log.WithFields(requestFields).Infof("context is done")
				return nil
			case <-nextRecv:
				msg, err := stream.Recv()
				if err != nil {
					if errors.Is(err, io.EOF) {
						log.WithFields(requestFields).Info("Reached stream end for JobSetSubscription")
						return nil
					} else if strings.Contains(err.Error(), "context canceled") {
						// The select case will handle context being done/canceled.
						continue
					}

					log.WithFields(requestFields).WithError(err).Error("could not obtain job set event message, retrying")
					settingSubscribeErr := js.jobUpdater.SetSubscriptionError(
						js.ctx, js.Queue, js.JobSetId, err.Error(), js.fromMessageId)
					if settingSubscribeErr != nil {
						log.WithFields(requestFields).WithError(settingSubscribeErr).Error("could not set error field in job set table")
					}
					nextRecv = time.After(5 * time.Second)
					continue
				}

				errClear := js.jobUpdater.AddMessageIdAndClearSubscriptionError(
					js.ctx, js.Queue, js.JobSetId, js.fromMessageId)
				if errClear != nil {
					log.WithFields(requestFields).WithError(errClear).Error("could not clear subscription error from job set table")
				}
				currentJobId := api.JobIdFromApiEvent(msg.Message)
				jobStatus := EventsToJobResponse(*msg.Message)
				if jobStatus != nil {
					log.WithFields(requestFields).WithFields(log.Fields{
						"job_id":     currentJobId,
						"job_status": jobStatus.GetState().String(),
					}).Info("Got event")
					jobStatus := repository.NewJobStatus(js.Queue, js.JobSetId, currentJobId, *jobStatus)
					err := js.jobUpdater.UpdateJobServiceDb(js.ctx, jobStatus)
					if err != nil {
						log.WithFields(requestFields).WithError(err).Error("could not update job status, retrying")
						nextRecv = time.After(5 * time.Second)
						continue
					}
				} else {
					log.WithFields(requestFields).WithFields(log.Fields{
						"job_id":  currentJobId,
						"message": msg.Message,
					}).Debug("Got non-status event")
				}
				// advance the message id for next loop
				js.fromMessageId = msg.GetId()
				requestFields["from_message_id"] = js.fromMessageId
				// Nanosecond, essentially go again now.
				nextRecv = time.After(1 * time.Nanosecond)
			}
		}
	})

	return g.Wait()
}
