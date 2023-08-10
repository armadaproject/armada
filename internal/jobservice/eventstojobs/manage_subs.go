package eventstojobs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/armadaproject/armada/internal/jobservice/events"
	"github.com/armadaproject/armada/internal/jobservice/repository"
	"github.com/armadaproject/armada/pkg/api"
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
	ctx context.Context

	sqlJobService repository.SQLJobService
	eventReader   events.JobEventReader

	subscriptions map[repository.JobSetKey]*JobSetSubscription
	mutex         sync.Mutex

	newSubChan  <-chan *repository.SubscribedTuple
	subDoneChan chan *repository.JobSetKey

	subTimeout time.Duration
}

func NewJobSetSubscriptionExecutor(ctx context.Context,
	eventReader events.JobEventReader,
	sqlJobService repository.SQLJobService,
	newSubChan <-chan *repository.SubscribedTuple,
	subTimeout time.Duration,
) *JobSetSubscriptionExecutor {
	return &JobSetSubscriptionExecutor{
		ctx:           ctx,
		eventReader:   eventReader,
		sqlJobService: sqlJobService,
		subscriptions: make(map[repository.JobSetKey]*JobSetSubscription),
		newSubChan:    newSubChan,
		subDoneChan:   make(chan *repository.JobSetKey, 1000),
		subTimeout:    subTimeout,
	}
}

func (jse *JobSetSubscriptionExecutor) Manage() {
	go jse.ScanForMissingSubscriptions()

	// Main {un}subscribe loop.
	for {
		select {
		case <-jse.ctx.Done():
			log.Debug("Context is done.")
			return
		case newSubInfo := <-jse.newSubChan:
			jse.addSubscription(newSubInfo)
			err := jse.launchSubscriber(&newSubInfo.JobSetKey)
			if err != nil {
				log.WithError(err).Error("JobSet subscribe error")
			}
		case subDoneKey := <-jse.subDoneChan:
			log.Infof("Removing subscription on %s/%s", subDoneKey.Queue, subDoneKey.JobSetId)
			err := jse.removeSubscription(subDoneKey)
			if err != nil {
				log.WithError(err).Error("JobSet unsubscribe error")
			}

		}
	}
}

// Looks for subscriptions that show as active in the DB but are not active
// in this executor.
func (jse *JobSetSubscriptionExecutor) ScanForMissingSubscriptions() {
	nextScan := time.After(1 * time.Nanosecond)
	for {
		select {
		case <-jse.ctx.Done():
			return
		case <-nextScan:
			scanStart := time.Now()

			subscriptions, err := jse.sqlJobService.GetSubscribedJobSets(jse.ctx)
			if err != nil {
				log.WithError(err).Error("error getting subscribed job sets")
				nextScan = time.After(60 * time.Second)
				continue
			}

			for _, sub := range subscriptions {
				// Adding an already existing subscription is a no-op.
				jse.addSubscription(&sub)
			}

			scanEnd := time.Now()
			duration := scanEnd.Sub(scanStart)
			log.Infof("Scan for missing subs took %s", duration.String())
			log.Infof("There are %d active jobset subscriptions", jse.NumActiveSubscriptions())

			nextScan = time.After(60 * time.Second)
		}
	}
}

func (jse *JobSetSubscriptionExecutor) addSubscription(sub *repository.SubscribedTuple) {
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

		err := jse.sqlJobService.SubscribeJobSet(jse.ctx, sub.Queue, sub.JobSetId, sub.FromMessageId)
		if err != nil {
			log.Errorf("Could not add subscription on %s/%s to DB: %s", sub.Queue, sub.JobSetId, err.Error())
		}
	}
}

func (jse *JobSetSubscriptionExecutor) launchSubscriber(key *repository.JobSetKey) error {
	jse.mutex.Lock()
	sub, ok := jse.subscriptions[*key]
	jse.mutex.Unlock()

	if ok {
		go sub.Subscribe() //nolint:errcheck
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

func NewJobSetSubscription(ctx context.Context,
	eventReader events.JobEventReader,
	subInfo *repository.SubscribedTuple,
	subTimeout time.Duration,
	subDoneChan chan<- *repository.JobSetKey,
	sqlJobService repository.SQLJobService,
) *JobSetSubscription {
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
		log.WithFields(requestFields).Debugf("Sent message to subDoneChan")
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
		return err
	}

	log.WithFields(requestFields).Debug("Got stream")

	g, _ := errgroup.WithContext(js.ctx)

	// Subscription status check ticker.
	timeout := time.NewTicker(js.subTimeout)

	g.Go(func() error {
		for {
			select {
			case <-js.ctx.Done():
				return nil
			case <-timeout.C:
				log.WithFields(requestFields).Debug("JobSetSubscription.Subscribe checking subscription status")
				// Stream is created with *our* context, therefore if we cancel, stream.Recv() should bail out too.
				unsub, err := js.sqlJobService.CheckToUnSubscribe(js.ctx, js.Queue, js.JobSetId, js.subTimeout)
				if err != nil {
					log.WithFields(requestFields).WithError(err).Error("IsJobSetSubscribed error")
				}
				// We should unsubscribe
				if unsub {
					log.WithFields(requestFields).Info("subscription done")
					js.cancel()
					return nil
				}
			}
		}
	})

	// Nanosecond, zero wait essentially.
	g.Go(func() error {
		nextRecv := time.After(1 * time.Nanosecond)

		defer func() {
			js.cancel()
			log.WithFields(requestFields).Debugf("Called cancel")
		}()

		// this loop will run until the context is canceled
		for {
			select {
			case <-js.ctx.Done():
				log.WithFields(requestFields).Debug("context is done")
				return nil
			case <-nextRecv:
				msg, err := stream.Recv()
				if err != nil {
					if strings.Contains(err.Error(), io.EOF.Error()) {
						log.WithFields(requestFields).Info("Reached stream end for JobSetSubscription")
						return nil
					} else if strings.Contains(err.Error(), "context canceled") {
						// The select case will handle context being done/canceled.
						continue
					}

					log.WithFields(requestFields).WithError(err).Error("could not obtain job set event message, retrying")
					settingSubscribeErr := js.sqlJobService.SetSubscriptionError(
						js.ctx, js.Queue, js.JobSetId, err.Error(), js.fromMessageId)
					if settingSubscribeErr != nil {
						log.WithFields(requestFields).WithError(settingSubscribeErr).Error("could not set error field in job set table")
					}
					nextRecv = time.After(5 * time.Second)
					continue
				}

				errClear := js.sqlJobService.AddMessageIdAndClearSubscriptionError(
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
					}).Debug("Got event")
					jobStatus := repository.NewJobStatus(js.Queue, js.JobSetId, currentJobId, *jobStatus)
					err := js.sqlJobService.UpdateJobServiceDb(js.ctx, jobStatus)
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
