package eventapi

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/eventapi/postgres"
)

type internalSubscription struct {
	subscriptionId int64
	jobset         int64
	channel        chan []*Event
}

type EventRequest struct {
	SubscriptionId int64
	Jobset         int64
	Sequence       int64
}

type EventResponse struct {
	SubscriptionId int64
	Events         []*Event
}

type SubscriptionManager struct {
	offsets           *OffsetManager
	db                *postgres.EventDb
	pollPeriod        time.Duration
	requestChannel    chan *EventRequest
	subscriptionIndex int64
	subscriptionsById map[int64]*internalSubscription
	subscriptionMutex sync.Mutex
}

// NewSubscriptionManager returns a SubscriptionManager that can fetch events from postgres and manage subscription requests for new data
func NewSubscriptionManager(offsets *OffsetManager, db *postgres.EventDb, maxBatchSize int, maxTimeout time.Duration, pollPeriod time.Duration, queryConcurrency int) *SubscriptionManager {

	sm := SubscriptionManager{
		offsets:           offsets,
		pollPeriod:        pollPeriod,
		requestChannel:    make(chan *EventRequest),
		subscriptionsById: make(map[int64]*internalSubscription),
		subscriptionIndex: 0,
	}

	batches := BatchQueries(sm.requestChannel, maxBatchSize, maxTimeout)

	// Have n readers who each read batches from the queue and execute db queries
	// This allows us to limit how many concurrent db queries we can make and thus
	// avoid overloading the database
	for i := 0; i < queryConcurrency; i++ {
		go func() {
			for req := range batches {
				events, err := db.GetEvents(req)
				if err == nil {
					for _, e := range events {
						sm.subscriptionMutex.Lock()
						sub, present := sm.subscriptionsById[e.SubscriptionId]
						sm.subscriptionMutex.Unlock()
						if present {
							sub.channel <- e.Events
						}
					}
				} else {
					log.Warnf("Error retrieving events from db: %+v", err)
				}
			}
		}()
	}
	return &sm
}

func (sm *SubscriptionManager) Subscribe(jobset int64) *EventSubscription {
	sub := sm.createInternalSubscription(jobset)
	var currentOffset int64 = -1
	requestInFlight := false

	externalSubscripton := &EventSubscription{
		SubscriptionId: sub.subscriptionId,
		Channel:        make(chan []*Event),
	}

	// Spin up a goroutine that will request more data evey poll period
	// It wil short circuit if there are no updates available or if the previous request
	// has not yet returned
	go func() {
		for sm.hasSubscription(sub.subscriptionId) {
			storedOffset, ok := sm.offsets.Get(jobset)
			if ok && (storedOffset > currentOffset && !requestInFlight) {
				requestInFlight = true
				req := &EventRequest{SubscriptionId: sub.subscriptionId, Jobset: jobset, Sequence: currentOffset}
				sm.requestChannel <- req
			}
			time.Sleep(sm.pollPeriod)
		}
		close(sm.requestChannel)
	}()

	// Go routine that will publish events to the external channel
	go func() {
		for data := range sub.channel {
			if len(data) > 0 {
				currentOffset = data[len(data)-1].Sequence
			}
			requestInFlight = false
			externalSubscripton.Channel <- data
		}
	}()

	return externalSubscripton
}

func (sm *SubscriptionManager) Unsubscribe(subscriptionId int64) {
	sm.subscriptionMutex.Lock()
	delete(sm.subscriptionsById, subscriptionId)
	sm.subscriptionMutex.Lock()
}

func (sm *SubscriptionManager) hasSubscription(subscriptionId int64) bool {
	sm.subscriptionMutex.Lock()
	_, ok := sm.subscriptionsById[subscriptionId]
	sm.subscriptionMutex.Lock()
	return ok
}

func (sm *SubscriptionManager) createInternalSubscription(jobsetId int64) *internalSubscription {
	sm.subscriptionMutex.Lock()
	sub := &internalSubscription{
		subscriptionId: sm.subscriptionIndex,
		jobset:         jobsetId,
		channel:        make(chan []*Event),
	}
	sm.subscriptionIndex++
	sm.subscriptionsById[sub.subscriptionId] = sub
	sm.subscriptionMutex.Unlock()
	return sub
}

// BatchQueries Reads from a channel of UpdateRequests and batches them up, waiting for either maxItems to be present in the batch or
// maxTimeout to have elapsed since the batch was started (whichever occurs first).  Once the condition has been met, the
// batch is released and the next batch is started.
func BatchQueries(values <-chan *EventRequest, maxItems int, maxTimeout time.Duration) chan []*EventRequest {
	batches := make(chan []*EventRequest)

	go func() {
		defer close(batches)

		for keepGoing := true; keepGoing; {
			var batch []*EventRequest
			expire := time.After(maxTimeout)
			for {
				select {
				case value, ok := <-values:
					if !ok {
						keepGoing = false
						goto done
					}

					batch = append(batch, value)
					if len(batch) == maxItems {
						goto done
					}

				case <-expire:
					goto done
				}
			}

		done:
			if len(batch) > 0 {
				batches <- batch
			}
		}
	}()

	return batches
}
