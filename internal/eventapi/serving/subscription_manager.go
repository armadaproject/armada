package serving

import (
	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/eventapi/model"
	"k8s.io/apimachinery/pkg/util/clock"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type internalSubscription struct {
	subscriptionId int64
	jobset         int64
	channel        chan []*model.EventRow
}

type SubscriptionManager struct {
	offsets           SequenceManager
	db                *eventdb.EventDb
	pollPeriod        time.Duration
	maxFetchSize      int
	batchedChannel    chan *model.EventRequest
	catchupChannel    chan *model.EventRequest
	subscriptionIndex int64
	subscriptionsById map[int64]*internalSubscription
	subscriptionMutex sync.RWMutex
	clock             clock.Clock
}

type EventDbRO interface {
	GetEvents(requests []*model.EventRequest, limit int) ([]*model.EventResponse, error)
}

// NewSubscriptionManager returns a SubscriptionManager that can fetch events from postgres and manage subscription requests for new data
func NewSubscriptionManager(sequenceManager SequenceManager, db EventDbRO, maxBatchSize int, maxTimeout time.Duration, pollPeriod time.Duration, queryConcurrency int, maxFetchSize int, clock clock.Clock) *SubscriptionManager {

	sm := SubscriptionManager{
		offsets:           sequenceManager,
		pollPeriod:        pollPeriod,
		batchedChannel:    make(chan *model.EventRequest),
		catchupChannel:    make(chan *model.EventRequest),
		subscriptionsById: make(map[int64]*internalSubscription),
		subscriptionIndex: 0,
		subscriptionMutex: sync.RWMutex{},
		clock:             clock,
	}

	batches := BatchQueries(sm.batchedChannel, maxBatchSize, maxTimeout)

	// Have n readers who each read batches from the queue and execute db queries
	// This allows us to limit how many concurrent db queries we can make and thus
	// avoid overloading the database
	for i := 0; i < queryConcurrency; i++ {

		// This function processes single requests without batching ast lower throughput but lower latency
		// We use it when streams have got behind and need to catch up
		go func() {
			for req := range sm.catchupChannel {
				events, err := db.GetEvents([]*model.EventRequest{req}, maxFetchSize)
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

		// This function processes batches at high throughput but high latency
		// We use it when streams are caught up and are periodically checking for updates
		go func() {
			for req := range batches {
				events, err := db.GetEvents(req, maxFetchSize)
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

func (sm *SubscriptionManager) Subscribe(jobset int64, fromOffset int64) *model.EventSubscription {
	sub := sm.createInternalSubscription(jobset)
	currentOffset := fromOffset

	externalSubscription := &model.EventSubscription{
		SubscriptionId: sub.subscriptionId,
		Channel:        make(chan []*model.EventRow),
	}

	// In an ideal world we would be catching up if the fromOffset eas less than the current offset.
	// Unfortunately most of the clients use rest which means they would alsw
	catchingUp := fromOffset == 0

	cond := sync.NewCond(&sync.Mutex{})

	// Spin up a goroutine that will request more data evey poll period
	// It wil short circuit if there are no updates available or if the previous request
	// has not yet returned
	go func() {
		for sm.hasSubscription(sub.subscriptionId) {
			start := sm.clock.Now()
			storedOffset, ok := sm.offsets.Get(jobset)
			if ok && storedOffset > currentOffset {
				req := &model.EventRequest{SubscriptionId: sub.subscriptionId, Jobset: jobset, Sequence: currentOffset}
				if catchingUp {
					sm.catchupChannel <- req
				} else {
					sm.batchedChannel <- req
				}
			}
			cond.L.Lock()
			cond.Wait()
			cond.L.Unlock()
			// If we've caught up then sleep until we next want to check results
			if !catchingUp {
				taken := sm.clock.Now().Sub(start)
				sm.clock.Sleep(sm.pollPeriod - taken)
			}
		}
		close(sub.channel)
	}()

	// Go routine that will publish events to the external channel
	go func() {
		for data := range sub.channel {
			cond.L.Lock()
			cond.Signal()
			cond.L.Unlock()
			if len(data) > 0 {
				receivedOffset := data[len(data)-1].SeqNo
				if catchingUp {
					// If we are in catch up mode, see if we have caught up
					storedOffset, _ := sm.offsets.Get(jobset)
					catchingUp = storedOffset > receivedOffset
				}
				currentOffset = receivedOffset
			}
			externalSubscription.Channel <- data
		}
	}()
	return externalSubscription
}

func (sm *SubscriptionManager) Unsubscribe(subscriptionId int64) {
	sm.subscriptionMutex.Lock()
	defer sm.subscriptionMutex.Unlock()
	delete(sm.subscriptionsById, subscriptionId)
}

func (sm *SubscriptionManager) hasSubscription(subscriptionId int64) bool {
	sm.subscriptionMutex.Lock()
	defer sm.subscriptionMutex.Unlock()
	_, ok := sm.subscriptionsById[subscriptionId]
	return ok
}

func (sm *SubscriptionManager) createInternalSubscription(jobsetId int64) *internalSubscription {
	sm.subscriptionMutex.Lock()
	defer sm.subscriptionMutex.Unlock()
	sub := &internalSubscription{
		subscriptionId: sm.subscriptionIndex,
		jobset:         jobsetId,
		channel:        make(chan []*model.EventRow),
	}
	sm.subscriptionIndex++
	sm.subscriptionsById[sub.subscriptionId] = sub
	return sub
}

// BatchQueries Reads from a channel of UpdateRequests and batches them up, waiting for either maxItems to be present in the batch or
// maxTimeout to have elapsed since the batch was started (whichever occurs first).  Once the condition has been met, the
// batch is released and the next batch is started.
func BatchQueries(values <-chan *model.EventRequest, maxItems int, maxTimeout time.Duration) chan []*model.EventRequest {
	batches := make(chan []*model.EventRequest)

	go func() {
		defer close(batches)

		for keepGoing := true; keepGoing; {
			var batch []*model.EventRequest
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
