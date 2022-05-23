package serving

import (
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/eventapi/eventdb"
	"github.com/G-Research/armada/internal/eventapi/model"

	log "github.com/sirupsen/logrus"
)

// SubscriptionManager lets callers subscribe to channels of events in an efficient manner
type SubscriptionManager interface {
	Subscribe(jobset int64, fromOffset int64) *model.EventSubscription
	Unsubscribe(subscriptionId int64)
}

type internalSubscription struct {
	subscriptionId int64
	jobset         int64
	channel        chan []*model.EventRow
}

type DefaultSubscriptionManager struct {
	offsets           SequenceManager
	db                *eventdb.EventDb
	pollPeriod        time.Duration
	maxFetchSize      int
	batchedChannel    chan *model.EventRequest // A channel where queries are batched together which increases overall throughput at the cost of latency
	catchupChannel    chan *model.EventRequest // A channel for queries to be made in a low latency manner at the cost of overall throughput
	subscriptionIndex int64
	subscriptionsById *sync.Map
	clock             clock.Clock
}

// EventDbRO is a Simplified view of EventsDb which helps testing
type EventDbRO interface {
	GetEvents(requests []*model.EventRequest, limit int) ([]*model.EventResponse, error)
}

// NewSubscriptionManager returns a DefaultSubscriptionManager that can fetch events from postgres and manage subscription requests for new data
func NewSubscriptionManager(sequenceManager SequenceManager, db EventDbRO, maxBatchSize int, maxTimeout time.Duration, pollPeriod time.Duration, queryConcurrency int, maxFetchSize int, clock clock.Clock) *DefaultSubscriptionManager {

	sm := DefaultSubscriptionManager{
		offsets:           sequenceManager,
		pollPeriod:        pollPeriod,
		batchedChannel:    make(chan *model.EventRequest),
		catchupChannel:    make(chan *model.EventRequest),
		subscriptionsById: &sync.Map{},
		subscriptionIndex: 0,
		clock:             clock,
	}

	batches := BatchQueries(sm.batchedChannel, maxBatchSize, maxTimeout)

	// Have n readers who each read batches from the queue and execute db queries
	// This allows us to limit how many concurrent db queries we can make and thus
	// avoid overloading the database
	for i := 0; i < queryConcurrency; i++ {

		fetchFromDb := func(req []*model.EventRequest) {
			start := time.Now()
			events, err := db.GetEvents(req, maxFetchSize)
			taken := time.Now().Sub(start).Milliseconds()
			if taken > 500 {
				log.Warnf("Slow query: Fetched %d events from the database in %dms", len(events), taken)
			} else {
				log.Debugf("Fetched %d events from the database in %dms", len(events), taken)
			}
			if err == nil {
				for _, e := range events {
					sub, present := sm.subscriptionsById.Load(e.SubscriptionId)
					if present {
						sub.(*internalSubscription).channel <- e.Events
					}
				}
			} else {
				log.Warnf("Error retrieving events from db: %+v", err)
			}
		}

		// This function processes single requests without batching which gives lower throughput but lower latency
		// We use it when streams have got behind and need to catch up
		go func() {
			for req := range sm.catchupChannel {
				fetchFromDb([]*model.EventRequest{req})
			}
		}()

		// This function processes batches at high throughput but high latency
		// We use it when streams are caught up and are periodically checking for updates
		go func() {
			for req := range batches {
				fetchFromDb(req)
			}
		}()
	}
	return &sm
}

// Subscribe returns an EventSubscription which consists of a stream of events along with a subscription id
// Callers should pass back the subscriptionId when they want to Unsubscribe.
func (sm *DefaultSubscriptionManager) Subscribe(jobset int64, fromOffset int64) *model.EventSubscription {
	sub := sm.createInternalSubscription(jobset)
	currentOffset := fromOffset

	externalSubscription := &model.EventSubscription{
		SubscriptionId: sub.subscriptionId,
		Channel:        make(chan []*model.EventRow),
	}

	// In an ideal world we would be catching up if the fromOffset is less than the current offset.
	// Unfortunately most of the clients use rest which means they would alsw
	catchingUp := fromOffset == -1

	cond := sync.NewCond(&sync.Mutex{})

	// Spin up a goroutine that will request more data evey poll period
	// It wil short circuit if there are no updates available or if the previous request
	// has not yet returned
	go func() {
		for sm.hasSubscription(sub.subscriptionId) {
			start := sm.clock.Now()
			storedOffset, ok := sm.offsets.Get(jobset)
			if ok && storedOffset > currentOffset {
				// We've determined that there are new events available.  Make a request for data
				req := &model.EventRequest{SubscriptionId: sub.subscriptionId, Jobset: jobset, Sequence: currentOffset}
				if catchingUp {
					sm.catchupChannel <- req
				} else {
					sm.batchedChannel <- req
				}

				// Wait for the request to be serviced
				cond.L.Lock()
				cond.Wait()
				cond.L.Unlock()
			} else {
				// There was no data available so by definition we can't be catching up
				catchingUp = false
			}
			// If we've caught up then sleep until we next want to check results
			if !catchingUp {
				taken := sm.clock.Now().Sub(start)
				// A simple time.sleep would be simpler here, but less easy for testing
				select {
				case <-time.After(sm.pollPeriod - taken):
				}
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

// Unsubscribe frees up resources associated with the stream
func (sm *DefaultSubscriptionManager) Unsubscribe(subscriptionId int64) {
	sm.subscriptionsById.Delete(subscriptionId)
}

func (sm *DefaultSubscriptionManager) hasSubscription(subscriptionId int64) bool {
	_, ok := sm.subscriptionsById.Load(subscriptionId)
	return ok
}

func (sm *DefaultSubscriptionManager) createInternalSubscription(jobsetId int64) *internalSubscription {
	atomic.AddInt64(&sm.subscriptionIndex, 1)
	sub := &internalSubscription{
		subscriptionId: sm.subscriptionIndex,
		jobset:         jobsetId,
		channel:        make(chan []*model.EventRow),
	}
	sm.subscriptionsById.Store(sub.subscriptionId, sub)
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
