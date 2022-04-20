package eventapi

import (
	"sync"
	"time"

	"github.com/G-Research/armada/internal/eventapi/model"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

type UpdateRequest struct {
	SubscriptionId uuid.UUID
	Jobset         uuid.UUID
	Sequence       int64
}

type UpdateResponse struct {
	SubscriptionId uuid.UUID
	Events         []model.Event
}

type EventSubscription struct {
	SubscriptionId uuid.UUID
	Channel        chan []model.Event
}

type EventManager struct {
	availableOffsets  *sync.Map
	pollPeriod        time.Duration
	requestChannel    chan UpdateRequest
	subscriptionsById *sync.Map
}

type internalSubscription struct {
	subscriptionId uuid.UUID
	jobset         uuid.UUID
	channel        chan []model.Event
}

// NewEventManager returns an event manager that can fetch events from postgres and manage subscription requests for new data
// db database pool
// maxBatchSize maximum number of requests that will be batched together in a single query
// maxTimeout maximum time that wil be waited before releasing a batch of requests
// pollPeriod period at which a given subscription will check if there are new events available
// queryConcurrency number of concurrent event queries we can have in flight at any one time
// offsetUpdates channel over which events will be sent, telling us that there is new data for a given jobset.
func NewEventManager(db *pgxpool.Pool, maxBatchSize int, maxTimeout time.Duration, pollPeriod time.Duration, queryConcurrency int, offsetUpdates chan model.EventUpdate) EventManager {

	// Set up a function to store latest available offsets
	availableOffsets := &sync.Map{}
	go func() {
		for update := range offsetUpdates {
			availableOffsets.Store(update.Jobset, int(update.Sequence))
		}
	}()

	em := EventManager{
		availableOffsets:  availableOffsets,
		pollPeriod:        pollPeriod,
		requestChannel:    make(chan UpdateRequest),
		subscriptionsById: &sync.Map{},
	}

	batches := BatchQueries(em.requestChannel, maxBatchSize, maxTimeout)

	// Have n readers who each read batches from the queue and execute db queries
	// This allows us to limit how many concurrent db queries we can make and thus
	// avoid overloading the database
	for i := 0; i < queryConcurrency; i++ {
		go func() {
			for req := range batches {
				events, err := GetEvents(db, req)
				if err == nil {
					for _, e := range events {
						sub, present := em.subscriptionsById.Load(e.SubscriptionId)
						if present {
							sub.(internalSubscription).channel <- e.Events
						}
					}
				} else {
					//TODO log context here and raise alert?
					log.Warnf("Error retrieving events from db: %v", err)
				}
			}
		}()
	}
	return em
}

func (m *EventManager) Subscribe(jobset uuid.UUID) EventSubscription {
	sub := m.createSubscription(jobset)
	var currentOffset int64 = -1
	requestInFlight := false

	externalSubscripton := EventSubscription{
		SubscriptionId: sub.subscriptionId,
		Channel:        make(chan []models.Event),
	}

	// Spin up a goroutine that will request more data evey poll period
	// It wil short circuit if there are no updates available or if the previous request
	// has not yet returned
	go func() {
		for {
			storedOffset := int64(m.getOffset(jobset))
			if storedOffset > currentOffset && !requestInFlight {
				requestInFlight = true
				req := UpdateRequest{SubscriptionId: sub.subscriptionId, Jobset: jobset, Sequence: currentOffset}
				m.requestChannel <- req
			}
			time.Sleep(m.pollPeriod)
		}
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

func (m *EventManager) createSubscription(jobset uuid.UUID) internalSubscription {
	sub := internalSubscription{
		subscriptionId: uuid.New(),
		jobset:         jobset,
		channel:        make(chan []models.Event),
	}
	m.subscriptionsById.Store(sub.subscriptionId, sub)
	return sub
}

func (m *EventManager) getOffset(id uuid.UUID) int {
	result, ok := m.availableOffsets.Load(id)
	if ok {
		return result.(int)
	} else {
		return -1
	}
}

// BatchQueries Reads from a channel of UpdateRequests and batches them up, waiting for either maxItems to be present in the batch or
// maxTimeout to have elapsed since the batch was started (whichever occurs first).  Once the condition has been met, the
// bath is released and the next batch is started.
// TODO: this should be done in a generic way and not tied to UpdateRequest
// TODO: Probably want to inject a clock so that we can test this
func BatchQueries(values <-chan UpdateRequest, maxItems int, maxTimeout time.Duration) chan []UpdateRequest {
	batches := make(chan []UpdateRequest)

	go func() {
		defer close(batches)

		for keepGoing := true; keepGoing; {
			var batch []UpdateRequest
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
