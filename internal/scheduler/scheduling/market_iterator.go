package scheduling

import (
	"container/heap"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
)

type MarketIteratorPQ struct {
	items               []*MarketIteratorPQItem
	previousResultCost  float64
	previousResultQueue string
}

type MarketBasedCandidateGangIterator struct {
	pool            string
	queueRepository fairness.QueueRepository
	// If true, this iterator only yields gangs where all jobs are evicted.
	onlyYieldEvicted bool
	// If, e.g., onlyYieldEvictedByQueue["A"] is true,
	// this iterator only yields gangs where all jobs are evicted for queue A.
	onlyYieldEvictedByQueue map[string]bool
	// Priority queue containing per-queue iterators.
	// Determines the order in which queues are processed.
	pq MarketIteratorPQ
}

func NewMarketCandidateGangIterator(
	pool string,
	queueRepository fairness.QueueRepository,
	iteratorsByQueue map[string]*QueuedGangIterator,
) (*MarketBasedCandidateGangIterator, error) {
	it := &MarketBasedCandidateGangIterator{
		pool:                    pool,
		queueRepository:         queueRepository,
		onlyYieldEvictedByQueue: make(map[string]bool),
		pq: MarketIteratorPQ{
			items: make([]*MarketIteratorPQItem, 0, len(iteratorsByQueue)),
		},
	}
	for queue, queueIt := range iteratorsByQueue {
		if _, err := it.updateAndPushPQItem(it.newPQItem(queue, queueIt)); err != nil {
			return nil, err
		}
	}
	return it, nil
}

func (it *MarketBasedCandidateGangIterator) newPQItem(queue string, queueIt *QueuedGangIterator) *MarketIteratorPQItem {
	return &MarketIteratorPQItem{
		queue: queue,
		it:    queueIt,
	}
}

func (it *MarketBasedCandidateGangIterator) GetAllocationForQueue(queue string) (internaltypes.ResourceList, bool) {
	q, ok := it.queueRepository.GetQueue(queue)
	if !ok {
		return internaltypes.ResourceList{}, false
	}
	return q.GetAllocation(), true
}

// Clear removes the first item in the iterator.
// If it.onlyYieldEvicted is true, any consecutive non-evicted jobs are also removed.
func (it *MarketBasedCandidateGangIterator) Clear() error {
	if it.pq.Len() == 0 {
		return nil
	}
	item := heap.Pop(&it.pq).(*MarketIteratorPQItem)
	item.it.Clear()
	it.pq.previousResultQueue = item.queue
	it.pq.previousResultCost = item.price
	if _, err := it.updateAndPushPQItem(item); err != nil {
		return err
	}
	return nil
}

func (it *MarketBasedCandidateGangIterator) Peek() (*schedulercontext.GangSchedulingContext, float64, error) {
	if it.pq.Len() == 0 {
		// No queued jobs left.
		return nil, 0.0, nil
	}
	first := it.pq.items[0]

	// The second return value here is the queue cost which we don't have here.
	// This is only used for metrics so should be fine.
	return first.gctx, 0.0, nil
}

func (it *MarketBasedCandidateGangIterator) updateAndPushPQItem(item *MarketIteratorPQItem) (bool, error) {
	if err := it.updatePQItem(item); err != nil {
		return false, err
	}
	if item.gctx == nil {
		return false, nil
	}
	heap.Push(&it.pq, item)
	return true, nil
}

func (it *MarketBasedCandidateGangIterator) updatePQItem(item *MarketIteratorPQItem) error {
	item.gctx = nil
	item.price = 0
	gctx, err := item.it.Peek()
	if err != nil {
		return err
	}
	if gctx == nil {
		return nil
	}

	job := gctx.JobSchedulingContexts[0].Job
	item.gctx = gctx
	item.price = job.GetBidPrice(it.pool)
	item.queued = job.Queued()
	if !job.Queued() && job.LatestRun() != nil {
		item.runtime = time.Now().UnixNano() - job.LatestRun().Created()
	} else {
		item.runtime = 0
	}
	item.submittedTime = job.SubmitTime().UnixNano()

	return nil
}

func (it *MarketBasedCandidateGangIterator) OnlyYieldEvicted() error {
	if !it.onlyYieldEvicted {
		newPq := make([]*MarketIteratorPQItem, 0, it.pq.Len())

		for _, item := range it.pq.items {
			item.it.OnlyYieldEvicted()
			err := it.updatePQItem(item)
			if err != nil {
				return err
			}

			// Do not add items that no longer have jobs
			if item.gctx != nil {
				newPq = append(newPq, item)
			}
		}
		it.pq.items = newPq

		// Sort heap now items have been updated
		heap.Init(&it.pq)
	}
	it.onlyYieldEvicted = true
	return nil
}

func (it *MarketBasedCandidateGangIterator) OnlyYieldEvictedForQueue(queue string) error {
	if !it.onlyYieldEvicted && !it.onlyYieldEvictedByQueue[queue] {
		for _, item := range it.pq.items {
			if item.queue == queue {
				item.it.OnlyYieldEvicted()
				err := it.updatePQItem(item)
				if err != nil {
					return err
				}
				if item.gctx == nil {
					heap.Remove(&it.pq, item.index)
				} else {
					heap.Fix(&it.pq, item.index)
				}
				break
			}
		}
	}
	it.onlyYieldEvictedByQueue[queue] = true
	return nil
}

type MarketIteratorPQItem struct {
	queue         string
	price         float64
	runtime       int64
	queued        bool
	submittedTime int64
	// Most recent value produced by the iterator.
	// Cached here to avoid repeating scheduling checks unnecessarily.
	gctx *schedulercontext.GangSchedulingContext
	// Iterator for this queue.
	it *QueuedGangIterator
	// The index of the item in the heap.
	// maintained by the heap.Interface methods.
	index int
}

func (pq *MarketIteratorPQ) Len() int { return len(pq.items) }

func (pq *MarketIteratorPQ) Less(i, j int) bool {
	// First by price
	if pq.items[i].price != pq.items[j].price {
		return pq.items[i].price > pq.items[j].price
	}

	// If price is the same, order running jobs over queued jobs
	// This prevents preempting running jobs to run jobs of the same price
	if pq.items[i].queued != pq.items[j].queued {
		return !pq.items[i].queued
	}

	// This section causes the iterator to round robin between queues bidding the same price
	// We want this as many queues may bid the same price and ideally the queues would get an even distribution of jobs
	// Without this the first queue alphabetically would get all their jobs considered first (assuming all queues bid the same price)
	if pq.items[i].price == pq.previousResultCost {
		if pq.items[i].queue > pq.previousResultQueue && pq.items[j].queue > pq.previousResultQueue {
			return pq.items[i].queue < pq.items[j].queue
		}
		if pq.items[i].queue > pq.previousResultQueue || pq.items[j].queue == pq.previousResultQueue {
			return true
		}
		if pq.items[j].queue > pq.previousResultQueue || pq.items[i].queue == pq.previousResultQueue {
			return false
		}
	}

	// Then by runtime (highest first)
	if pq.items[i].runtime != pq.items[j].runtime {
		return pq.items[i].runtime > pq.items[j].runtime
	}

	// Then by submitted time (lowest first)
	if pq.items[i].submittedTime != pq.items[j].submittedTime {
		return pq.items[i].submittedTime < pq.items[j].submittedTime
	}

	// Tie-break by queue name.
	return pq.items[i].queue < pq.items[j].queue
}

func (pq *MarketIteratorPQ) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

func (pq *MarketIteratorPQ) Push(x any) {
	n := pq.Len()
	item := x.(*MarketIteratorPQItem)
	item.index = n
	pq.items = append(pq.items, item)
}

func (pq *MarketIteratorPQ) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.items = old[0 : n-1]
	return item
}
