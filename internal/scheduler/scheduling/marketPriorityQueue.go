package scheduling

import (
	"container/heap"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
)

type MarketIteratorPQ struct {
	items []*MarketIteratorPQItem
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
	// TODO implement me
	panic("implement me")
}

// Clear removes the first item in the iterator.
// If it.onlyYieldEvicted is true, any consecutive non-evicted jobs are also removed.
func (it *MarketBasedCandidateGangIterator) Clear() error {
	if it.pq.Len() == 0 {
		return nil
	}
	item := heap.Pop(&it.pq).(*MarketIteratorPQItem)
	if err := item.it.Clear(); err != nil {
		return err
	}
	if _, err := it.updateAndPushPQItem(item); err != nil {
		return err
	}

	// If set to only yield evicted gangs, drop any queues for which the next gang is non-evicted here.
	// We assume here that all evicted jobs appear before non-evicted jobs in the queue.
	// Hence, it's safe to drop a queue if the first job is non-evicted.
	if it.onlyYieldEvicted {
		for it.pq.Len() > 0 && !it.pq.items[0].gctx.AllJobsEvicted {
			heap.Pop(&it.pq)
		}
	} else {
		// Same check as above on a per-queue basis.
		for it.pq.Len() > 0 && it.onlyYieldEvictedByQueue[it.pq.items[0].gctx.Queue] && !it.pq.items[0].gctx.AllJobsEvicted {
			heap.Pop(&it.pq)
		}
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
	if it.onlyYieldEvicted && !item.gctx.AllJobsEvicted {
		return false, nil
	}
	if it.onlyYieldEvictedByQueue[item.gctx.Queue] && !item.gctx.AllJobsEvicted {
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
	item.price = job.BidPrice()
	if !job.Queued() && job.LatestRun() != nil {
		item.runtime = time.Now().UnixNano() - job.LatestRun().Created()
	} else {
		item.runtime = 0
	}
	item.submittedTime = job.SubmitTime().UnixNano()

	return nil
}

func (it *MarketBasedCandidateGangIterator) OnlyYieldEvicted() {
	it.onlyYieldEvicted = true
}

func (it *MarketBasedCandidateGangIterator) OnlyYieldEvictedForQueue(queue string) {
	it.onlyYieldEvictedByQueue[queue] = true
}

type MarketIteratorPQItem struct {
	queue         string
	price         float64
	runtime       int64
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
		return pq.items[i].price < pq.items[j].price
	}

	// Then by runtime (highest first)
	if pq.items[i].runtime != pq.items[j].runtime {
		return pq.items[i].runtime < pq.items[j].runtime
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
