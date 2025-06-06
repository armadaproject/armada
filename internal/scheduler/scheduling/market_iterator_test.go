package scheduling

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestMarketIteratorPQ_Ordering(t *testing.T) {
	queueA := &MarketIteratorPQItem{queue: "A", price: 3, runtime: 10, submittedTime: 10}
	queueB := &MarketIteratorPQItem{queue: "B", price: 2, runtime: 10, submittedTime: 10}
	queueC := &MarketIteratorPQItem{queue: "C", price: 2, runtime: 8, submittedTime: 10}
	queueD := &MarketIteratorPQItem{queue: "D", price: 2, runtime: 8, submittedTime: 5}
	queueE := &MarketIteratorPQItem{queue: "E", price: 1, runtime: 8, submittedTime: 10}
	queueF := &MarketIteratorPQItem{queue: "F", price: 1, runtime: 8, submittedTime: 10}
	pq := &MarketIteratorPQ{items: []*MarketIteratorPQItem{queueF, queueE, queueD, queueC, queueB, queueA}}

	sort.Sort(pq)
	expectedOrder := []*MarketIteratorPQItem{queueA, queueB, queueD, queueC, queueE, queueF}
	assert.Equal(t, expectedOrder, pq.items)
}

func TestMarketBasedCandidateGangIterator_RoundRobin(t *testing.T) {
	sctx := createSchedulingContext(t)

	queueAIt := createJobIterator(sctx, "A", 600, 600, 400)
	queueBIt := createJobIterator(sctx, "B", 600, 600, 600)
	queueCIt := createJobIterator(sctx, "C", 600, 600, 300)
	iteratorsByQueue := map[string]*QueuedGangIterator{"A": queueAIt, "B": queueBIt, "C": queueCIt}

	iter, err := NewMarketCandidateGangIterator("pool", nil, iteratorsByQueue)
	assert.NoError(t, err)
	expectedItemOrder := []string{"A", "B", "C", "A", "B", "C", "B", "A", "C"}

	actualItemOrder := []string{}
	for {
		result, _, err := iter.Peek()
		assert.NoError(t, err)
		if result == nil {
			break
		}
		actualItemOrder = append(actualItemOrder, result.Queue)
		err = iter.Clear()
		assert.NoError(t, err)
	}

	assert.Equal(t, expectedItemOrder, actualItemOrder)
}

func createSchedulingContext(t *testing.T) *context.SchedulingContext {
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(
		testfixtures.Cpu("1"),
		testfixtures.TestPool,
		testfixtures.TestSchedulingConfig(),
	)
	require.NoError(t, err)
	return context.NewSchedulingContext(
		"pool",
		fairnessCostProvider,
		rate.NewLimiter(
			rate.Limit(testfixtures.TestSchedulingConfig().MaximumSchedulingRate),
			testfixtures.TestSchedulingConfig().MaximumSchedulingBurst,
		),
		testfixtures.Cpu("1"),
	)
}

func createJobIterator(sctx *context.SchedulingContext, queue string, prices ...float64) *QueuedGangIterator {
	jctxs := []*context.JobSchedulingContext{}
	for _, price := range prices {
		job := testfixtures.Test1Cpu4GiJob(queue, testfixtures.PriorityClass1)
		job = job.WithPoolBidPrices(map[string]float64{"pool": price})
		jctx := context.JobSchedulingContextFromJob(job)
		jctxs = append(jctxs, jctx)
	}
	return NewQueuedGangIterator(sctx, NewInMemoryJobIterator(jctxs), 1000, false)
}
