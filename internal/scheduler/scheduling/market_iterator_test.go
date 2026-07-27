package scheduling

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/armadaproject/armada/internal/scheduler/pricing"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestMarketIteratorPQ_HomeBeforeAway(t *testing.T) {
	home := &MarketIteratorPQItem{queue: "q", price: 1}
	away := &MarketIteratorPQItem{queue: context.CalculateAwayQueueName("q"), away: true, price: 1000}

	// With the flag on, home sorts before away despite the lower price.
	pq := MarketIteratorPQ{
		preemptCrossPoolJobsFirst: true,
		items:                     []*MarketIteratorPQItem{home, away},
	}
	require.True(t, pq.Less(0, 1))
	require.False(t, pq.Less(1, 0))

	// With the flag off, away wins on price (1000 > 1).
	pq.preemptCrossPoolJobsFirst = false
	require.False(t, pq.Less(0, 1))
	require.True(t, pq.Less(1, 0))
}

func TestMarketIteratorPQ_Ordering(t *testing.T) {
	queueA := &MarketIteratorPQItem{queue: "A", price: 3, queued: true, runtime: 10, submittedTime: 10}
	queueB := &MarketIteratorPQItem{queue: "B", price: 3, queued: false, runtime: 10, submittedTime: 10}
	queueC := &MarketIteratorPQItem{queue: "C", price: 2, queued: true, runtime: 10, submittedTime: 10}
	queueD := &MarketIteratorPQItem{queue: "D", price: 2, queued: true, runtime: 8, submittedTime: 10}
	queueE := &MarketIteratorPQItem{queue: "E", price: 2, queued: true, runtime: 8, submittedTime: 5}
	queueF := &MarketIteratorPQItem{queue: "F", price: 1, queued: true, runtime: 8, submittedTime: 10}
	queueG := &MarketIteratorPQItem{queue: "G", price: 1, queued: true, runtime: 8, submittedTime: 10}
	pq := &MarketIteratorPQ{items: []*MarketIteratorPQItem{queueG, queueF, queueE, queueD, queueC, queueB, queueA}}

	sort.Sort(pq)
	expectedOrder := []*MarketIteratorPQItem{queueB, queueA, queueC, queueE, queueD, queueF, queueG}
	assert.Equal(t, expectedOrder, pq.items)
}

func TestMarketBasedCandidateGangIterator_RoundRobin(t *testing.T) {
	type queuePricingInfo struct {
		Queue     string
		JobPrices []float64
	}

	// Instantiation order matters here, first declared has the older jobs - meaning it'll get ordered first
	tests := map[string]struct {
		input         []queuePricingInfo
		expectedOrder []string
	}{
		"two queues": {
			input: []queuePricingInfo{
				{Queue: "A", JobPrices: []float64{600, 600, 600}},
				{Queue: "B", JobPrices: []float64{600, 600, 400}},
			},
			expectedOrder: []string{"A", "B", "A", "B", "A", "B"},
		},
		"two queues - reverse": {
			input: []queuePricingInfo{
				{Queue: "B", JobPrices: []float64{600, 600, 400}},
				{Queue: "A", JobPrices: []float64{600, 600, 600}},
			},
			expectedOrder: []string{"B", "A", "B", "A", "A", "B"},
		},
		"three queues": {
			input: []queuePricingInfo{
				{Queue: "A", JobPrices: []float64{600, 600, 400}},
				{Queue: "B", JobPrices: []float64{600, 600, 600}},
				{Queue: "C", JobPrices: []float64{600, 600, 300}},
			},
			expectedOrder: []string{"A", "B", "C", "A", "B", "C", "B", "A", "C"},
		},
		"three queues - mixed": {
			input: []queuePricingInfo{
				{Queue: "C", JobPrices: []float64{600, 600, 300}},
				{Queue: "A", JobPrices: []float64{600, 600, 400}},
				{Queue: "B", JobPrices: []float64{600, 600, 600}},
			},
			expectedOrder: []string{"C", "A", "B", "C", "A", "B", "B", "A", "C"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			sctx := createSchedulingContext(t)

			iteratorsByQueue := map[string]*QueuedGangIterator{}
			for _, queueInfo := range tc.input {
				iteratorsByQueue[queueInfo.Queue] = createJobIterator(sctx, queueInfo.Queue, queueInfo.JobPrices...)
			}

			iter, err := NewMarketCandidateGangIterator("pool", nil, iteratorsByQueue, false)
			assert.NoError(t, err)

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

			assert.Equal(t, tc.expectedOrder, actualItemOrder)
		})
	}
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
		job = job.WithBidPrices(map[string]pricing.Bid{"pool": {RunningBid: price, QueuedBid: price}})
		jctx := context.JobSchedulingContextFromJob(job)
		jctxs = append(jctxs, jctx)
	}
	return NewQueuedGangIterator(sctx, NewInMemoryJobIterator(jctxs), 1000, false)
}
