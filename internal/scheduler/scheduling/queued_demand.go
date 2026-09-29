package scheduling

import (
	"maps"
	"time"

	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/api"
)

// compareAggregateQueuedDemand computes queued demand by scanning jobs and from
// the JobDb aggregate, compares them, and publishes any difference. The
// scan-derived value remains authoritative.
func (l *FairSchedulingAlgo) compareAggregateQueuedDemand(
	ctx *armadacontext.Context,
	jobs []*jobdb.Job,
	txn *jobdb.Txn,
	queues map[string]*api.Queue,
	currentPool string,
) {
	scanned := scanQueuedDemand(jobs, queues, currentPool)

	start := time.Now()
	aggregate := queuedDemandFromAggregate(txn, queues, currentPool)
	observeJobAggregateLookupDuration(currentPool, time.Since(start).Seconds())

	jobAggregateComparisons.WithLabelValues(currentPool).Inc()
	if !queuedDemandEqual(scanned, aggregate) {
		jobAggregateMismatches.WithLabelValues(currentPool).Inc()
		ctx.Errorf("JobDb queued demand aggregate mismatch for pool %s", currentPool)
	}
}

// scanQueuedDemand derives queued demand from queued jobs eligible for currentPool.
func scanQueuedDemand(
	jobs []*jobdb.Job,
	queues map[string]*api.Queue,
	currentPool string,
) map[string]map[string]internaltypes.ResourceList {
	demand := map[string]map[string]internaltypes.ResourceList{}
	for _, job := range jobs {
		if job.InTerminalState() || !job.Queued() {
			continue
		}
		queue, present := queues[job.Queue()]
		if !present || queue.Cordoned {
			continue
		}
		if !slices.Contains(job.Pools(), currentPool) {
			continue
		}
		queueName := job.Queue()
		pc := job.PriorityClassName()
		byPriorityClass, ok := demand[queueName]
		if !ok {
			byPriorityClass = map[string]internaltypes.ResourceList{}
			demand[queueName] = byPriorityClass
		}
		byPriorityClass[pc] = byPriorityClass[pc].Add(job.AllResourceRequirements())
	}
	return demand
}

func queuedDemandFromAggregate(
	txn *jobdb.Txn,
	queues map[string]*api.Queue,
	currentPool string,
) map[string]map[string]internaltypes.ResourceList {
	knownQueues := make(map[string]bool, len(queues))
	cordonedQueues := make(map[string]bool, len(queues))
	for name, queue := range queues {
		knownQueues[name] = true
		cordonedQueues[name] = queue.Cordoned
	}
	return txn.GetQueuedDemand(currentPool, knownQueues, cordonedQueues)
}

// queuedDemandEqual reports whether the scan-derived and aggregate-derived
// queued demand are equivalent.
func queuedDemandEqual(a, b map[string]map[string]internaltypes.ResourceList) bool {
	return maps.EqualFunc(a, b, func(x, y map[string]internaltypes.ResourceList) bool {
		return maps.EqualFunc(x, y, func(x, y internaltypes.ResourceList) bool {
			return x.AllZero() && y.AllZero() || x.Equal(y)
		})
	})
}
