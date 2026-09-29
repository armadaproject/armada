package scheduling

import (
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/api"
)

// compareAggregateQueuedDemand computes queued demand by scanning jobs and from the JobDb
// aggregate, compares them, and publishes any difference. The scan-derived value
// remains authoritative.
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

	mismatchedComponents, diff := compareQueuedDemand(scanned, aggregate)
	recordJobAggregateComparisonResult(currentPool, mismatchedComponents)
	if diff != "" {
		ctx.Errorf("JobDb queued demand aggregate mismatch for pool %s (using scan result): %s", currentPool, diff)
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

// compareQueuedDemand returns the mismatching component names and a description
// of the differences between scan-derived and aggregate-derived queued demand.
// Both are empty if the two are equivalent.
func compareQueuedDemand(scan, aggregate map[string]map[string]internaltypes.ResourceList) ([]string, string) {
	if diff := compareResourceListMaps("queuedDemandByQueueAndPriorityClass", scan, aggregate); diff != "" {
		return []string{"demand_queued"}, diff
	}
	return nil, ""
}

func compareResourceListMaps(name string, scan, aggregate map[string]map[string]internaltypes.ResourceList) string {
	diffs := make([]string, 0)
	for _, queue := range unionKeys(scan, aggregate) {
		for _, priorityClass := range unionResourcePriorityClasses(scan[queue], aggregate[queue]) {
			scanRl := scan[queue][priorityClass]
			aggregateRl := aggregate[queue][priorityClass]
			scanZero := scanRl.AllZero()
			aggregateZero := aggregateRl.AllZero()
			if scanZero && aggregateZero {
				continue
			}
			if scanZero != aggregateZero || !scanRl.Equal(aggregateRl) {
				diffs = append(diffs, fmt.Sprintf("%s[queue=%s,priorityClass=%s]: scan=%s aggregate=%s",
					name, queue, priorityClass, scanRl.String(), aggregateRl.String()))
			}
		}
	}
	slices.Sort(diffs)
	return strings.Join(diffs, ", ")
}

func unionKeys(a, b map[string]map[string]internaltypes.ResourceList) []string {
	keySet := make(map[string]bool, len(a)+len(b))
	for key := range a {
		keySet[key] = true
	}
	for key := range b {
		keySet[key] = true
	}
	keys := maps.Keys(keySet)
	slices.Sort(keys)
	return keys
}

func unionResourcePriorityClasses(a, b map[string]internaltypes.ResourceList) []string {
	keySet := make(map[string]bool, len(a)+len(b))
	for key := range a {
		keySet[key] = true
	}
	for key := range b {
		keySet[key] = true
	}
	keys := maps.Keys(keySet)
	slices.Sort(keys)
	return keys
}
