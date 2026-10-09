package scheduling

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/api"
)

// compareScannedWithAggregate scans queued demand from jobs, compares it
// against a precomputed aggregate-derived demand, and publishes any
// difference. The scan-derived value remains authoritative. The caller passes
// in the aggregate lookup shared with the Use path, so Compare=true+Use=true
// pays for a single lookup per pool per round.
func (l *FairSchedulingAlgo) compareScannedWithAggregate(
	ctx *armadacontext.Context,
	jobs []*jobdb.Job,
	queues map[string]*api.Queue,
	aggregate map[string]map[string]internaltypes.ResourceList,
	currentPool string,
) {
	scanned := scanQueuedDemand(jobs, queues, currentPool)

	jobAggregateComparisons.WithLabelValues(currentPool).Inc()
	if !queuedDemandEqual(scanned, aggregate) {
		jobAggregateMismatches.WithLabelValues(currentPool).Inc()
		ctx.Errorf("JobDb queued demand aggregate mismatch for pool %s: %s", currentPool, queuedDemandDiff(scanned, aggregate))
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
	demand := make(map[string]map[string]internaltypes.ResourceList, len(queues))
	for name, queue := range queues {
		if queue.Cordoned {
			continue
		}
		byPriorityClass := txn.GetQueueDemand(currentPool, name)
		if len(byPriorityClass) == 0 {
			continue
		}
		demand[name] = byPriorityClass
	}
	return demand
}

// queuedDemandEqual reports whether the scan-derived and aggregate-derived
// queued demand are equivalent.
//
// An absent bucket is treated as an empty (zero) bucket. The aggregate drops
// buckets once their resources reach zero, so a queue/priority-class that only
// ever held zero-resource jobs may be present in one side and absent in the
// other; that is not a difference in demand. A bucket holding non-zero demand
// on either side must be present and equal on both.
func queuedDemandEqual(a, b map[string]map[string]internaltypes.ResourceList) bool {
	return queuedDemandSubsetEqual(a, b) && queuedDemandSubsetEqual(b, a)
}

func queuedDemandSubsetEqual(a, b map[string]map[string]internaltypes.ResourceList) bool {
	for queue, aByPriorityClass := range a {
		bByPriorityClass := b[queue]
		for priorityClass, aRL := range aByPriorityClass {
			if !resourceListsEqual(aRL, bByPriorityClass[priorityClass]) {
				return false
			}
		}
	}
	return true
}

func resourceListsEqual(a, b internaltypes.ResourceList) bool {
	return a.AllZero() && b.AllZero() || a.Equal(b)
}

// queuedDemandDiff renders the queue/priority-class buckets that differ between
// the scan-derived and aggregate-derived queued demand. It is only called when a
// mismatch is detected, so the extra allocation is kept off the hot path.
func queuedDemandDiff(scanned, aggregate map[string]map[string]internaltypes.ResourceList) string {
	var differences []string
	for _, queue := range unionQueueNames(scanned, aggregate) {
		for _, priorityClass := range unionPriorityClasses(scanned[queue], aggregate[queue]) {
			scannedRL := scanned[queue][priorityClass]
			aggregateRL := aggregate[queue][priorityClass]
			if !resourceListsEqual(scannedRL, aggregateRL) {
				differences = append(differences, fmt.Sprintf("%s/%s scan=%s aggregate=%s", queue, priorityClass, scannedRL, aggregateRL))
			}
		}
	}
	return strings.Join(differences, "; ")
}

func unionQueueNames(a, b map[string]map[string]internaltypes.ResourceList) []string {
	names := make(map[string]bool, len(a)+len(b))
	for queue := range a {
		names[queue] = true
	}
	for queue := range b {
		names[queue] = true
	}
	result := make([]string, 0, len(names))
	for queue := range names {
		result = append(result, queue)
	}
	sort.Strings(result)
	return result
}

func unionPriorityClasses(a, b map[string]internaltypes.ResourceList) []string {
	names := make(map[string]bool, len(a)+len(b))
	for priorityClass := range a {
		names[priorityClass] = true
	}
	for priorityClass := range b {
		names[priorityClass] = true
	}
	result := make([]string, 0, len(names))
	for priorityClass := range names {
		result = append(result, priorityClass)
	}
	sort.Strings(result)
	return result
}
