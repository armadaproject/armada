package scheduling

import (
	"fmt"
	"sort"
	"strings"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/api"
)

// compareScannedDemandWithAggregate compares the demand derived from scanning
// jobs against the aggregate-derived demand and publishes any difference to the
// metrics identified by comparison. The scan-derived value remains authoritative.
func compareScannedDemandWithAggregate(
	ctx *armadacontext.Context,
	comparison aggregateComparison,
	aggregate map[string]map[string]internaltypes.ResourceList,
	currentPool string,
	scanned map[string]map[string]internaltypes.ResourceList,
) {
	comparison.comparisons.WithLabelValues(currentPool).Inc()
	if !demandEqual(scanned, aggregate) {
		comparison.mismatches.WithLabelValues(currentPool).Inc()
		ctx.Errorf("JobDb %s demand aggregate mismatch for pool %s: %s", comparison.kind, currentPool, demandDiff(scanned, aggregate))
	}
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

// demandEqual reports whether the scan-derived and aggregate-derived demand are
// equivalent.
//
// An absent bucket is treated as an empty (zero) bucket. Both sides keep a
// bucket (with all-zero resources) while any job contributes to it and drop it
// when the last contributing job leaves, so absence and zero demand are
// equivalent. A bucket holding non-zero demand on either side must be present
// and equal on both.
func demandEqual(a, b map[string]map[string]internaltypes.ResourceList) bool {
	return demandSubsetEqual(a, b) && demandSubsetEqual(b, a)
}

func demandSubsetEqual(a, b map[string]map[string]internaltypes.ResourceList) bool {
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

// demandDiff renders the queue/priority-class buckets that differ between the
// scan-derived and aggregate-derived demand. It is only called when a mismatch
// is detected, so the extra allocation is kept off the hot path.
func demandDiff(scanned, aggregate map[string]map[string]internaltypes.ResourceList) string {
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
