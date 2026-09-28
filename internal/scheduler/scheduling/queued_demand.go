package scheduling

import (
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/pkg/api"
)

// queuedDemandSource supplies the queued demand used for scheduling.
//
// The scan-derived value is authoritative. The shadow source additionally
// computes queued demand from the JobDb aggregate and publishes any diff,
// without changing the result. A future aggregate source can return the
// aggregate value directly.
type queuedDemandSource interface {
	QueuedDemand(
		ctx *armadacontext.Context,
		scanned map[string]map[string]internaltypes.ResourceList,
		txn *jobdb.Txn,
		queues map[string]*api.Queue,
		currentPool string,
	) map[string]map[string]internaltypes.ResourceList
}

func newQueuedDemandSource(shadow bool) queuedDemandSource {
	if shadow {
		return shadowQueuedDemandSource{}
	}
	return scanQueuedDemandSource{}
}

// scanQueuedDemandSource uses the scan-derived queued demand (current behaviour).
type scanQueuedDemandSource struct{}

func (scanQueuedDemandSource) QueuedDemand(
	_ *armadacontext.Context,
	scanned map[string]map[string]internaltypes.ResourceList,
	_ *jobdb.Txn,
	_ map[string]*api.Queue,
	_ string,
) map[string]map[string]internaltypes.ResourceList {
	return scanned
}

// shadowQueuedDemandSource uses the scan-derived queued demand, but also computes
// the JobDb aggregate queued demand and publishes any difference.
type shadowQueuedDemandSource struct{}

func (shadowQueuedDemandSource) QueuedDemand(
	ctx *armadacontext.Context,
	scanned map[string]map[string]internaltypes.ResourceList,
	txn *jobdb.Txn,
	queues map[string]*api.Queue,
	currentPool string,
) map[string]map[string]internaltypes.ResourceList {
	start := time.Now()
	aggregate := queuedDemandFromAggregate(txn, queues, currentPool)
	observeJobAggregateLookupDuration(currentPool, time.Since(start).Seconds())

	mismatchedComponents, diff := compareQueuedDemand(scanned, aggregate)
	recordJobAggregateCanaryResult(currentPool, mismatchedComponents)
	if diff != "" {
		ctx.Errorf("JobDb queued demand aggregate mismatch for pool %s (using scan result): %s", currentPool, diff)
	}
	return scanned
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
	return txn.GetQueuedDemandWithTxn(currentPool, knownQueues, cordonedQueues)
}
