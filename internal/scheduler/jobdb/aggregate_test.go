package jobdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

const aggregateTestPriorityClass = "foo"

func newAggregateTestJob(t *testing.T, jobDb *JobDb, id, queue string, queued bool, pools []string, cpu int64) *Job {
	t.Helper()
	info := &internaltypes.JobSchedulingInfo{
		PriorityClass: aggregateTestPriorityClass,
		PodRequirements: &internaltypes.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu": *k8sResource.NewQuantity(cpu, k8sResource.DecimalSI),
				},
			},
		},
	}
	job, err := jobDb.NewJob(id, "jobset", queue, 0, info, queued, 0, false, false, false, 0, true, pools, 0)
	require.NoError(t, err)
	return job
}

func queuedDemandOf(txn *Txn, currentPool string, known, cordoned map[string]bool) map[string]map[string]internaltypes.ResourceList {
	return txn.GetQueuedDemandWithTxn(currentPool, known, cordoned)
}

func cpuOf(rl internaltypes.ResourceList) int64 {
	q := rl.GetByNameZeroIfMissing("cpu")
	return q.Value()
}

func TestJobAggregate_QueuedDemand(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1", "pool-2"}, 1)
	jobB := newAggregateTestJob(t, jobDb, "jobB", "queue-1", true, []string{"pool-1"}, 2)
	// Leased jobs never contribute to the queued-demand aggregate.
	jobC := newAggregateTestJob(t, jobDb, "jobC", "queue-2", false, []string{"pool-1"}, 3).
		WithNewRun("executor-1", "node-1", "node-1", "pool-1", 5)
	jobD := newAggregateTestJob(t, jobDb, "jobD", "queue-3", false, []string{"pool-2"}, 4).
		WithNewRun("executor-2", "node-2", "node-2", "pool-2", 5)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA, jobB, jobC, jobD}))
	txn.Commit()

	known := map[string]bool{"queue-1": true, "queue-2": true, "queue-3": true}

	demand := queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, nil)

	// Queued demand on pool-1 is the sum of queued jobs eligible for it.
	assert.Equal(t, int64(3), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))
	// Leased jobs contribute nothing.
	assert.Nil(t, demand["queue-2"])
	assert.Nil(t, demand["queue-3"])

	// jobA is eligible for pool-2 as well.
	demandPool2 := queuedDemandOf(jobDb.ReadTxn(), "pool-2", known, nil)
	assert.Equal(t, int64(1), cpuOf(demandPool2["queue-1"][aggregateTestPriorityClass]))

	// Cordoned queues are excluded.
	cordoned := map[string]bool{"queue-1": true}
	demand = queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, cordoned)
	assert.Nil(t, demand["queue-1"])

	// Unknown queues are dropped. Use the non-transactional convenience wrapper,
	// mirroring NodeDb.GetNode vs GetNodeWithTxn.
	knownWithoutQueue1 := map[string]bool{"queue-2": true, "queue-3": true}
	demand = jobDb.GetQueuedDemand("pool-1", knownWithoutQueue1, nil)
	assert.Nil(t, demand["queue-1"])
}

func TestJobAggregate_QueuedToLeasedTransition(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA}))
	txn.Commit()

	known := map[string]bool{"queue-1": true}
	demand := queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, nil)
	assert.Equal(t, int64(1), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))

	// Queued jobA becomes leased: removal of the old queued state must drop it
	// from the aggregate, and the leased state must not re-add it.
	jobAUpdated := jobA.WithQueued(false).WithNewRun("executor-2", "node-2", "node-2", "pool-2", 5)
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobAUpdated}))
	txn.Commit()

	demand = queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, nil)
	assert.Nil(t, demand["queue-1"])
	demand = queuedDemandOf(jobDb.ReadTxn(), "pool-2", known, nil)
	assert.Nil(t, demand["queue-1"])

	// Deleting a queued job removes it from the aggregate.
	jobB := newAggregateTestJob(t, jobDb, "jobB", "queue-1", true, []string{"pool-1"}, 2)
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobB}))
	txn.Commit()
	demand = queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, nil)
	assert.Equal(t, int64(2), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))

	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{jobB.Id()}))
	txn.Commit()
	demand = queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, nil)
	assert.Nil(t, demand["queue-1"])
}

func TestJobAggregate_TransactionIsolation(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA}))
	txn.Commit()

	known := map[string]bool{"queue-1": true}
	committedDemand := func() int64 {
		return cpuOf(jobDb.ReadTxn().GetQueuedDemandWithTxn("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass])
	}
	txnDemand := func(t *Txn) int64 {
		return cpuOf(t.GetQueuedDemandWithTxn("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass])
	}
	require.Equal(t, int64(1), committedDemand())

	// A write transaction must not affect the committed aggregate until it commits.
	writeTxn := jobDb.WriteTxn()
	jobB := newAggregateTestJob(t, jobDb, "jobB", "queue-1", true, []string{"pool-1"}, 2)
	require.NoError(t, writeTxn.Upsert([]*Job{jobB}))
	assert.Equal(t, int64(1), committedDemand())
	assert.Equal(t, int64(3), txnDemand(writeTxn))

	// Aborting discards the changes.
	writeTxn.Abort()
	assert.Equal(t, int64(1), committedDemand())

	// Committing applies them.
	writeTxn = jobDb.WriteTxn()
	require.NoError(t, writeTxn.Upsert([]*Job{jobB}))
	writeTxn.Commit()
	assert.Equal(t, int64(3), committedDemand())
}

func TestJobAggregate_DryRunTxnDoesNotAffectDb(t *testing.T) {
	jobDb := NewTestJobDb()

	known := map[string]bool{"queue-1": true}
	txn := jobDb.DryRunTxn()
	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)
	require.NoError(t, txn.Upsert([]*Job{jobA}))
	assert.Equal(t, int64(1), cpuOf(txn.GetQueuedDemandWithTxn("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass]))
	txn.Commit()

	assert.Nil(t, jobDb.ReadTxn().GetQueuedDemandWithTxn("pool-1", known, nil)["queue-1"])
}
