package jobdb

import (
	"sort"
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

func calculateInfo(txn *Txn, currentPool string, awayPools, allPools []string, known, cordoned map[string]bool) *SchedulingInfo {
	return txn.CalculateSchedulingInfo(
		map[string]bool{"executor-1": true, "executor-2": true},
		currentPool,
		awayPools,
		allPools,
		known,
		cordoned,
	)
}

func cpuOf(rl internaltypes.ResourceList) int64 {
	q := rl.GetByNameZeroIfMissing("cpu")
	return q.Value()
}

func TestJobAggregate_Query(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1", "pool-2"}, 1)
	jobB := newAggregateTestJob(t, jobDb, "jobB", "queue-1", true, []string{"pool-1"}, 2)
	jobC := newAggregateTestJob(t, jobDb, "jobC", "queue-2", false, []string{"pool-1"}, 3).
		WithNewRun("executor-1", "node-1", "node-1", "pool-1", 5)
	jobD := newAggregateTestJob(t, jobDb, "jobD", "queue-3", false, []string{"pool-2"}, 4).
		WithNewRun("executor-2", "node-2", "node-2", "pool-2", 5)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA, jobB, jobC, jobD}))
	txn.Commit()

	known := map[string]bool{"queue-1": true, "queue-2": true, "queue-3": true}

	info := calculateInfo(jobDb.ReadTxn(), "pool-1", []string{"pool-2"}, []string{"pool-1", "pool-2"}, known, nil)

	// Demand on pool-1 is the sum of all jobs eligible for (queued) or running on it.
	assert.Equal(t, int64(3), cpuOf(info.DemandByQueueAndPriorityClass["queue-1"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(3), cpuOf(info.DemandByQueueAndPriorityClass["queue-2"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(3), cpuOf(info.AllocatedByQueueAndPriorityClass["queue-2"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(4), cpuOf(info.AwayAllocatedByQueueAndPriorityClass["queue-3"][aggregateTestPriorityClass]))

	assert.Equal(t, []string{"jobC"}, jobIds(info.JobsByPool["pool-1"]))
	assert.Equal(t, []string{"jobD"}, jobIds(info.JobsByPool["pool-2"]))
	assert.Equal(t, []string{"jobC"}, jobIds(info.JobsByExecutorId["executor-1"]))
	assert.Equal(t, []string{"jobD"}, jobIds(info.JobsByExecutorId["executor-2"]))
	assert.Equal(t, map[string]bool{aggregateTestPriorityClass: true}, info.InUsePriorityClasses)

	// Cordoned queues should not contribute queued demand, but running jobs still do.
	cordoned := map[string]bool{"queue-1": true}
	info = calculateInfo(jobDb.ReadTxn(), "pool-1", []string{"pool-2"}, []string{"pool-1", "pool-2"}, known, cordoned)
	assert.Nil(t, info.DemandByQueueAndPriorityClass["queue-1"])
	assert.Equal(t, int64(3), cpuOf(info.DemandByQueueAndPriorityClass["queue-2"][aggregateTestPriorityClass]))

	// Unknown queues should be dropped entirely.
	knownWithoutQueue1 := map[string]bool{"queue-2": true, "queue-3": true}
	info = calculateInfo(jobDb.ReadTxn(), "pool-1", []string{"pool-2"}, []string{"pool-1", "pool-2"}, knownWithoutQueue1, nil)
	assert.Nil(t, info.DemandByQueueAndPriorityClass["queue-1"])
	assert.Equal(t, []string{"jobC"}, jobIds(info.JobsByPool["pool-1"]))
}

func TestJobAggregate_UpdateAndDelete(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)
	jobB := newAggregateTestJob(t, jobDb, "jobB", "queue-1", false, []string{"pool-1"}, 2).
		WithNewRun("executor-1", "node-1", "node-1", "pool-1", 5)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA, jobB}))
	txn.Commit()

	known := map[string]bool{"queue-1": true}

	// Queued jobA becomes leased on pool-2; removal of the old state and addition of the new
	// state must both be reflected.
	jobAUpdated := jobA.WithQueued(false).WithNewRun("executor-2", "node-2", "node-2", "pool-2", 5)
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobAUpdated}))
	txn.Commit()

	info := calculateInfo(jobDb.ReadTxn(), "pool-1", nil, []string{"pool-1", "pool-2"}, known, nil)
	assert.Equal(t, int64(2), cpuOf(info.DemandByQueueAndPriorityClass["queue-1"][aggregateTestPriorityClass]))
	assert.Equal(t, []string{"jobB"}, jobIds(info.JobsByPool["pool-1"]))

	info = calculateInfo(jobDb.ReadTxn(), "pool-1", []string{"pool-2"}, []string{"pool-1", "pool-2"}, known, nil)
	assert.Equal(t, int64(1), cpuOf(info.AwayAllocatedByQueueAndPriorityClass["queue-1"][aggregateTestPriorityClass]))
	assert.Equal(t, []string{"jobA"}, jobIds(info.JobsByPool["pool-2"]))

	// Deleting a job removes it from the aggregate.
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{jobB.Id()}))
	txn.Commit()

	info = calculateInfo(jobDb.ReadTxn(), "pool-1", nil, []string{"pool-1", "pool-2"}, known, nil)
	assert.Nil(t, info.DemandByQueueAndPriorityClass["queue-1"])
	assert.Empty(t, info.JobsByPool["pool-1"])

	// Deleting the last job removes it from the in-use priority classes.
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{jobAUpdated.Id()}))
	txn.Commit()
	info = calculateInfo(jobDb.ReadTxn(), "pool-1", nil, []string{"pool-1", "pool-2"}, known, nil)
	assert.Empty(t, info.InUsePriorityClasses)
}

func TestJobAggregate_TransactionIsolation(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA}))
	txn.Commit()

	known := map[string]bool{"queue-1": true}
	committedDemand := func() int64 {
		return cpuOf(jobDb.ReadTxn().CalculateSchedulingInfo(
			map[string]bool{}, "pool-1", nil, []string{"pool-1"}, known, nil,
		).DemandByQueueAndPriorityClass["queue-1"][aggregateTestPriorityClass])
	}
	txnDemand := func(t *Txn) int64 {
		return cpuOf(t.CalculateSchedulingInfo(
			map[string]bool{}, "pool-1", nil, []string{"pool-1"}, known, nil,
		).DemandByQueueAndPriorityClass["queue-1"][aggregateTestPriorityClass])
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
	assert.Equal(t, int64(1), cpuOf(txn.CalculateSchedulingInfo(
		map[string]bool{}, "pool-1", nil, []string{"pool-1"}, known, nil,
	).DemandByQueueAndPriorityClass["queue-1"][aggregateTestPriorityClass]))
	txn.Commit()

	assert.Nil(t, jobDb.ReadTxn().CalculateSchedulingInfo(
		map[string]bool{}, "pool-1", nil, []string{"pool-1"}, known, nil,
	).DemandByQueueAndPriorityClass["queue-1"])
}

func jobIds(jobs []*Job) []string {
	ids := make([]string, len(jobs))
	for i, job := range jobs {
		ids[i] = job.Id()
	}
	sort.Strings(ids)
	return ids
}
