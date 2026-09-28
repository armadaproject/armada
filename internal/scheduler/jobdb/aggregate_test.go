package jobdb

import (
	"math/rand"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

const aggregateTestPriorityClass = "foo"

func newAggregateTestJob(t testing.TB, jobDb *JobDb, id, queue string, queued bool, pools []string, cpu int64) *Job {
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
	return txn.GetQueuedDemand(currentPool, known, cordoned)
}

func cpuOf(rl internaltypes.ResourceList) int64 {
	q := rl.GetByNameZeroIfMissing("cpu")
	return q.Value()
}

func TestJobAggregate_JobAggregate(t *testing.T) {
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

	// Unknown queues are dropped.
	knownWithoutQueue1 := map[string]bool{"queue-2": true, "queue-3": true}
	demand = jobDb.ReadTxn().GetQueuedDemand("pool-1", knownWithoutQueue1, nil)
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

func TestJobAggregate_UpsertDeduplicatesJobIds(t *testing.T) {
	jobDb := NewTestJobDb()

	// Two distinct job instances sharing an ID. jobsById keeps only the last one,
	// so the aggregate must also count the ID once rather than once per entry.
	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)
	jobADuplicate := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA, jobADuplicate}))
	txn.Commit()

	known := map[string]bool{"queue-1": true}
	demand := queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, nil)
	assert.Equal(t, int64(1), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))

	// Deleting the job must clear the single counted entry.
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{jobA.Id()}))
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
		return cpuOf(jobDb.ReadTxn().GetQueuedDemand("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass])
	}
	txnDemand := func(t *Txn) int64 {
		return cpuOf(t.GetQueuedDemand("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass])
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
	assert.Equal(t, int64(1), cpuOf(txn.GetQueuedDemand("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass]))
	txn.Commit()

	assert.Nil(t, jobDb.ReadTxn().GetQueuedDemand("pool-1", known, nil)["queue-1"])
}

func TestJobAggregate_DuplicatePoolsCountedOnce(t *testing.T) {
	jobDb := NewTestJobDb()

	job := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1", "pool-1"}, 2)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{job}))
	txn.Commit()

	known := map[string]bool{"queue-1": true}
	demand := queuedDemandOf(jobDb.ReadTxn(), "pool-1", known, nil)
	assert.Equal(t, int64(2), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))
}

func TestJobAggregate_Transitions(t *testing.T) {
	const pool = "pool-1"
	known := map[string]bool{"queue-1": true}

	queued := func(t *testing.T, jobDb *JobDb) *Job {
		return newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{pool}, 1)
	}
	leased := func(t *testing.T, jobDb *JobDb) *Job {
		return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{pool}, 1).
			WithNewRun("executor-1", "node-1", "node-1", pool, 0)
	}

	tests := map[string]struct {
		initial func(t *testing.T, jobDb *JobDb) *Job
		updated func(*Job) *Job
		wantCPU int64
	}{
		"queued to leased":    {queued, func(j *Job) *Job { return j.WithQueued(false).WithNewRun("executor-1", "node-1", "node-1", pool, 0) }, 0},
		"queued to cancelled": {queued, func(j *Job) *Job { return j.WithCancelled(true) }, 0},
		"queued to failed":    {queued, func(j *Job) *Job { return j.WithFailed(true) }, 0},
		"queued to succeeded": {queued, func(j *Job) *Job { return j.WithSucceeded(true) }, 0},
		"leased to cancelled": {leased, func(j *Job) *Job { return j.WithCancelled(true) }, 0},
		"leased to failed":    {leased, func(j *Job) *Job { return j.WithFailed(true) }, 0},
		"leased to succeeded": {leased, func(j *Job) *Job { return j.WithSucceeded(true) }, 0},
		"queued unchanged":    {queued, func(j *Job) *Job { return j }, 1},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobDb := NewTestJobDb()
			initial := tc.initial(t, jobDb)
			txn := jobDb.WriteTxn()
			require.NoError(t, txn.Upsert([]*Job{initial}))
			txn.Commit()

			updated := tc.updated(initial)
			txn = jobDb.WriteTxn()
			require.NoError(t, txn.Upsert([]*Job{updated}))
			txn.Commit()

			demand := jobDb.ReadTxn().GetQueuedDemand(pool, known, nil)
			if tc.wantCPU == 0 {
				assert.Nil(t, demand["queue-1"])
			} else {
				assert.Equal(t, tc.wantCPU, cpuOf(demand["queue-1"][aggregateTestPriorityClass]))
			}
		})
	}
}

func TestJobAggregate_RemoveInvariantViolationRecorded(t *testing.T) {
	jobDb := NewTestJobDb()
	job := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)

	before := testutil.ToFloat64(jobAggregateInvariantViolations.WithLabelValues("remove_missing_pool"))

	// A write transaction removing a job that was never added must record a violation.
	txn := jobDb.WriteTxn()
	txn.aggregate.remove(job)
	txn.Abort()

	after := testutil.ToFloat64(jobAggregateInvariantViolations.WithLabelValues("remove_missing_pool"))
	assert.Equal(t, before+1, after)
}

type aggregateTestKey struct {
	pool  string
	queue string
	pc    string
}

func TestJobAggregate_ChurnMatchesReference(t *testing.T) {
	jobDb := NewTestJobDb()
	rng := rand.New(rand.NewSource(42))
	pools := []string{"pool-1", "pool-2", "pool-3"}
	queues := []string{"q1", "q2"}
	ids := []string{"a", "b", "c", "d", "e", "f"}

	for i := 0; i < 400; i++ {
		if rng.Intn(6) == 0 {
			id := ids[rng.Intn(len(ids))]
			txn := jobDb.WriteTxn()
			require.NoError(t, txn.BatchDelete([]string{id}))
			txn.Commit()
		} else {
			id := ids[rng.Intn(len(ids))]
			queued := rng.Intn(3) != 0
			selectedPools := randomPoolSubset(rng, pools)
			job := newAggregateTestJob(t, jobDb, id, queues[rng.Intn(len(queues))], queued, selectedPools, int64(1+rng.Intn(4)))
			if !queued && rng.Intn(2) == 0 {
				job = job.WithNewRun("executor", "node", id, selectedPools[0], 0)
			}
			if rng.Intn(7) == 0 {
				job = job.WithFailed(true)
			}
			txn := jobDb.WriteTxn()
			require.NoError(t, txn.Upsert([]*Job{job}))
			txn.Commit()
		}

		require.Equal(t,
			referenceAggregate(jobDb.ReadTxn().GetAll()),
			observedAggregate(jobDb.aggregate),
			"op %d", i,
		)
	}
}

func randomPoolSubset(rng *rand.Rand, pools []string) []string {
	n := 1 + rng.Intn(len(pools))
	perm := rng.Perm(len(pools))[:n]
	result := make([]string, 0, n+1)
	for _, idx := range perm {
		result = append(result, pools[idx])
	}
	// Occasionally duplicate a pool to exercise deduplication.
	if rng.Intn(5) == 0 {
		result = append(result, result[0])
	}
	return result
}

// referenceAggregate computes the expected aggregate from the current jobs,
// independently of the incremental maintenance.
func referenceAggregate(jobs []*Job) map[aggregateTestKey]int64 {
	result := map[aggregateTestKey]int64{}
	for _, job := range jobs {
		if job.InTerminalState() || !job.Queued() {
			continue
		}
		cpu := cpuOf(job.AllResourceRequirements())
		seen := make(map[string]bool, len(job.Pools()))
		for _, pool := range job.Pools() {
			if seen[pool] {
				continue
			}
			seen[pool] = true
			result[aggregateTestKey{pool: pool, queue: job.Queue(), pc: job.PriorityClassName()}] += cpu
		}
	}
	return result
}

func observedAggregate(a *JobAggregate) map[aggregateTestKey]int64 {
	result := map[aggregateTestKey]int64{}
	if a == nil || a.queuedDemand == nil {
		return result
	}
	poolIt := a.queuedDemand.Iterator()
	for !poolIt.Done() {
		pool, queueMap, _ := poolIt.Next()
		if queueMap == nil {
			continue
		}
		queueIt := queueMap.Iterator()
		for !queueIt.Done() {
			queue, pcMap, _ := queueIt.Next()
			if pcMap == nil {
				continue
			}
			pcIt := pcMap.Iterator()
			for !pcIt.Done() {
				pc, rl, _ := pcIt.Next()
				result[aggregateTestKey{pool: pool, queue: queue, pc: pc}] = cpuOf(rl)
			}
		}
	}
	return result
}
