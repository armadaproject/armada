package jobdb

import (
	"fmt"
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
	return newAggregateTestJobWithPC(t, jobDb, id, queue, aggregateTestPriorityClass, queued, pools, v1.ResourceList{
		"cpu": *k8sResource.NewQuantity(cpu, k8sResource.DecimalSI),
	})
}

func newAggregateTestJobWithPC(t testing.TB, jobDb *JobDb, id, queue, priorityClass string, queued bool, pools []string, requests v1.ResourceList) *Job {
	t.Helper()
	info := &internaltypes.JobSchedulingInfo{
		PriorityClass: priorityClass,
		PodRequirements: &internaltypes.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: requests,
			},
		},
	}
	job, err := jobDb.NewJob(id, "jobset", queue, 0, info, queued, 0, false, false, false, 0, true, pools, 0)
	require.NoError(t, err)
	return job
}

func cpuAndMemory(cpu int64, memoryGi int64) v1.ResourceList {
	return v1.ResourceList{
		"cpu":    *k8sResource.NewQuantity(cpu, k8sResource.DecimalSI),
		"memory": *k8sResource.NewQuantity(memoryGi, k8sResource.BinarySI),
	}
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

func TestJobAggregate_MultiplePriorityClassesAndResources(t *testing.T) {
	jobDb := NewTestJobDb()

	foo := newAggregateTestJobWithPC(t, jobDb, "foo", "queue-1", "foo", true, []string{"pool-1"}, cpuAndMemory(1, 1))
	bar := newAggregateTestJobWithPC(t, jobDb, "bar", "queue-1", "bar", true, []string{"pool-1"}, cpuAndMemory(2, 4))

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{foo, bar}))
	txn.Commit()

	known := map[string]bool{"queue-1": true}
	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", known, nil)

	require.Truef(t, demand["queue-1"]["foo"].Equal(foo.AllResourceRequirements()), "foo: got %s", demand["queue-1"]["foo"])
	require.Truef(t, demand["queue-1"]["bar"].Equal(bar.AllResourceRequirements()), "bar: got %s", demand["queue-1"]["bar"])
}

func TestJobAggregate_BatchUpsertAndDelete(t *testing.T) {
	jobDb := NewTestJobDb()
	known := map[string]bool{"queue-1": true}

	a := newAggregateTestJob(t, jobDb, "a", "queue-1", true, []string{"pool-1"}, 1)
	b := newAggregateTestJob(t, jobDb, "b", "queue-1", true, []string{"pool-1"}, 2)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{a, b}))
	txn.Commit()

	// One batch mixing an unchanged job, a queued->leased transition, a new job,
	// duplicate IDs, and a terminal job.
	bLeased := b.WithQueued(false).WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
	c := newAggregateTestJob(t, jobDb, "c", "queue-1", true, []string{"pool-1"}, 4)
	dFirst := newAggregateTestJob(t, jobDb, "d", "queue-1", true, []string{"pool-1"}, 5)
	dLast := newAggregateTestJob(t, jobDb, "d", "queue-1", true, []string{"pool-1"}, 6)
	e := newAggregateTestJob(t, jobDb, "e", "queue-1", true, []string{"pool-1"}, 7).WithFailed(true)

	txn = jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{a, bLeased, c, dFirst, dLast, e}))
	txn.Commit()

	// a=1, b leased excluded, c=4, d=6 (last wins), e terminal excluded -> 11.
	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", known, nil)
	assert.Equal(t, int64(11), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))
	require.Equal(t, dLast, jobDb.ReadTxn().GetById("d"))

	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{"a", "does-not-exist", "d"}))
	txn.Commit()

	// c=4 remains.
	demand = jobDb.ReadTxn().GetQueuedDemand("pool-1", known, nil)
	assert.Equal(t, int64(4), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))
}

func TestJobAggregate_NilKnownQueues(t *testing.T) {
	jobDb := NewTestJobDb()
	job := newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-1"}, 2)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{job}))
	txn.Commit()

	// nil knownQueues means all queues are considered known.
	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", nil, nil)
	assert.Equal(t, int64(2), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))
}

func TestJobAggregate_CloneIsolation(t *testing.T) {
	jobDb := NewTestJobDb()
	known := map[string]bool{"queue-1": true}

	a := newAggregateTestJob(t, jobDb, "a", "queue-1", true, []string{"pool-1"}, 1)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{a}))
	txn.Commit()

	clone := jobDb.Clone()

	// Mutating the original must not affect the clone.
	b := newAggregateTestJob(t, jobDb, "b", "queue-1", true, []string{"pool-1"}, 2)
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{b}))
	txn.Commit()
	assert.Equal(t, int64(3), cpuOf(jobDb.ReadTxn().GetQueuedDemand("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(1), cpuOf(clone.ReadTxn().GetQueuedDemand("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass]))

	// Mutating the clone must not affect the original.
	c := newAggregateTestJob(t, clone, "c", "queue-1", true, []string{"pool-1"}, 4)
	txn = clone.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{c}))
	txn.Commit()
	assert.Equal(t, int64(3), cpuOf(jobDb.ReadTxn().GetQueuedDemand("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(5), cpuOf(clone.ReadTxn().GetQueuedDemand("pool-1", known, nil)["queue-1"][aggregateTestPriorityClass]))
}

func TestJobAggregate_RemoveInvariantViolations(t *testing.T) {
	tests := map[string]struct {
		add    func(t *testing.T, jobDb *JobDb) *Job
		remove func(t *testing.T, jobDb *JobDb) *Job
		want   string
	}{
		"missing pool": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-1"}, 1)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-2"}, 1)
			},
			want: "remove_missing_pool",
		},
		"missing queue": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-1"}, 1)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-2", true, []string{"pool-1"}, 1)
			},
			want: "remove_missing_queue",
		},
		"missing priority class": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJobWithPC(t, jobDb, "job", "queue-1", "foo", true, []string{"pool-1"}, cpuAndMemory(1, 1))
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJobWithPC(t, jobDb, "job", "queue-1", "bar", true, []string{"pool-1"}, cpuAndMemory(1, 1))
			},
			want: "remove_missing_priority_class",
		},
		"negative remaining": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-1"}, 1)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-1"}, 2)
			},
			want: "remove_negative_remaining",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobDb := NewTestJobDb()
			added := tc.add(t, jobDb)
			txn := jobDb.WriteTxn()
			require.NoError(t, txn.Upsert([]*Job{added}))
			txn.Commit()

			before := testutil.ToFloat64(jobAggregateInvariantViolations.WithLabelValues(tc.want))

			writeTxn := jobDb.WriteTxn()
			writeTxn.aggregate.remove(tc.remove(t, jobDb))
			writeTxn.Abort()

			after := testutil.ToFloat64(jobAggregateInvariantViolations.WithLabelValues(tc.want))
			assert.Equal(t, before+1, after)
		})
	}
}

func TestJobAggregate_NilReceiver(t *testing.T) {
	var a *JobAggregate
	require.NotNil(t, a.Clone())
	require.Empty(t, a.getQueuedDemand("pool-1", nil, nil))
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
	priorityClasses := []string{"foo", "bar"}
	ids := []string{"a", "b", "c", "d", "e", "f"}
	known := map[string]bool{"q1": true, "q2": true}

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
			job := newAggregateTestJobWithPC(
				t, jobDb, id, queues[rng.Intn(len(queues))], priorityClasses[rng.Intn(len(priorityClasses))],
				queued, selectedPools, cpuAndMemory(int64(1+rng.Intn(4)), int64(rng.Intn(8))),
			)
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

		reference := referenceAggregate(jobDb.ReadTxn().GetAll())
		requireAggregatesEqual(t, reference, observedAggregate(jobDb.aggregate), fmt.Sprintf("op %d raw", i))

		// Periodically cross-check the filtered read against the reference.
		if i%20 == 0 {
			for _, pool := range pools {
				requireDemandEqual(
					t,
					referenceDemandForPool(reference, pool, known, nil),
					jobDb.ReadTxn().GetQueuedDemand(pool, known, nil),
					fmt.Sprintf("op %d pool %s", i, pool),
				)
			}
		}
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
func referenceAggregate(jobs []*Job) map[aggregateTestKey]internaltypes.ResourceList {
	result := map[aggregateTestKey]internaltypes.ResourceList{}
	for _, job := range jobs {
		if job.InTerminalState() || !job.Queued() {
			continue
		}
		req := job.AllResourceRequirements()
		seen := make(map[string]bool, len(job.Pools()))
		for _, pool := range job.Pools() {
			if seen[pool] {
				continue
			}
			seen[pool] = true
			key := aggregateTestKey{pool: pool, queue: job.Queue(), pc: job.PriorityClassName()}
			result[key] = result[key].Add(req)
		}
	}
	return result
}

func observedAggregate(a *JobAggregate) map[aggregateTestKey]internaltypes.ResourceList {
	result := map[aggregateTestKey]internaltypes.ResourceList{}
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
				result[aggregateTestKey{pool: pool, queue: queue, pc: pc}] = rl
			}
		}
	}
	return result
}

func requireAggregatesEqual(t *testing.T, want, got map[aggregateTestKey]internaltypes.ResourceList, context string) {
	t.Helper()
	for key, wantRL := range want {
		gotRL, ok := got[key]
		require.Truef(t, ok, "%s: missing key %v", context, key)
		require.Truef(t, wantRL.Equal(gotRL), "%s: key %v want %s got %s", context, key, wantRL, gotRL)
	}
	require.Equalf(t, len(want), len(got), "%s: key count", context)
}

func referenceDemandForPool(ref map[aggregateTestKey]internaltypes.ResourceList, pool string, known, cordoned map[string]bool) map[string]map[string]internaltypes.ResourceList {
	demand := map[string]map[string]internaltypes.ResourceList{}
	for key, rl := range ref {
		if key.pool != pool || cordoned[key.queue] || !queueKnown(known, key.queue) {
			continue
		}
		byPriorityClass, ok := demand[key.queue]
		if !ok {
			byPriorityClass = map[string]internaltypes.ResourceList{}
			demand[key.queue] = byPriorityClass
		}
		byPriorityClass[key.pc] = byPriorityClass[key.pc].Add(rl)
	}
	return demand
}

func requireDemandEqual(t *testing.T, want, got map[string]map[string]internaltypes.ResourceList, context string) {
	t.Helper()
	require.Equalf(t, len(want), len(got), "%s: queue count", context)
	for queue, wantByPC := range want {
		gotByPC, ok := got[queue]
		require.Truef(t, ok, "%s: missing queue %s", context, queue)
		require.Equalf(t, len(wantByPC), len(gotByPC), "%s: queue %s priority class count", context, queue)
		for pc, wantRL := range wantByPC {
			gotRL, ok := gotByPC[pc]
			require.Truef(t, ok, "%s: queue %s missing priority class %s", context, queue, pc)
			require.Truef(t, wantRL.Equal(gotRL), "%s: queue %s pc %s want %s got %s", context, queue, pc, wantRL, gotRL)
		}
	}
}
