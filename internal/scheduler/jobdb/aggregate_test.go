package jobdb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/benbjohnson/immutable"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
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

func cpuOf(rl internaltypes.ResourceList) int64 {
	q := rl.GetByNameZeroIfMissing("cpu")
	return q.Value()
}

// demandForPool derives the bulk per-queue demand for a pool by querying the
// aggregate one queue at a time, mirroring how the scheduler consumes the API.
func demandForPool(txn *Txn, pool string, queues ...string) map[string]map[string]internaltypes.ResourceList {
	demand := make(map[string]map[string]internaltypes.ResourceList, len(queues))
	for _, queue := range queues {
		byPriorityClass := txn.GetQueuedDemand(pool, queue)
		if len(byPriorityClass) == 0 {
			continue
		}
		demand[queue] = byPriorityClass
	}
	return demand
}

// leasedDemandForPool derives the bulk per-queue running demand for a pool by
// querying the aggregate one queue at a time.
func leasedDemandForPool(txn *Txn, pool string, queues ...string) map[string]map[string]internaltypes.ResourceList {
	demand := make(map[string]map[string]internaltypes.ResourceList, len(queues))
	for _, queue := range queues {
		byPriorityClass := txn.GetLeasedDemand(pool, queue)
		if len(byPriorityClass) == 0 {
			continue
		}
		demand[queue] = byPriorityClass
	}
	return demand
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

	readTxn := jobDb.ReadTxn()

	// Queued demand on pool-1 is the sum of queued jobs eligible for it.
	demand := demandForPool(readTxn, "pool-1", "queue-1", "queue-2", "queue-3")
	assert.Equal(t, int64(3), cpuOf(demand["queue-1"][aggregateTestPriorityClass]))
	// Leased jobs contribute nothing.
	assert.Nil(t, demand["queue-2"])
	assert.Nil(t, demand["queue-3"])

	// jobA is eligible for pool-2 as well.
	demandPool2 := demandForPool(readTxn, "pool-2", "queue-1", "queue-2", "queue-3")
	assert.Equal(t, int64(1), cpuOf(demandPool2["queue-1"][aggregateTestPriorityClass]))

	// Querying an unknown queue yields no demand.
	assert.Empty(t, readTxn.GetQueuedDemand("pool-1", "does-not-exist"))

	// Running demand is keyed by the run's pool.
	leasedDemand := leasedDemandForPool(readTxn, "pool-1", "queue-1", "queue-2", "queue-3")
	assert.Equal(t, int64(3), cpuOf(leasedDemand["queue-2"][aggregateTestPriorityClass]))
	assert.Nil(t, leasedDemand["queue-1"])
	assert.Nil(t, leasedDemand["queue-3"])

	// The derived scheduling info combines queued and running demand, and splits
	// allocation home vs away.
	info := readTxn.CalculateSchedulingInfo(
		map[string]bool{"executor-1": true, "executor-2": true},
		"pool-1", []string{"pool-2"}, []string{"pool-1", "pool-2"},
		map[string]bool{"queue-1": true, "queue-2": true, "queue-3": true}, map[string]bool{},
		true, true,
	)
	assert.Equal(t, int64(3), cpuOf(info.DemandByQueueAndPriorityClass["queue-1"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(3), cpuOf(info.DemandByQueueAndPriorityClass["queue-2"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(3), cpuOf(info.AllocatedByQueueAndPriorityClass["queue-2"][aggregateTestPriorityClass]))
	assert.Nil(t, info.AwayAllocatedByQueueAndPriorityClass["queue-1"])
	assert.Equal(t, int64(4), cpuOf(info.AwayAllocatedByQueueAndPriorityClass["queue-3"][aggregateTestPriorityClass]))
	assert.Len(t, info.JobsByPool["pool-1"], 1)
	assert.Len(t, info.JobsByPool["pool-2"], 1)
	assert.Len(t, info.JobsByExecutorId["executor-1"], 1)
	assert.Len(t, info.JobsByExecutorId["executor-2"], 1)
}

func TestJobAggregate_QueuedToLeasedTransition(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA}))
	txn.Commit()

	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")
	assert.Equal(t, int64(1), cpuOf(demand[aggregateTestPriorityClass]))

	// Queued jobA becomes leased: removal of the old queued state must drop it
	// from the aggregate, and the leased state must not re-add it.
	jobAUpdated := jobA.WithQueued(false).WithNewRun("executor-2", "node-2", "node-2", "pool-2", 5)
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobAUpdated}))
	txn.Commit()

	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1"))
	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("pool-2", "queue-1"))

	// The leased state is now present in the running aggregate.
	assert.Equal(t, int64(1), cpuOf(jobDb.ReadTxn().GetLeasedDemand("pool-2", "queue-1")[aggregateTestPriorityClass]))
	assert.Empty(t, jobDb.ReadTxn().GetLeasedDemand("pool-1", "queue-1"))

	// Deleting a queued job removes it from the aggregate.
	jobB := newAggregateTestJob(t, jobDb, "jobB", "queue-1", true, []string{"pool-1"}, 2)
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobB}))
	txn.Commit()
	demand = jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")
	assert.Equal(t, int64(2), cpuOf(demand[aggregateTestPriorityClass]))

	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{jobB.Id()}))
	txn.Commit()
	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1"))
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

	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")
	assert.Equal(t, int64(1), cpuOf(demand[aggregateTestPriorityClass]))

	// Deleting the job must clear the single counted entry.
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{jobA.Id()}))
	txn.Commit()
	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1"))
}

func TestJobAggregate_TransactionIsolation(t *testing.T) {
	jobDb := NewTestJobDb()

	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{jobA}))
	txn.Commit()

	committedDemand := func() int64 {
		return cpuOf(jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")[aggregateTestPriorityClass])
	}
	txnDemand := func(t *Txn) int64 {
		return cpuOf(t.GetQueuedDemand("pool-1", "queue-1")[aggregateTestPriorityClass])
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

	txn := jobDb.DryRunTxn()
	jobA := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1"}, 1)
	require.NoError(t, txn.Upsert([]*Job{jobA}))
	assert.Equal(t, int64(1), cpuOf(txn.GetQueuedDemand("pool-1", "queue-1")[aggregateTestPriorityClass]))
	txn.Commit()

	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1"))
}

func TestJobAggregate_DuplicatePoolsCountedOnce(t *testing.T) {
	jobDb := NewTestJobDb()

	job := newAggregateTestJob(t, jobDb, "jobA", "queue-1", true, []string{"pool-1", "pool-1"}, 2)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{job}))
	txn.Commit()

	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")
	assert.Equal(t, int64(2), cpuOf(demand[aggregateTestPriorityClass]))
}

func TestJobAggregate_Transitions(t *testing.T) {
	const pool = "pool-1"

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

			demand := jobDb.ReadTxn().GetQueuedDemand(pool, "queue-1")
			if tc.wantCPU == 0 {
				assert.Empty(t, demand)
			} else {
				assert.Equal(t, tc.wantCPU, cpuOf(demand[aggregateTestPriorityClass]))
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

	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")

	require.Truef(t, demand["foo"].Equal(foo.AllResourceRequirements()), "foo: got %s", demand["foo"])
	require.Truef(t, demand["bar"].Equal(bar.AllResourceRequirements()), "bar: got %s", demand["bar"])
}

func TestJobAggregate_BatchUpsertAndDelete(t *testing.T) {
	jobDb := NewTestJobDb()

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
	demand := jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")
	assert.Equal(t, int64(11), cpuOf(demand[aggregateTestPriorityClass]))
	require.Equal(t, dLast, jobDb.ReadTxn().GetById("d"))

	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{"a", "does-not-exist", "d"}))
	txn.Commit()

	// c=4 remains.
	demand = jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")
	assert.Equal(t, int64(4), cpuOf(demand[aggregateTestPriorityClass]))
}

func TestJobAggregate_BatchDeleteDuplicateIds(t *testing.T) {
	jobDb := NewTestJobDb()
	job := newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-1"}, 2)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{job}))
	txn.Commit()

	before := testutil.ToFloat64(jobAggregateInvariantViolations.WithLabelValues("remove_missing_pool"))
	txn = jobDb.WriteTxn()
	require.NoError(t, txn.BatchDelete([]string{"job", "job"}))
	txn.Commit()
	assert.Equal(t, before, testutil.ToFloat64(jobAggregateInvariantViolations.WithLabelValues("remove_missing_pool")))
	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1"))
}

func TestJobAggregate_EmptyForUnknownQueue(t *testing.T) {
	jobDb := NewTestJobDb()
	job := newAggregateTestJob(t, jobDb, "job", "queue-1", true, []string{"pool-1"}, 2)
	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{job}))
	txn.Commit()

	// Callers decide which queues to query; unknown queues simply yield no demand.
	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("pool-1", "does-not-exist"))
	assert.Empty(t, jobDb.ReadTxn().GetQueuedDemand("does-not-exist", "queue-1"))
}

func TestJobAggregate_CloneIsolation(t *testing.T) {
	jobDb := NewTestJobDb()

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
	assert.Equal(t, int64(3), cpuOf(jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")[aggregateTestPriorityClass]))
	assert.Equal(t, int64(1), cpuOf(clone.ReadTxn().GetQueuedDemand("pool-1", "queue-1")[aggregateTestPriorityClass]))

	// Mutating the clone must not affect the original.
	c := newAggregateTestJob(t, clone, "c", "queue-1", true, []string{"pool-1"}, 4)
	txn = clone.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{c}))
	txn.Commit()
	assert.Equal(t, int64(3), cpuOf(jobDb.ReadTxn().GetQueuedDemand("pool-1", "queue-1")[aggregateTestPriorityClass]))
	assert.Equal(t, int64(5), cpuOf(clone.ReadTxn().GetQueuedDemand("pool-1", "queue-1")[aggregateTestPriorityClass]))
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
		"leased missing executor": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 1).
					WithNewRun("executor-2", "node-2", "node-2", "pool-1", 0)
			},
			want: "remove_missing_executor",
		},
		"leased missing job": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "other", "queue-1", false, []string{"pool-1"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			want: "remove_missing_job",
		},
		"leased missing pool": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-2"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-2", 0)
			},
			want: "remove_missing_pool",
		},
		"leased missing queue": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-2", false, []string{"pool-1"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			want: "remove_missing_queue",
		},
		"leased missing priority class": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJobWithPC(t, jobDb, "job", "queue-1", "foo", false, []string{"pool-1"}, cpuAndMemory(1, 1)).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJobWithPC(t, jobDb, "job", "queue-1", "bar", false, []string{"pool-1"}, cpuAndMemory(1, 1)).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			want: "remove_missing_priority_class",
		},
		"leased negative remaining": {
			add: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 1).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
			},
			remove: func(t *testing.T, jobDb *JobDb) *Job {
				return newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 2).
					WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
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
	require.Empty(t, a.getQueuedDemand("pool-1", "queue-1"))
	require.Empty(t, a.getLeasedDemand("pool-1", "queue-1"))
	info := a.CalculateSchedulingInfo(nil, "pool-1", nil, nil, nil, nil, true, true)
	require.Empty(t, info.JobsByPool)
}

// TestJobAggregate_ZeroValue exercises a zero-value aggregate, whose maps are
// nil, to ensure the read paths are nil safe.
func TestJobAggregate_ZeroValue(t *testing.T) {
	a := &JobAggregate{}
	assert.Empty(t, a.getQueuedDemand("pool-1", "queue-1"))
	assert.Empty(t, a.getLeasedDemand("pool-1", "queue-1"))

	info := a.CalculateSchedulingInfo(
		map[string]bool{"executor-1": true}, "pool-1", []string{"pool-2"}, []string{"pool-1", "pool-2"}, nil, nil,
		true, true,
	)
	assert.Empty(t, info.JobsByPool)
	assert.Empty(t, info.DemandByQueueAndPriorityClass)
}

// TestJobAggregate_NilInnerMapsAndNilKnownQueues covers the defensive guards for
// nil nested maps and for callers that pass no known-queue filter.
func TestJobAggregate_NilInnerMapsAndNilKnownQueues(t *testing.T) {
	jobDb := NewTestJobDb()
	job := newAggregateTestJob(t, jobDb, "job", "queue-1", false, []string{"pool-1"}, 1).
		WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)

	nilQueue := immutable.NewMap[string, *immutable.Map[string, internaltypes.ResourceList]](nil).Set("queue-1", nil)
	executorMap := immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]](nil).
		Set("executor-1", nilQueue).
		Set("executor-2", nil)
	allocatedByExecutor := immutable.NewMap[string, *immutable.Map[string, *immutable.Map[string, *immutable.Map[string, internaltypes.ResourceList]]]](nil).
		Set("pool-1", executorMap).
		Set("pool-2", nil)

	a := &JobAggregate{
		allocatedByExecutor: allocatedByExecutor,
		leasedJobs:          immutable.NewMap[string, *Job](nil).Set("job", job),
	}
	assert.Empty(t, a.getLeasedDemand("pool-1", "queue-1"))
	// A nil known-queue filter means every queue is known.
	info := a.CalculateSchedulingInfo(map[string]bool{"executor-1": true}, "pool-1", nil, []string{"pool-1"}, nil, nil, true, true)
	assert.Len(t, info.JobsByPool["pool-1"], 1)
}

// TestJobAggregate_CalculateSchedulingInfo exercises the full derived
// scheduling info, including cordoned queues, inactive executors, away pools,
// unknown queues, zero-resource jobs and terminal jobs.
func TestJobAggregate_CalculateSchedulingInfo(t *testing.T) {
	jobDb := NewTestJobDb()

	queuedQ1 := newAggregateTestJob(t, jobDb, "queued-q1", "q1", true, []string{"pool-1", "pool-2"}, 1)
	queuedCordoned := newAggregateTestJob(t, jobDb, "queued-cordoned", "q2", true, []string{"pool-1"}, 1)
	queuedUnknown := newAggregateTestJob(t, jobDb, "queued-unknown", "unknown", true, []string{"pool-1"}, 1)
	leasedUnknown := newAggregateTestJob(t, jobDb, "leased-unknown", "unknown", false, []string{"pool-1"}, 1).
		WithNewRun("executor-1", "node-6", "node-6", "pool-1", 0)
	queuedZero := newAggregateTestJobWithPC(t, jobDb, "queued-zero", "q1", "zero-pc", true, []string{"pool-1"}, v1.ResourceList{})
	leasedActive := newAggregateTestJob(t, jobDb, "leased-active", "q1", false, []string{"pool-1"}, 1).
		WithNewRun("executor-1", "node-1", "node-1", "pool-1", 0)
	leasedInactive := newAggregateTestJob(t, jobDb, "leased-inactive", "q1", false, []string{"pool-1"}, 1).
		WithNewRun("executor-2", "node-2", "node-2", "pool-1", 0)
	leasedCordonedInactive := newAggregateTestJob(t, jobDb, "leased-cordoned-inactive", "q2", false, []string{"pool-1"}, 1).
		WithNewRun("executor-2", "node-5", "node-5", "pool-1", 0)
	leasedAway := newAggregateTestJob(t, jobDb, "leased-away", "q2", false, []string{"pool-2"}, 1).
		WithNewRun("executor-1", "node-3", "node-3", "pool-2", 0)
	leasedOtherPool := newAggregateTestJob(t, jobDb, "leased-other", "q1", false, []string{"pool-3"}, 1).
		WithNewRun("executor-1", "node-4", "node-4", "pool-3", 0)
	terminal := newAggregateTestJob(t, jobDb, "terminal", "q1", true, []string{"pool-1"}, 1).WithFailed(true)

	txn := jobDb.WriteTxn()
	require.NoError(t, txn.Upsert([]*Job{
		queuedQ1, queuedCordoned, queuedUnknown, queuedZero,
		leasedActive, leasedInactive, leasedCordonedInactive, leasedAway, leasedOtherPool, leasedUnknown, terminal,
	}))
	txn.Commit()

	readTxn := jobDb.ReadTxn()
	info := readTxn.CalculateSchedulingInfo(
		map[string]bool{"executor-1": true},
		"pool-1", []string{"pool-2"}, []string{"pool-1", "pool-2"},
		map[string]bool{"q1": true, "q2": true}, map[string]bool{"q2": true},
		true, true,
	)

	// Demand: queued q1 (1) + running q1 (active 1 + inactive 1) = 3. q2 is
	// cordoned so its queued job is excluded, but the running q2 job counts.
	assert.Equal(t, int64(3), cpuOf(info.DemandByQueueAndPriorityClass["q1"][aggregateTestPriorityClass]))
	assert.Equal(t, int64(1), cpuOf(info.DemandByQueueAndPriorityClass["q2"][aggregateTestPriorityClass]))
	assert.Nil(t, info.DemandByQueueAndPriorityClass["unknown"])

	// Allocation only counts active executors.
	assert.Equal(t, int64(1), cpuOf(info.AllocatedByQueueAndPriorityClass["q1"][aggregateTestPriorityClass]))
	assert.Nil(t, info.AllocatedByQueueAndPriorityClass["q2"])

	// Away allocation for the away pool.
	assert.Equal(t, int64(1), cpuOf(info.AwayAllocatedByQueueAndPriorityClass["q2"][aggregateTestPriorityClass]))

	// Jobs by pool includes all pools, even those not relevant to the round.
	assert.Len(t, info.JobsByPool["pool-1"], 3)
	assert.Len(t, info.JobsByPool["pool-2"], 1)
	assert.Len(t, info.JobsByPool["pool-3"], 1)

	// Jobs by executor is restricted to pools in allPools.
	assert.Len(t, info.JobsByExecutorId["executor-1"], 2) // active + away
	assert.Len(t, info.JobsByExecutorId["executor-2"], 2) // inactive + cordoned-inactive
	assert.Empty(t, info.JobsByExecutorId["executor-3"])

	// In-use priority classes come from all active jobs, including cordoned
	// queues and zero-resource jobs, but not unknown queues or terminal jobs.
	assert.True(t, info.InUsePriorityClasses[aggregateTestPriorityClass])
	assert.True(t, info.InUsePriorityClasses["zero-pc"])
}

// TestJobAggregate_ChurnSchedulingInfoMatchesReference runs randomized churn
// and checks the derived scheduling info against an independent reference
// computed from the current jobs.
func TestJobAggregate_ChurnSchedulingInfoMatchesReference(t *testing.T) {
	jobDb := NewTestJobDb()
	rng := rand.New(rand.NewSource(7))
	pools := []string{"pool-1", "pool-2", "pool-3"}
	queues := []string{"q1", "q2"}
	priorityClasses := []string{"foo", "bar"}
	ids := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	activeExecutorsSet := map[string]bool{"executor-1": true, "executor-2": true}
	knownQueues := map[string]bool{"q1": true, "q2": true}
	cordonedQueues := map[string]bool{"q2": true}
	currentPool := "pool-1"
	awayAllocationPools := []string{"pool-2"}
	allPools := []string{"pool-1", "pool-2"}

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
				job = job.WithNewRun(fmt.Sprintf("executor-%d", 1+rng.Intn(2)), "node", id, selectedPools[0], 0)
			}
			if rng.Intn(7) == 0 {
				job = job.WithFailed(true)
			}
			txn := jobDb.WriteTxn()
			require.NoError(t, txn.Upsert([]*Job{job}))
			txn.Commit()
		}

		readTxn := jobDb.ReadTxn()
		reference := referenceSchedulingInfo(
			readTxn.GetAll(), activeExecutorsSet, currentPool, awayAllocationPools, allPools, knownQueues, cordonedQueues,
		)
		got := readTxn.CalculateSchedulingInfo(
			activeExecutorsSet, currentPool, awayAllocationPools, allPools, knownQueues, cordonedQueues,
			true, true,
		)
		requireSchedulingInfoEqual(t, reference, got, fmt.Sprintf("op %d", i))
	}
}

// referenceSchedulingInfo independently derives the scheduling info from the
// current jobs, mirroring the scheduler's scan.
func referenceSchedulingInfo(
	jobs []*Job,
	activeExecutorsSet map[string]bool,
	currentPool string,
	awayAllocationPools []string,
	allPools []string,
	knownQueues map[string]bool,
	cordonedQueues map[string]bool,
) *SchedulingInfo {
	info := &SchedulingInfo{
		JobsByExecutorId:                     map[string][]*Job{},
		JobsByPool:                           map[string][]*Job{},
		DemandByQueueAndPriorityClass:        map[string]map[string]internaltypes.ResourceList{},
		AllocatedByQueueAndPriorityClass:     map[string]map[string]internaltypes.ResourceList{},
		AwayAllocatedByQueueAndPriorityClass: map[string]map[string]internaltypes.ResourceList{},
		InUsePriorityClasses:                 map[string]bool{},
	}
	allPoolsSet := make(map[string]bool, len(allPools))
	for _, pool := range allPools {
		allPoolsSet[pool] = true
	}
	awaySet := make(map[string]bool, len(awayAllocationPools))
	for _, pool := range awayAllocationPools {
		awaySet[pool] = true
	}
	addAllocation := func(dst map[string]map[string]internaltypes.ResourceList, job *Job) {
		byPC := dst[job.Queue()]
		if byPC == nil {
			byPC = map[string]internaltypes.ResourceList{}
			dst[job.Queue()] = byPC
		}
		byPC[job.PriorityClassName()] = byPC[job.PriorityClassName()].Add(job.AllResourceRequirements())
	}

	for _, job := range jobs {
		if !knownQueues[job.Queue()] || job.InTerminalState() {
			continue
		}
		// The scan never receives jobs that are neither queued nor leased.
		if !job.Queued() && job.LatestRun() == nil {
			continue
		}

		pools := job.Pools()
		if !job.Queued() && job.LatestRun() != nil {
			pools = []string{job.LatestRun().Pool()}
		}
		// The scan only receives queued jobs eligible for a pool in allPools.
		if job.Queued() {
			eligible := false
			for _, pool := range pools {
				if allPoolsSet[pool] {
					eligible = true
					break
				}
			}
			if !eligible {
				continue
			}
		}

		info.InUsePriorityClasses[job.PriorityClassName()] = true

		if slices.Contains(pools, currentPool) {
			byPC := info.DemandByQueueAndPriorityClass[job.Queue()]
			if byPC == nil {
				byPC = map[string]internaltypes.ResourceList{}
				info.DemandByQueueAndPriorityClass[job.Queue()] = byPC
			}
			if !cordonedQueues[job.Queue()] || !job.Queued() {
				byPC[job.PriorityClassName()] = byPC[job.PriorityClassName()].Add(job.AllResourceRequirements())
			}
		}

		if job.Queued() || job.LatestRun() == nil {
			continue
		}
		run := job.LatestRun()
		executor := run.Executor()
		pool := run.Pool()
		info.JobsByPool[pool] = append(info.JobsByPool[pool], job)
		if !allPoolsSet[pool] {
			continue
		}
		if activeExecutorsSet[executor] {
			if pool == currentPool {
				addAllocation(info.AllocatedByQueueAndPriorityClass, job)
			} else if awaySet[pool] {
				addAllocation(info.AwayAllocatedByQueueAndPriorityClass, job)
			}
		}
		info.JobsByExecutorId[executor] = append(info.JobsByExecutorId[executor], job)
	}
	return info
}

func requireSchedulingInfoEqual(t *testing.T, want, got *SchedulingInfo, context string) {
	t.Helper()
	require.Equalf(t, want.InUsePriorityClasses, got.InUsePriorityClasses, "%s: in use priority classes", context)
	requireResourceMapsEqual(t, want.DemandByQueueAndPriorityClass, got.DemandByQueueAndPriorityClass, context+" demand")
	requireResourceMapsEqual(t, want.AllocatedByQueueAndPriorityClass, got.AllocatedByQueueAndPriorityClass, context+" allocated")
	requireResourceMapsEqual(t, want.AwayAllocatedByQueueAndPriorityClass, got.AwayAllocatedByQueueAndPriorityClass, context+" away allocated")
	requireJobsByKeyEqual(t, want.JobsByPool, got.JobsByPool, context+" jobs by pool")
	requireJobsByKeyEqual(t, want.JobsByExecutorId, got.JobsByExecutorId, context+" jobs by executor")
}

func requireResourceMapsEqual(t *testing.T, want, got map[string]map[string]internaltypes.ResourceList, context string) {
	t.Helper()
	for queue, wantByPC := range want {
		for pc, wantRL := range wantByPC {
			if wantRL.AllZero() {
				continue
			}
			require.Truef(t, wantRL.Equal(got[queue][pc]), "%s: queue %s pc %s want %s got %s", context, queue, pc, wantRL, got[queue][pc])
		}
	}
	for queue, gotByPC := range got {
		for pc, gotRL := range gotByPC {
			if gotRL.AllZero() {
				continue
			}
			require.Truef(t, want[queue][pc].Equal(gotRL), "%s: queue %s pc %s want %s got %s", context, queue, pc, want[queue][pc], gotRL)
		}
	}
}

func requireJobsByKeyEqual(t *testing.T, want, got map[string][]*Job, context string) {
	t.Helper()
	keys := map[string]bool{}
	for key := range want {
		keys[key] = true
	}
	for key := range got {
		keys[key] = true
	}
	for key := range keys {
		wantIds := map[string]bool{}
		for _, job := range want[key] {
			wantIds[job.Id()] = true
		}
		gotIds := map[string]bool{}
		for _, job := range got[key] {
			gotIds[job.Id()] = true
		}
		require.Equalf(t, wantIds, gotIds, "%s: key %s", context, key)
	}
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

		// Periodically cross-check the per-queue read against the reference.
		if i%20 == 0 {
			for _, pool := range pools {
				requireDemandEqual(
					t,
					referenceDemandForPool(reference, pool, queues),
					demandForPool(jobDb.ReadTxn(), pool, queues...),
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

func referenceDemandForPool(ref map[aggregateTestKey]internaltypes.ResourceList, pool string, queues []string) map[string]map[string]internaltypes.ResourceList {
	known := make(map[string]bool, len(queues))
	for _, queue := range queues {
		known[queue] = true
	}
	demand := map[string]map[string]internaltypes.ResourceList{}
	for key, rl := range ref {
		if key.pool != pool || !known[key.queue] {
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
