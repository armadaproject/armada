package jobdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// BenchmarkQueuedDemandAggregate compares deriving queued demand by scanning
// queued jobs against the incrementally maintained aggregate lookup.
func BenchmarkQueuedDemandAggregate(b *testing.B) {
	const (
		numQueues         = 8
		numQueuedPerQueue = 2000
	)
	pool := "pool-1"
	jobDb := NewTestJobDb()
	jobs := make([]*Job, 0, numQueues*numQueuedPerQueue)
	queues := make([]string, 0, numQueues)
	for i := 0; i < numQueues; i++ {
		queue := fmt.Sprintf("queue-%d", i)
		queues = append(queues, queue)
		for j := 0; j < numQueuedPerQueue; j++ {
			job := newBenchmarkJob(b, jobDb, fmt.Sprintf("job-%d-%d", i, j), queue, true, pool)
			jobs = append(jobs, job)
		}
	}
	txn := jobDb.WriteTxn()
	require.NoError(b, txn.Upsert(jobs))
	txn.Commit()
	readTxn := jobDb.ReadTxn()

	b.Run("impl=scan", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			demand := map[string]map[string]internaltypes.ResourceList{}
			for _, job := range readTxn.GetQueuedJobsByPool(pool) {
				queue := job.Queue()
				byPriorityClass, ok := demand[queue]
				if !ok {
					byPriorityClass = map[string]internaltypes.ResourceList{}
					demand[queue] = byPriorityClass
				}
				pc := job.PriorityClassName()
				byPriorityClass[pc] = byPriorityClass[pc].Add(job.AllResourceRequirements())
			}
			_ = demand
		}
	})

	b.Run("impl=aggregate", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			for _, queue := range queues {
				_ = readTxn.GetQueuedDemand(pool, queue)
			}
		}
	})
}

// BenchmarkSchedulingInfo compares scanning all queued and leased jobs against
// deriving the full scheduling info from the aggregate.
func BenchmarkSchedulingInfoAggregate(b *testing.B) {
	const (
		numQueues         = 8
		numQueuedPerQueue = 2000
		numLeasedPerQueue = 500
	)
	poolNames := []string{"pool-1", "pool-2", "pool-3", "pool-4"}
	jobDb := NewTestJobDb()
	jobs := make([]*Job, 0, numQueues*(numQueuedPerQueue+numLeasedPerQueue))
	queues := make([]string, 0, numQueues)
	for i := 0; i < numQueues; i++ {
		queue := fmt.Sprintf("queue-%d", i)
		queues = append(queues, queue)
		for j := 0; j < numQueuedPerQueue; j++ {
			jobs = append(jobs, newBenchmarkJob(b, jobDb, fmt.Sprintf("queued-%d-%d", i, j), queue, true, poolNames...))
		}
		for j := 0; j < numLeasedPerQueue; j++ {
			job := newBenchmarkJob(
				b, jobDb, fmt.Sprintf("leased-%d-%d", i, j), queue, false, poolNames...,
			).WithNewRun(
				fmt.Sprintf("executor-%d", j%len(poolNames)), fmt.Sprintf("node-%d", j), fmt.Sprintf("node-%d", j),
				poolNames[j%len(poolNames)], 0,
			)
			jobs = append(jobs, job)
		}
	}
	txn := jobDb.WriteTxn()
	require.NoError(b, txn.Upsert(jobs))
	txn.Commit()
	readTxn := jobDb.ReadTxn()

	knownQueues := make(map[string]bool, numQueues)
	for _, queue := range queues {
		knownQueues[queue] = true
	}
	activeExecutorsSet := map[string]bool{}
	for i := range poolNames {
		activeExecutorsSet[fmt.Sprintf("executor-%d", i)] = true
	}
	currentPool := poolNames[0]
	awayAllocationPools := poolNames[1:]
	allPools := poolNames

	b.Run("impl=scan", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			info := referenceSchedulingInfo(
				readTxn.GetAll(), activeExecutorsSet, currentPool, awayAllocationPools, allPools, knownQueues, map[string]bool{},
			)
			_ = info
		}
	})

	b.Run("impl=aggregate", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_ = readTxn.CalculateSchedulingInfo(
				activeExecutorsSet, currentPool, awayAllocationPools, allPools, knownQueues, map[string]bool{},
				true, true,
			)
		}
	})
}

func newBenchmarkJob(b *testing.B, jobDb *JobDb, id, queue string, queued bool, pools ...string) *Job {
	b.Helper()
	info := &internaltypes.JobSchedulingInfo{
		PriorityClass: "foo",
		PodRequirements: &internaltypes.PodRequirements{
			ResourceRequirements: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"cpu": *k8sResource.NewQuantity(1, k8sResource.DecimalSI),
				},
			},
		},
	}
	job, err := jobDb.NewJob(id, "jobset", queue, 0, info, queued, 0, false, false, false, 0, true, pools, 0)
	require.NoError(b, err)
	return job
}
