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
	knownQueues := make(map[string]bool, numQueues)
	for i := 0; i < numQueues; i++ {
		queue := fmt.Sprintf("queue-%d", i)
		knownQueues[queue] = true
		for j := 0; j < numQueuedPerQueue; j++ {
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
			job, err := jobDb.NewJob(
				fmt.Sprintf("job-%d-%d", i, j), "jobset", queue, 0, info,
				true, 0, false, false, false, 0, true, []string{pool}, 0,
			)
			require.NoError(b, err)
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
			queued := readTxn.GetQueuedJobsByPool(pool)
			demand := map[string]map[string]int64{}
			for _, job := range queued {
				q := job.Queue()
				if demand[q] == nil {
					demand[q] = map[string]int64{}
				}
				demand[q][job.PriorityClassName()]++
			}
			_ = demand
		}
	})

	b.Run("impl=aggregate", func(b *testing.B) {
		b.ReportAllocs()
		for n := 0; n < b.N; n++ {
			_ = readTxn.GetQueuedDemandWithTxn(pool, knownQueues, nil)
		}
	})
}
