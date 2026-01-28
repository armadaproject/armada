package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

var (
	defaultPool                 = "test"
	reconciliationEnabledConfig = configuration.PoolConfig{
		Name:                          defaultPool,
		ExperimentalRunReconciliation: &configuration.RunReconciliationConfig{Enabled: true},
	}
	ensureReservationMatchConfig = configuration.PoolConfig{
		Name:                          defaultPool,
		ExperimentalRunReconciliation: &configuration.RunReconciliationConfig{Enabled: true, EnsureReservationMatch: true},
	}
	ensureReservationMismatchConfig = configuration.PoolConfig{
		Name:                          defaultPool,
		ExperimentalRunReconciliation: &configuration.RunReconciliationConfig{Enabled: true, EnsureReservationDoesNotMatch: true},
	}
	reconciliationDisabledConfig = configuration.PoolConfig{Name: defaultPool}
)

func TestReconcileJobRuns(t *testing.T) {
	tests := map[string]struct {
		node                        *schedulerobjects.Node
		job                         *jobdb.Job
		poolConfig                  configuration.PoolConfig
		expectReconciliationFailure bool
	}{
		"pool match": {
			job:                         createLeasedJob("node-1", defaultPool),
			node:                        createNodeWithPool("node-1", defaultPool),
			poolConfig:                  reconciliationEnabledConfig,
			expectReconciliationFailure: false,
		},
		"reservation matches - ensure reservation match": {
			job:                         withReservations(createLeasedJob("node-1", defaultPool), []string{"reservation-1"}),
			node:                        createNodeWithPoolAndReservation("node-1", defaultPool, "reservation-1"),
			poolConfig:                  ensureReservationMatchConfig,
			expectReconciliationFailure: false,
		},
		"reservation does not match - ensure reservation does not match": {
			job:                         withReservations(createLeasedJob("node-1", defaultPool), []string{"reservation-1"}),
			node:                        createNodeWithPoolAndReservation("node-1", defaultPool, "reservation-2"),
			poolConfig:                  ensureReservationMismatchConfig,
			expectReconciliationFailure: false,
		},
		"reconciliation success - no matching nodes": {
			job:                         createLeasedJob("node-1", defaultPool),
			node:                        createNodeWithPool("node-2", "updated"),
			poolConfig:                  reconciliationEnabledConfig,
			expectReconciliationFailure: false,
		},
		"gang job on deleted node - reconciliation failure": {
			job:                         createLeasedGangJob("node-1", defaultPool),
			node:                        createNodeWithPool("node-2", defaultPool),
			poolConfig:                  reconciliationEnabledConfig,
			expectReconciliationFailure: true,
		},
		"reconciliation success - pool mismatch - ignores queued job": {
			job:                         createQueuedJob("node-1", defaultPool),
			node:                        createNodeWithPool("node-1", "updated"),
			poolConfig:                  reconciliationEnabledConfig,
			expectReconciliationFailure: false,
		},
		"reconciliation success - pool mismatch - ignores terminated job": {
			job:                         createTerminalJob("node-1", defaultPool),
			node:                        createNodeWithPool("node-1", "updated"),
			poolConfig:                  reconciliationEnabledConfig,
			expectReconciliationFailure: false,
		},
		"reconciliation success - pool mismatch - pool reconciliation disabled": {
			job:                         createLeasedJob("node-1", defaultPool),
			node:                        createNodeWithPool("node-2", "updated"),
			poolConfig:                  reconciliationDisabledConfig,
			expectReconciliationFailure: false,
		},
		"pool mismatch": {
			job:                         createLeasedJob("node-1", defaultPool),
			node:                        createNodeWithPool("node-1", "updated"),
			poolConfig:                  reconciliationEnabledConfig,
			expectReconciliationFailure: true,
		},
		"reservation does not match - ensure reservation match": {
			job:                         withReservations(createLeasedJob("node-1", defaultPool), []string{"reservation-1"}),
			node:                        createNodeWithPoolAndReservation("node-1", defaultPool, "reservation-2"),
			poolConfig:                  ensureReservationMatchConfig,
			expectReconciliationFailure: true,
		},
		"reservation does match - ensure reservation does not match": {
			job:                         withReservations(createLeasedJob("node-1", defaultPool), []string{"reservation-1"}),
			node:                        createNodeWithPoolAndReservation("node-1", defaultPool, "reservation-1"),
			poolConfig:                  ensureReservationMismatchConfig,
			expectReconciliationFailure: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jobDb := testfixtures.NewJobDb(testfixtures.TestResourceListFactory)
			txn := jobDb.WriteTxn()
			err := txn.Upsert([]*jobdb.Job{tc.job})
			require.NoError(t, err)
			txn.Commit()

			executor := createExecutor("cluster-1", tc.node)

			reconciler := NewRunNodeReconciler([]configuration.PoolConfig{tc.poolConfig})
			result := reconciler.ReconcileJobRuns(jobDb.ReadTxn(), []*schedulerobjects.Executor{executor})
			assert.NoError(t, err)

			if tc.expectReconciliationFailure {
				assert.Len(t, result, 1)
				assert.Equal(t, tc.job, result[0].Job)
			} else {
				assert.Len(t, result, 0)
			}
		})
	}
}

func createQueuedJob(nodeId string, pool string) *jobdb.Job {
	job := testfixtures.Test1Cpu4GiJob("testQueue", testfixtures.PriorityClass6Preemptible).
		WithNewRun("testExecutor", nodeId, "node", pool, 5).WithQueued(true)
	job = job.WithUpdatedRun(job.LatestRun().WithReturned(true).WithFailed(true))
	return job
}

func createExecutor(clusterName string, nodes ...*schedulerobjects.Node) *schedulerobjects.Executor {
	return &schedulerobjects.Executor{
		Id:    clusterName,
		Pool:  testfixtures.TestPool,
		Nodes: nodes,
	}
}

func createTerminalJob(nodeId string, pool string) *jobdb.Job {
	job := testfixtures.Test1Cpu4GiJob("testQueue", testfixtures.PriorityClass6Preemptible).
		WithSucceeded(true).WithNewRun("testExecutor", nodeId, "node", pool, 5)
	job = job.WithUpdatedRun(job.LatestRun().WithSucceeded(true))
	return job
}

func createLeasedJob(nodeId string, pool string) *jobdb.Job {
	return testfixtures.Test1Cpu4GiJob("testQueue", testfixtures.PriorityClass6Preemptible).
		WithNewRun("testExecutor", nodeId, "node", pool, 5)
}

func createLeasedGangJob(nodeId string, pool string) *jobdb.Job {
	gangInfo := jobdb.CreateGangInfo("test-gang-id", 2, "")
	return testfixtures.Test1Cpu4GiJob("testQueue", testfixtures.PriorityClass6Preemptible).
		WithGangInfo(gangInfo).
		WithNewRun("testExecutor", nodeId, "node", pool, 5)
}

func withReservations(job *jobdb.Job, reservations []string) *jobdb.Job {
	schedulingInfo := job.JobSchedulingInfo()
	for _, reservation := range reservations {
		toleration := v1.Toleration{
			Key:      constants.ReservationTaintKey,
			Operator: v1.TolerationOpEqual,
			Value:    reservation,
			Effect:   v1.TaintEffectNoSchedule,
		}
		schedulingInfo.PodRequirements.Tolerations = append(schedulingInfo.PodRequirements.Tolerations, toleration)
	}
	job, err := job.WithJobSchedulingInfo(schedulingInfo)
	if err != nil {
		panic(err)
	}
	return job
}

func createNodeWithPool(nodeId string, pool string) *schedulerobjects.Node {
	return createNodeWithPoolAndReservation(nodeId, pool, "")
}

func createNodeWithPoolAndReservation(nodeId string, pool string, reservation string) *schedulerobjects.Node {
	node := testfixtures.TestSchedulerObjectsNode(
		testfixtures.TestPriorities,
		map[string]*k8sResource.Quantity{
			"cpu":    pointer.MustParseResource("32"),
			"memory": pointer.MustParseResource("256Gi"),
		},
	)
	node.Id = nodeId
	node.Pool = pool
	node.Reservation = reservation
	return node
}
