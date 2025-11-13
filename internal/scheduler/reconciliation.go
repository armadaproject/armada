package scheduler

import (
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type FailedReconciliationResult struct {
	Job    *jobdb.Job
	Reason string
}

type JobRunNodeReconciler interface {
	ReconcileJobRunPools(ctx *armadacontext.Context, txn *jobdb.Txn) ([]*FailedReconciliationResult, error)
}

type RunNodeReconciler struct {
	poolsToReconcile   []configuration.PoolConfig
	executorRepository database.ExecutorRepository
	previousNodeState  atomic.Pointer[nodeState]
}

type nodeState struct {
	nodeInfos []*nodeInfo
}

type nodeInfo struct {
	node        *schedulerobjects.Node
	reservation string
}

func NewRunNodeReconciler(poolConfigs []configuration.PoolConfig, executorRepository database.ExecutorRepository) *RunNodeReconciler {
	poolsToReconcile := armadaslices.Filter(poolConfigs, func(p configuration.PoolConfig) bool {
		return p.ExperimentalRunReconciliation != nil && p.ExperimentalRunReconciliation.Enabled
	})

	return &RunNodeReconciler{
		poolsToReconcile:   poolsToReconcile,
		executorRepository: executorRepository,
		previousNodeState:  atomic.Pointer[nodeState]{},
	}
}

func (r *RunNodeReconciler) ReconcileJobRunPools(ctx *armadacontext.Context, txn *jobdb.Txn) ([]*FailedReconciliationResult, error) {
	latestNodeInfos, err := r.getLatestNodeInfo(ctx)
	if err != nil {
		return nil, err
	}
	// This is purely an optimisation so we don't need to get/reconcile every job each time
	// If no nodes have been updated, we can assume no jobs need reconciling
	updatedNodes := r.getUpdatedNodes(latestNodeInfos)
	if len(updatedNodes) == 0 {
		r.previousNodeState.Store(&nodeState{nodeInfos: latestNodeInfos})
		return nil, nil
	}

	jobsToReconcile := r.getJobsToReconcileByNodeId(txn)
	configByPool := poolConfigSliceToMap(r.poolsToReconcile)

	var result []*FailedReconciliationResult
	for _, node := range updatedNodes {
		jobsOnNode := jobsToReconcile[node.node.Id]

		for _, job := range jobsOnNode {
			run := job.LatestRun()
			config, present := configByPool[run.Pool()]
			if !present {
				continue
			}
			runPools := []string{run.Pool()}
			if len(config.AwayPools) > 0 {
				runPools = append(runPools, config.AwayPools...)
			}

			if !slices.Contains(runPools, node.node.Pool) {
				failedReconciliationResult := &FailedReconciliationResult{
					Job: job,
					Reason: fmt.Sprintf("The pool of node %s has been changed from %s to %s - this jobs placement is now invalid",
						node.node.Name, run.Pool(), node.node.Pool),
				}

				result = append(result, failedReconciliationResult)
			} else if config.ExperimentalRunReconciliation.EnsureReservationMatch && !job.MatchesReservation(node.reservation) {
				failedReconciliationResult := &FailedReconciliationResult{
					Job: job,
					Reason: fmt.Sprintf("The reservation of node %s has been changed and is now %s - this job no longer matches the node reservation",
						node.node.Name, node.reservation),
				}

				result = append(result, failedReconciliationResult)
			} else if config.ExperimentalRunReconciliation.EnsureReservationDoesNotMatch && job.MatchesReservation(node.reservation) {
				failedReconciliationResult := &FailedReconciliationResult{
					Job: job,
					Reason: fmt.Sprintf("The reservation of node %s has been changed and is now %s - this job is now incorrectly running away on a node with a matching resevation",
						node.node.Name, node.reservation),
				}

				result = append(result, failedReconciliationResult)
			}
		}
	}

	r.previousNodeState.Store(&nodeState{nodeInfos: latestNodeInfos})
	return result, nil
}

func poolConfigSliceToMap(config []configuration.PoolConfig) map[string]configuration.PoolConfig {
	return maps.FromSlice(config,
		func(p configuration.PoolConfig) string {
			return p.Name
		}, func(p configuration.PoolConfig) configuration.PoolConfig {
			return p
		},
	)
}

func (r *RunNodeReconciler) getUpdatedNodes(latestNodeInfos []*nodeInfo) []*nodeInfo {
	updated := []*nodeInfo{}

	previousNodeInfos := []*nodeInfo{}
	previousNodeState := r.previousNodeState.Load()
	if previousNodeState != nil {
		previousNodeInfos = previousNodeState.nodeInfos
	}

	existingNodesById := make(map[string]*nodeInfo, len(previousNodeInfos))
	for _, node := range previousNodeInfos {
		if node.node == nil {
			continue
		}
		existingNodesById[node.node.Id] = node
	}

	for _, node := range latestNodeInfos {
		if node.node == nil {
			continue
		}
		previous, present := existingNodesById[node.node.Id]
		if !present {
			updated = append(updated, node)
		} else if node.node.Pool != previous.node.Pool || node.reservation != previous.reservation {
			updated = append(updated, node)
		}
	}

	return updated
}

func (r *RunNodeReconciler) getLatestNodeInfo(ctx *armadacontext.Context) ([]*nodeInfo, error) {
	executors, err := r.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}

	nodes := []*nodeInfo{}
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			nodes = append(nodes, &nodeInfo{
				node:        node,
				reservation: node.ReservationName(),
			})
		}
	}

	return nodes, nil
}

func (r *RunNodeReconciler) getJobsToReconcileByNodeId(txn *jobdb.Txn) map[string][]*jobdb.Job {
	poolsToReconcile := armadaslices.Map(r.poolsToReconcile, func(p configuration.PoolConfig) string {
		return p.Name
	})

	activeJobByNodeId := map[string][]*jobdb.Job{}
	// TODO make more efficient by having an index of running jobs (or running jobs by pool / all jobs by pool)
	jobs := txn.GetAll()
	for _, job := range jobs {
		if job.InTerminalState() || job.LatestRun() == nil || job.LatestRun().InTerminalState() {
			continue
		}

		run := job.LatestRun()
		// We only want to validate jobs for specific pools
		// This should be the scheduling pool and not the pool the node is now on
		if !slices.Contains(poolsToReconcile, run.Pool()) {
			continue
		}

		if _, present := activeJobByNodeId[job.LatestRun().NodeId()]; !present {
			activeJobByNodeId[job.LatestRun().NodeId()] = []*jobdb.Job{}
		}

		activeJobByNodeId[job.LatestRun().NodeId()] = append(activeJobByNodeId[job.LatestRun().NodeId()], job)
	}

	return activeJobByNodeId
}
