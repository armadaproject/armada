package scheduling

import (
	"fmt"
	"slices"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type JobRunNodeReconciler interface {
	ReconcileJobRuns(txn *jobdb.Txn, executors []*schedulerobjects.Executor) []*FailedReconciliationResult
}

type RunNodeReconciler struct {
	poolsToReconcile []configuration.PoolConfig
}

func NewRunNodeReconciler(poolConfigs []configuration.PoolConfig) *RunNodeReconciler {
	poolsToReconcile := armadaslices.Filter(poolConfigs, func(p configuration.PoolConfig) bool {
		return p.ExperimentalRunReconciliation != nil && p.ExperimentalRunReconciliation.Enabled
	})

	return &RunNodeReconciler{
		poolsToReconcile: poolsToReconcile,
	}
}

func (r *RunNodeReconciler) ReconcileJobRuns(txn *jobdb.Txn, executors []*schedulerobjects.Executor) []*FailedReconciliationResult {
	if len(r.poolsToReconcile) == 0 {
		return nil
	}

	nodes := r.getNodes(executors)
	jobsToReconcileByNodeId := r.getJobsToReconcileByNodeId(txn)

	configByPool := poolConfigSliceToMap(r.poolsToReconcile)
	nodeIdSet := r.buildNodeIdSet(nodes)

	var result []*FailedReconciliationResult

	result = append(result, r.checkJobsOnDeletedNodes(jobsToReconcileByNodeId, nodeIdSet, configByPool)...)
	result = append(result, r.checkJobsOnExistingNodes(nodes, jobsToReconcileByNodeId, configByPool)...)

	return result
}

func (r *RunNodeReconciler) checkJobsOnDeletedNodes(jobsToReconcileByNodeId map[string][]*jobdb.Job, nodeIdSet map[string]bool, configByPool map[string]configuration.PoolConfig) []*FailedReconciliationResult {
	var result []*FailedReconciliationResult

	for nodeId, jobs := range jobsToReconcileByNodeId {
		if nodeIdSet[nodeId] {
			continue
		}
		for _, job := range jobs {
			run := job.LatestRun()
			if _, present := configByPool[run.Pool()]; !present {
				continue
			}

			reason := fmt.Sprintf("The node %s no longer exists - this job's placement is now invalid", nodeId)

			if job.IsInGang() {
				failedReconciliationResult := &FailedReconciliationResult{
					Job:    job,
					Reason: reason,
				}
				result = append(result, failedReconciliationResult)
			} else {
				log.Warnf("Non-gang job %s on deleted node: %s", job.Id(), reason)
			}
		}
	}

	return result
}

func (r *RunNodeReconciler) checkJobsOnExistingNodes(nodes []*schedulerobjects.Node, jobsToReconcileByNodeId map[string][]*jobdb.Job, configByPool map[string]configuration.PoolConfig) []*FailedReconciliationResult {
	var result []*FailedReconciliationResult

	for _, node := range nodes {
		jobsOnNode := jobsToReconcileByNodeId[node.GetId()]

		for _, job := range jobsOnNode {
			if failedResult := r.checkJobNodeMatch(job, node, configByPool); failedResult != nil {
				result = append(result, failedResult)
			}
		}
	}

	return result
}

func (r *RunNodeReconciler) checkJobNodeMatch(job *jobdb.Job, node *schedulerobjects.Node, configByPool map[string]configuration.PoolConfig) *FailedReconciliationResult {
	run := job.LatestRun()
	config, present := configByPool[run.Pool()]
	if !present {
		return nil
	}

	runPools := []string{run.Pool()}
	if len(config.AwayPools) > 0 {
		runPools = append(runPools, config.AwayPools...)
	}

	if !slices.Contains(runPools, node.GetPool()) {
		return &FailedReconciliationResult{
			Job: job,
			Reason: fmt.Sprintf("The pool of node %s has been changed from %s to %s - this job's placement is now invalid",
				node.GetName(), run.Pool(), node.GetPool()),
		}
	}

	if config.ExperimentalRunReconciliation.EnsureReservationMatch && !job.MatchesReservation(node.GetReservation()) {
		return &FailedReconciliationResult{
			Job: job,
			Reason: fmt.Sprintf("The reservation of node %s has been changed and is now %s - this job no longer matches the node reservation",
				node.GetName(), node.GetReservation()),
		}
	}

	if config.ExperimentalRunReconciliation.EnsureReservationDoesNotMatch && job.MatchesReservation(node.GetReservation()) {
		return &FailedReconciliationResult{
			Job: job,
			Reason: fmt.Sprintf("The reservation of node %s has been changed and is now %s - this job is now incorrectly running away on a node with a matching reservation",
				node.GetName(), node.GetReservation()),
		}
	}

	return nil
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

func (r *RunNodeReconciler) getNodes(executors []*schedulerobjects.Executor) []*schedulerobjects.Node {
	nodes := []*schedulerobjects.Node{}
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			nodes = append(nodes, node)
		}
	}

	return nodes
}

func (r *RunNodeReconciler) buildNodeIdSet(nodes []*schedulerobjects.Node) map[string]bool {
	nodeIdSet := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		nodeIdSet[node.GetId()] = true
	}
	return nodeIdSet
}

func (r *RunNodeReconciler) getJobsToReconcileByNodeId(txn *jobdb.Txn) map[string][]*jobdb.Job {
	poolsToReconcile := armadaslices.Map(r.poolsToReconcile, func(p configuration.PoolConfig) string {
		return p.Name
	})

	activeJobByNodeId := map[string][]*jobdb.Job{}
	// TODO make more efficient by having an index of running jobs (or running jobs by pool / all jobs by pool)
	jobs := txn.GetAll()
	for _, job := range jobs {
		if job.InTerminalState() || job.Queued() || job.LatestRun() == nil {
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
