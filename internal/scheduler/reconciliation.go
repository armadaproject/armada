package scheduler

import (
	"fmt"
	"slices"

	"github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
)

type JobRunNodeReconciler interface {
	ReconcileJobRuns(jobs []*jobdb.Job, executors []*schedulerobjects.Executor) []*scheduling.FailedReconciliationResult
}

type RunNodeReconciler struct {
	poolsToReconcile   []configuration.PoolConfig
	executorRepository database.ExecutorRepository
}

func NewRunNodeReconciler(poolConfigs []configuration.PoolConfig, executorRepository database.ExecutorRepository) *RunNodeReconciler {
	poolsToReconcile := armadaslices.Filter(poolConfigs, func(p configuration.PoolConfig) bool {
		return p.ExperimentalRunReconciliation != nil && p.ExperimentalRunReconciliation.Enabled
	})

	return &RunNodeReconciler{
		poolsToReconcile:   poolsToReconcile,
		executorRepository: executorRepository,
	}
}

func (r *RunNodeReconciler) ReconcileJobRuns(jobs []*jobdb.Job, executors []*schedulerobjects.Executor) []*scheduling.FailedReconciliationResult {
	if len(r.poolsToReconcile) == 0 {
		return nil
	}

	nodes := r.getNodes(executors)
	jobsToReconcile := r.getJobsToReconcileByNodeId(jobs)

	configByPool := poolConfigSliceToMap(r.poolsToReconcile)

	var result []*scheduling.FailedReconciliationResult
	for _, node := range nodes {
		jobsOnNode := jobsToReconcile[node.GetId()]

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

			if !slices.Contains(runPools, node.GetPool()) {
				failedReconciliationResult := &scheduling.FailedReconciliationResult{
					Job: job,
					Reason: fmt.Sprintf("The pool of node %s has been changed from %s to %s - this jobs placement is now invalid",
						node.GetName(), run.Pool(), node.GetPool()),
				}

				result = append(result, failedReconciliationResult)
			} else if config.ExperimentalRunReconciliation.EnsureReservationMatch && !job.MatchesReservation(node.GetReservation()) {
				failedReconciliationResult := &scheduling.FailedReconciliationResult{
					Job: job,
					Reason: fmt.Sprintf("The reservation of node %s has been changed and is now %s - this job no longer matches the node reservation",
						node.GetName(), node.GetReservation()),
				}

				result = append(result, failedReconciliationResult)
			} else if config.ExperimentalRunReconciliation.EnsureReservationDoesNotMatch && job.MatchesReservation(node.GetReservation()) {
				failedReconciliationResult := &scheduling.FailedReconciliationResult{
					Job: job,
					Reason: fmt.Sprintf("The reservation of node %s has been changed and is now %s - this job is now incorrectly running away on a node with a matching resevation",
						node.GetName(), node.GetReservation()),
				}

				result = append(result, failedReconciliationResult)
			}
		}
	}

	return result
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

func (r *RunNodeReconciler) getJobsToReconcileByNodeId(jobs []*jobdb.Job) map[string][]*jobdb.Job {
	poolsToReconcile := armadaslices.Map(r.poolsToReconcile, func(p configuration.PoolConfig) string {
		return p.Name
	})

	activeJobByNodeId := map[string][]*jobdb.Job{}
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
