package service

import (
	"context"
	"time"

	"github.com/armadaproject/armada/internal/common/slices"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

type JobRunRemover struct {
	clusterContext   executorContext.ClusterContext
	jobRunStateStore *job.JobRunStateStore
}

func NewJobRunRemover(clusterContext executorContext.ClusterContext, jobRunStateStore *job.JobRunStateStore) *JobRunRemover {
	return &JobRunRemover{
		clusterContext:   clusterContext,
		jobRunStateStore: jobRunStateStore,
	}
}

func (j *JobRunRemover) processRunsToCancel() {
	// Get job runs to cancel from state
	//runsToRemoveSet := util2.StringListToSet()
	runsToRemoveIds := make([]string, 0, 10)
	runsToRemoveSet := make(map[string]bool)

	managedPods, err := j.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to cancel runs because unable to get a current managed pods due to %s", err)
		return
	}

	// Find all runs with a pod
	podsToRemove := make([]*v1.Pod, 0, len(runsToRemoveSet))
	for _, pod := range managedPods {
		runId := util.ExtractJobRunId(pod)
		if _, ok := runsToRemoveSet[runId]; ok {
			podsToRemove = append(podsToRemove, pod)
		}
	}

	// Annotate corresponding pods with JobDoneAnnotation
	// Then update the runs state
	util.ProcessItemsWithThreadPool(context.Background(), 20, podsToRemove,
		func(pod *v1.Pod) {
			if util.IsInTerminalState(pod) && util.HasCurrentStateBeenReported(pod) {
				if !util.IsReportedDone(pod) {
					err := j.clusterContext.AddAnnotation(pod, map[string]string{
						domain.JobDoneAnnotation: time.Now().String(),
					})
					if err != nil {
						log.Errorf("Failed to annotate pod %s as done because %s", pod.Name, err)
						return
					}
				}

				runId := util.ExtractJobRunId(pod)
				j.jobRunStateStore.Delete(runId)
			} else {
				// This path should only happen during cancellation
				j.clusterContext.DeletePods(podsToRemove)
			}
		},
	)
	// For all runs that don't have a corresponding pod, delete the run from the state
	runsWithPods := slices.Map(podsToRemove, func(pod *v1.Pod) string {
		return util.ExtractJobRunId(pod)
	})

	runsToDelete := slices.Subtract(runsToRemoveIds, runsWithPods)

	for _, runToDelete := range runsToDelete {
		j.jobRunStateStore.Delete(runToDelete)
	}
}
