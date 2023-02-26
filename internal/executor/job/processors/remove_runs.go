package processors

import (
	"context"
	"time"

	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

type RemoveRunProcessor struct {
	clusterContext   executorContext.ClusterContext
	jobRunStateStore *job.JobRunStateStore
}

func NewRemoveRunProcessor(clusterContext executorContext.ClusterContext, jobRunStateStore *job.JobRunStateStore) *RemoveRunProcessor {
	return &RemoveRunProcessor{
		clusterContext:   clusterContext,
		jobRunStateStore: jobRunStateStore,
	}
}

func (j *RemoveRunProcessor) Run() {
	managedPods, err := j.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to cancel runs because unable to get a current managed pods due to %s", err)
		return
	}

	runsToCancel := j.jobRunStateStore.GetWithFilter(func(state *job.RunState) bool {
		return state.CancelRequested
	})
	runPodInfos := createRunPodInfos(runsToCancel, managedPods)

	util.ProcessItemsWithThreadPool(context.Background(), 20, runPodInfos,
		func(runInfo *runPodInfo) {
			pod := runInfo.Pod
			if pod == nil {
				j.jobRunStateStore.Delete(runInfo.Run.Meta.RunId)
				return
			}

			if util.IsInTerminalState(pod) && util.HasCurrentStateBeenReported(pod) {
				// If pod is already finished, don't delete it so users can view pods
				// Annotate it with done and remove the run from the state
				if !util.IsReportedDone(pod) {
					err := j.clusterContext.AddAnnotation(pod, map[string]string{
						domain.JobDoneAnnotation: time.Now().String(),
					})
					if err != nil {
						log.Errorf("Failed to annotate pod %s as done because %s", pod.Name, err)
						return
					}
				} else {
					// Only delete it from state once it reported done in our internal pod cache
					j.jobRunStateStore.Delete(runInfo.Run.Meta.RunId)
				}
			} else {
				// This path should only happen during cancellation, so delete the pod
				j.clusterContext.DeletePods([]*v1.Pod{pod})
			}
		},
	)
}
