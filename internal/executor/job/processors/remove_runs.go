package processors

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
)

type RemoveRunProcessor struct {
	clusterContext   executorContext.ClusterContext
	jobRunStateStore job.RunStateStore
}

func NewRemoveRunProcessor(clusterContext executorContext.ClusterContext, jobRunStateStore job.RunStateStore) *RemoveRunProcessor {
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

	runsToCancel := j.jobRunStateStore.GetAllWithFilter(func(state *job.RunState) bool {
		return state.CancelRequested
	})
	runPodInfos := createRunPodInfos(runsToCancel, managedPods)

	util.ProcessItemsWithThreadPool(armadacontext.Background(), 20, runPodInfos,
		func(runInfo *runPodInfo) {
			pod := runInfo.Pod
			if pod == nil {
				j.jobRunStateStore.Delete(runInfo.Run.Meta.RunId)
				return
			}

			if util.IsPodFinishedAndReported(pod) {
				// Just delete it from internal state
				// Don't delete it from k8s as users may want to view the pod state
				j.jobRunStateStore.Delete(runInfo.Run.Meta.RunId)
			} else {
				// This path should only happen during cancellation, so delete the pod
				j.clusterContext.DeletePods([]*v1.Pod{pod})
			}
		},
	)
}
