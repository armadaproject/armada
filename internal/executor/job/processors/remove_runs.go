package processors

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
)

type RemoveRunProcessor struct {
	clusterContext   executorContext.ClusterContext
	jobRunStateStore job.RunStateStore
	eventReporter    reporter.EventReporter
}

func NewRemoveRunProcessor(
	clusterContext executorContext.ClusterContext,
	jobRunStateStore job.RunStateStore,
	eventReporter reporter.EventReporter,
) *RemoveRunProcessor {
	return &RemoveRunProcessor{
		clusterContext:   clusterContext,
		jobRunStateStore: jobRunStateStore,
		eventReporter:    eventReporter,
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
				// If the pod is being cancelled before its main container ever started, record the
				// k8s events as debug data first - otherwise the reason the workload never ran is lost
				// (e.g. a gang member that never got scheduled while the rest of the gang timed out).
				j.reportDebugIfMainContainerNeverStarted(runInfo.Run, pod)
				j.clusterContext.DeletePods([]*v1.Pod{pod})
			}
		},
	)
}

// reportDebugIfMainContainerNeverStarted emits a JobCancelledDebugInfo event carrying the
// rendered k8s pod events, so the debug data survives the cancellation. The event is purely
// diagnostic and does not change the run's state (it remains cancelled) - only Lookout persists
// the message.
func (j *RemoveRunProcessor) reportDebugIfMainContainerNeverStarted(run *job.RunState, pod *v1.Pod) {
	if util.HasAppContainerStarted(pod) {
		return
	}

	podEvents, err := j.clusterContext.GetPodEvents(pod)
	if err != nil {
		log.Errorf("Failed retrieving pod events for cancelled pod %s: %v", pod.Name, err)
		return
	}
	// No k8s events means there is nothing useful to record - skip rather than emit a debug
	// event that would render to just "Events: <none>".
	if len(podEvents) == 0 {
		return
	}

	debugMessage := reporter.CreateDebugMessage(podEvents)
	debugEvent, err := reporter.CreateJobRunCancelledDebugEvent(pod, debugMessage)
	if err != nil {
		log.Errorf("Failed creating debug event for cancelled pod %s: %v", pod.Name, err)
		return
	}

	err = j.eventReporter.Report([]reporter.EventMessage{{Event: debugEvent, JobRunId: run.Meta.RunId}})
	if err != nil {
		log.Errorf("Failed reporting debug event for cancelled pod %s: %v", pod.Name, err)
	}
}
