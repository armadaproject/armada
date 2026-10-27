package processors

import (
	"sync"

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
	debugRenderer    *reporter.DebugMessageRenderer

	// Run ids a debug event has already been reported for. A cancelled pod that is still
	// terminating stays in the cancel-requested set across ticks, and Run is called every
	// stateProcessorInterval. The event is diagnostic, so losing this across a restart is fine.
	reportedRunIds   map[string]struct{}
	reportedRunsLock sync.Mutex
}

func NewRemoveRunProcessor(
	clusterContext executorContext.ClusterContext,
	jobRunStateStore job.RunStateStore,
	eventReporter reporter.EventReporter,
	debugRenderer *reporter.DebugMessageRenderer,
) *RemoveRunProcessor {
	return &RemoveRunProcessor{
		clusterContext:   clusterContext,
		jobRunStateStore: jobRunStateStore,
		eventReporter:    eventReporter,
		debugRenderer:    debugRenderer,
		reportedRunIds:   map[string]struct{}{},
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
				j.forgetReportedRun(runInfo.Run.Meta.RunId)
				return
			}

			if util.IsPodFinishedAndReported(pod) {
				// Just delete it from internal state
				// Don't delete it from k8s as users may want to view the pod state
				j.jobRunStateStore.Delete(runInfo.Run.Meta.RunId)
				j.forgetReportedRun(runInfo.Run.Meta.RunId)
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

// The event is purely diagnostic and does not change the run's state (it remains cancelled) - only
// Lookout persists the message.
//
// It fires only when the main container never started rather than on every cancel, because scheduler
// preemption reaches the executor through the cancel list: emitting unconditionally would attach a
// multi-KiB payload to every preemption.
func (j *RemoveRunProcessor) reportDebugIfMainContainerNeverStarted(run *job.RunState, pod *v1.Pod) {
	if util.HasAppContainerStarted(pod) {
		return
	}
	if run.Meta.RunId == "" || j.hasReported(run.Meta.RunId) {
		return
	}

	podEvents, err := j.clusterContext.GetPodEvents(pod)
	if err != nil {
		log.Errorf("Failed retrieving pod events for cancelled pod %s: %v", pod.Name, err)
		return
	}

	// This event exists only to carry the payload, so there is nothing to report without one - which
	// is also how capture being disabled reaches here.
	debugMessage := j.debugRenderer.Render(pod, podEvents, reporter.TriggerCancelledNotStarted)
	if debugMessage == "" {
		return
	}
	debugEvent, err := reporter.CreateJobRunTerminatedDebugEvent(pod, debugMessage)
	if err != nil {
		log.Errorf("Failed creating debug event for cancelled pod %s: %v", pod.Name, err)
		return
	}
	if !j.claimRunForReporting(run.Meta.RunId) {
		return
	}

	err = j.eventReporter.Report([]reporter.EventMessage{{Event: debugEvent, JobRunId: run.Meta.RunId}})
	if err != nil {
		log.Errorf("Failed reporting debug event for cancelled pod %s: %v", pod.Name, err)
	}
}

func (j *RemoveRunProcessor) hasReported(runId string) bool {
	j.reportedRunsLock.Lock()
	defer j.reportedRunsLock.Unlock()

	_, reported := j.reportedRunIds[runId]
	return reported
}

// Returns true the first time it is called for a run id, so a pod that stays in the cancel-requested
// set across ticks produces exactly one debug event.
func (j *RemoveRunProcessor) claimRunForReporting(runId string) bool {
	j.reportedRunsLock.Lock()
	defer j.reportedRunsLock.Unlock()

	if _, reported := j.reportedRunIds[runId]; reported {
		return false
	}
	j.reportedRunIds[runId] = struct{}{}
	return true
}

func (j *RemoveRunProcessor) forgetReportedRun(runId string) {
	j.reportedRunsLock.Lock()
	defer j.reportedRunsLock.Unlock()
	delete(j.reportedRunIds, runId)
}
