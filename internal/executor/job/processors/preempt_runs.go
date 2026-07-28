package processors

import (
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type RunPreemptedProcessor struct {
	clusterContext   executorContext.ClusterContext
	jobRunStateStore job.RunStateStore
	eventReporter    reporter.EventReporter
}

func NewRunPreemptedProcessor(
	clusterContext executorContext.ClusterContext,
	jobRunStateStore job.RunStateStore,
	eventReporter reporter.EventReporter,
) *RunPreemptedProcessor {
	return &RunPreemptedProcessor{
		clusterContext:   clusterContext,
		jobRunStateStore: jobRunStateStore,
		eventReporter:    eventReporter,
	}
}

func (j *RunPreemptedProcessor) Run() {
	managedPods, err := j.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to cancel runs because unable to get a current managed pods due to %s", err)
		return
	}

	runsToCancel := j.jobRunStateStore.GetAllWithFilter(func(state *job.RunState) bool {
		return state.PreemptionRequested
	})
	runPodInfos := createRunPodInfos(runsToCancel, managedPods)

	util.ProcessItemsWithThreadPool(armadacontext.Background(), 20, runPodInfos,
		func(runInfo *runPodInfo) {
			pod := runInfo.Pod
			if pod == nil {
				// No pod to preempt
				return
			}

			if util.IsInTerminalState(pod) {
				// If pod is already finished, nothing to preempt
				return
			}

			if !util.IsReportedPreempted(pod) {
				err := j.reportPodPreempted(runInfo.Run, runInfo.Pod)
				if err != nil {
					log.Errorf("failed to report run (runId = %s, jobId = %s) preempted because %s ",
						runInfo.Run.Meta.RunId, runInfo.Run.Meta.JobId, err)
					return
				}
			}

			j.clusterContext.DeletePods([]*v1.Pod{pod})
		},
	)
}

func (j *RunPreemptedProcessor) reportPodPreempted(run *job.RunState, pod *v1.Pod) error {
	preemptedEvent, err := reporter.CreateSimpleJobPreemptedEvent(pod)
	if err != nil {
		return fmt.Errorf("failed creating preempted event because - %s", err)
	}

	// If the run is preempted before its main container ever started, record the k8s events
	// as debug data so the reason the workload never ran isn't lost.
	debugMessage := j.debugMessageIfMainContainerNeverStarted(pod)
	failedEvent, err := reporter.CreateJobFailedEvent(pod, "Run preempted", armadaevents.KubernetesReason_AppError,
		debugMessage, []*armadaevents.ContainerError{}, j.clusterContext.GetClusterId(), "", "")
	if err != nil {
		return fmt.Errorf("failed creating failed event because - %s", err)
	}
	events := []reporter.EventMessage{
		{Event: preemptedEvent, JobRunId: run.Meta.RunId},
		{Event: failedEvent, JobRunId: run.Meta.RunId},
	}

	err = j.eventReporter.Report(events)
	if err != nil {
		return fmt.Errorf("failed reporting preempted events because - %s", err)
	}

	err = j.clusterContext.AddAnnotation(pod, map[string]string{
		domain.JobPreemptedAnnotation: time.Now().String(),
		string(v1.PodFailed):          time.Now().String(),
	})
	if err != nil {
		return fmt.Errorf("failed to annotate pod as preempted - %s", err)
	}
	return nil
}

// debugMessageIfMainContainerNeverStarted returns the rendered k8s pod events when the pod's
// main container never started, otherwise an empty string.
func (j *RunPreemptedProcessor) debugMessageIfMainContainerNeverStarted(pod *v1.Pod) string {
	if util.HasAppContainerStarted(pod) {
		return ""
	}
	podEvents, err := j.clusterContext.GetPodEvents(pod)
	if err != nil {
		log.Errorf("Failed retrieving pod events for preempted pod %s: %v", pod.Name, err)
		return ""
	}
	// No k8s events means there is nothing useful to record - avoid a debug string that would
	// render to just "Events: <none>" (common for preemption, where the pod is killed to free capacity).
	if len(podEvents) == 0 {
		return ""
	}
	return reporter.CreateDebugMessage(podEvents)
}
