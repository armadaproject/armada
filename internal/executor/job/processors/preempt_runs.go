package processors

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/context"
	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
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

	util.ProcessItemsWithThreadPool(context.Background(), 20, runPodInfos,
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
	preemptedEvent := reporter.CreateSimpleJobPreemptedEvent(pod, j.clusterContext.GetClusterId())
	failedEvent := reporter.CreateSimpleJobFailedEvent(pod, "Run preempted", j.clusterContext.GetClusterId(), api.Cause_Error)
	events := []reporter.EventMessage{
		{Event: preemptedEvent, JobRunId: run.Meta.RunId},
		{Event: failedEvent, JobRunId: run.Meta.RunId},
	}

	err := j.eventReporter.Report(events)
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
