package processors

import (
	"context"
	"fmt"
	"time"

	executorContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
)

type RunPreemptedProcessor struct {
	clusterContext   executorContext.ClusterContext
	jobRunStateStore job.RunStateStore
	eventReporter    reporter.EventReporter
}

func NewRunPreemptedProcessor(
	clusterContext executorContext.ClusterContext,
	jobRunStateStore job.RunStateStore,
	eventReporter reporter.EventReporter) *RunPreemptedProcessor {
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
					log.Error(err)
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
		return fmt.Errorf("Failed reporting preempted events for job %s run %s because %s",
			run.Meta.JobId, run.Meta.RunId, err)
	}

	err = j.clusterContext.AddAnnotation(pod, map[string]string{
		domain.JobPreemptedAnnotation: time.Now().String(),
		string(v1.PodFailed):          time.Now().String(),
	})

	if err != nil {
		return fmt.Errorf("Failed to annotate pod %s because %s", pod.Name, err)
	}
	return nil
}
