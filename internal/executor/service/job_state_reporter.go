package service

import (
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	domain2 "github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter"
	"github.com/armadaproject/armada/internal/executor/util"
)

type JobStateReporter struct {
	eventReporter    reporter.EventReporter
	jobRunStateStore *job.JobRunStateStore
	clusterContext   clusterContext.ClusterContext
	podIssueHandler  IssueHandler
}

func NewJobStateReporter(
	clusterContext clusterContext.ClusterContext,
	jobRunState *job.JobRunStateStore,
	eventReporter reporter.EventReporter,
	podIssueHandler IssueHandler,
) (*JobStateReporter, error) {
	stateReporter := &JobStateReporter{
		eventReporter:    eventReporter,
		clusterContext:   clusterContext,
		jobRunStateStore: jobRunState,
		podIssueHandler:  podIssueHandler,
	}

	_, err := clusterContext.AddPodEventHandler(stateReporter.podEventHandler())
	if err != nil {
		return nil, err
	}

	return stateReporter, nil
}

func (stateReporter *JobStateReporter) podEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			go stateReporter.reportCurrentStatus(pod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok := oldObj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", oldObj)
				return
			}
			newPod, ok := newObj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", newObj)
				return
			}
			go stateReporter.reportStatusUpdate(oldPod, newPod)
		},
	}
}

func (stateReporter *JobStateReporter) reportStatusUpdate(old *v1.Pod, new *v1.Pod) {
	// Don't report status if the pod phase didn't change
	if old.Status.Phase == new.Status.Phase {
		return
	}
	// Don't report status change for pods Armada is deleting
	// This prevents reporting JobFailed when we delete a pod - for example due to cancellation
	if util.IsMarkedForDeletion(new) {
		log.Infof("not sending event to report pod %s moving into phase %s as pod is marked for deletion", new.Name, new.Status.Phase)
		return
	}
	stateReporter.reportCurrentStatus(new)
}

func (stateReporter *JobStateReporter) reportCurrentStatus(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}
	if util.HasCurrentStateBeenReported(pod) {
		return
	}

	event, err := reporter.CreateEventForCurrentState(pod, stateReporter.clusterContext.GetClusterId())
	if pod.Status.Phase == v1.PodFailed {
		hasIssue := stateReporter.podIssueHandler.HasIssue(util.ExtractJobRunId(pod))
		if hasIssue {
			// Pod already being handled by issue handler
			return
		}
		issueAdded, err := stateReporter.podIssueHandler.DetectAndRegisterFailedPodIssue(pod)
		if issueAdded {
			// Pod already being handled by issue handler
			return
		}
		if err != nil {
			log.Errorf("Failed detecting issue on failed pod %s(%s) - %v", pod.Name, util.ExtractJobRunId(pod), err)
			// Don't return here, as it is very important we don't block reporting a terminal event (failed)
		}
	}

	if err != nil {
		log.Errorf("Failed to report event: %v", err)
		return
	}

	stateReporter.eventReporter.QueueEvent(reporter.EventMessage{Event: event, JobRunId: util.ExtractJobRunId(pod)}, func(err error) {
		if err != nil {
			log.Errorf("Failed to report event: %s", err)
			return
		}

		if util.IsReportingPhaseRequired(pod.Status.Phase) {
			err = stateReporter.addAnnotationToMarkStateReported(pod)
			if err != nil {
				log.Errorf("Failed to add state annotation %s to pod %s: %v", string(pod.Status.Phase), pod.Name, err)
				return
			}
		}
	})

	if pod.Status.Phase == v1.PodRunning && requiresIngressToBeReported(pod) {
		stateReporter.attemptToReportIngressInfoEvent(pod)
	}
}

func (stateReporter *JobStateReporter) addAnnotationToMarkStateReported(pod *v1.Pod) error {
	annotations := make(map[string]string)
	annotationName := string(pod.Status.Phase)
	annotations[annotationName] = time.Now().String()

	return stateReporter.clusterContext.AddAnnotation(pod, annotations)
}

func (stateReporter *JobStateReporter) addAnnotationToMarkIngressReported(pod *v1.Pod) error {
	annotations := make(map[string]string)
	annotationName := domain2.IngressReported
	annotations[annotationName] = time.Now().String()

	return stateReporter.clusterContext.AddAnnotation(pod, annotations)
}

func (stateReporter *JobStateReporter) ReportMissingJobEvents() {
	allBatchPods, err := stateReporter.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to reconcile missing job events: %v", err)
		return
	}
	podsWithCurrentPhaseNotReported := filterPodsWithCurrentStateNotReported(allBatchPods)

	for _, pod := range podsWithCurrentPhaseNotReported {
		if util.IsReportingPhaseRequired(pod.Status.Phase) && !stateReporter.eventReporter.HasPendingEvents(pod) {
			stateReporter.reportCurrentStatus(pod)
		}
	}

	podWithIngressNotReported := util.FilterPods(allBatchPods, func(pod *v1.Pod) bool {
		return pod.Status.Phase == v1.PodRunning &&
			requiresIngressToBeReported(pod) &&
			util.HasPodBeenInStateForLongerThanGivenDuration(pod, 15*time.Second)
	})

	for _, pod := range podWithIngressNotReported {
		if !stateReporter.eventReporter.HasPendingEvents(pod) {
			stateReporter.attemptToReportIngressInfoEvent(pod)
		}
	}
}

func (stateReporter *JobStateReporter) attemptToReportIngressInfoEvent(pod *v1.Pod) {
	expectedNumberOfServices := util.GetExpectedNumberOfAssociatedServices(pod)
	expectedNumberOfIngresses := util.GetExpectedNumberOfAssociatedIngresses(pod)
	associatedServices, err := stateReporter.clusterContext.GetServices(pod)
	if err != nil {
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s: %v", pod.Name, err)
		return
	}
	associatedIngresses, err := stateReporter.clusterContext.GetIngresses(pod)
	if err != nil {
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s: %v", pod.Name, err)
		return
	}
	if len(associatedServices) != expectedNumberOfServices || len(associatedIngresses) != expectedNumberOfIngresses {
		log.Warnf("Not reporting JobIngressInfoEvent for pod %s because not all expected associated services "+
			"(current %d, expected %d) or ingresses (current %d, expected %d) exist yet",
			pod.Name, len(associatedServices), expectedNumberOfServices, len(associatedIngresses), expectedNumberOfIngresses)
		// Don't report ingress info until all expected ingresses exist
		return
	}

	ingressInfoEvent, err := reporter.CreateJobIngressInfoEvent(pod, stateReporter.clusterContext.GetClusterId(), associatedServices, associatedIngresses)
	if err != nil {
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s: %v", pod.Name, err)
		return
	}
	stateReporter.eventReporter.QueueEvent(reporter.EventMessage{Event: ingressInfoEvent, JobRunId: util.ExtractJobRunId(pod)}, func(err error) {
		if err != nil {
			log.Errorf("Failed to report event JobIngressInfoEvent for pod %s: %v", pod.Name, err)
			return
		}

		err = stateReporter.addAnnotationToMarkIngressReported(pod)
		if err != nil {
			log.Errorf("Failed to add ingress reported annotation %s to pod %s: %v", string(pod.Status.Phase), pod.Name, err)
			return
		}
	})
}

func requiresIngressToBeReported(pod *v1.Pod) bool {
	if !util.HasIngress(pod) {
		return false
	}
	if _, exists := pod.Annotations[domain2.IngressReported]; exists {
		return false
	}
	return true
}

func filterPodsWithCurrentStateNotReported(pods []*v1.Pod) []*v1.Pod {
	podsWithMissingEvent := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if !util.HasCurrentStateBeenReported(pod) && util.HasPodBeenInStateForLongerThanGivenDuration(pod, 30*time.Second) {
			podsWithMissingEvent = append(podsWithMissingEvent, pod)
		}
	}
	return podsWithMissingEvent
}
