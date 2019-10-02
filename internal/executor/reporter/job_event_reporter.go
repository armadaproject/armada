package reporter

import (
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor/context"
	"github.com/G-Research/k8s-batch/internal/executor/util"
)

type EventReporter interface {
	Report(event api.Event) error
	ReportCurrentStatus(pod *v1.Pod)
	ReportStatusUpdate(old *v1.Pod, new *v1.Pod)
}

type JobEventReporter struct {
	eventClient    api.EventClient
	clusterId      string
	clusterContext *context.KubernetesClusterContext
}

func NewJobEventReporter(
	eventClient api.EventClient,
	clusterId string,
	clusterContext *context.KubernetesClusterContext) *JobEventReporter {

	reporter := &JobEventReporter{eventClient: eventClient, clusterId: clusterId, clusterContext: clusterContext}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			go reporter.ReportCurrentStatus(pod)
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
			go reporter.ReportStatusUpdate(oldPod, newPod)
		},
	})

	return reporter
}

func (eventReporter JobEventReporter) Report(event api.Event) error {
	return eventReporter.sendEvent(event)
}

func (eventReporter JobEventReporter) ReportCurrentStatus(pod *v1.Pod) {
	eventReporter.report(pod)
}

func (eventReporter JobEventReporter) ReportStatusUpdate(old *v1.Pod, new *v1.Pod) {
	if old.Status.Phase == new.Status.Phase {
		return
	}
	eventReporter.report(new)
}

func (eventReporter JobEventReporter) report(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}

	event, err := CreateEventForCurrentState(pod, eventReporter.clusterId)
	if err != nil {
		log.Errorf("Failed to report event because %s", err)
		return
	}

	err = eventReporter.sendEvent(event)

	if err != nil {
		log.Errorf("Failed to report event because %s", err)
		return
	}

	if util.IsReportingPhaseRequired(pod.Status.Phase) {
		err = eventReporter.addAnnotationToMarkStateReported(pod)
		if err != nil {
			log.Errorf("Failed to add stats reported annotation to pod %s because %s", pod.Name, err)
			return
		}
	}
}

func (eventReporter JobEventReporter) sendEvent(event api.Event) error {
	eventMessage, err := api.Wrap(event)
	if err != nil {
		return err
	}

	log.Infof("Reporting event %+v", eventMessage)
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, err = eventReporter.eventClient.Report(ctx, eventMessage)
	return err
}

func (eventReporter JobEventReporter) addAnnotationToMarkStateReported(pod *v1.Pod) error {
	annotations := make(map[string]string)
	annotationName := string(pod.Status.Phase)
	annotations[annotationName] = time.Now().String()

	return eventReporter.clusterContext.AddAnnotation(pod, annotations)
}

func (eventReporter JobEventReporter) ReportMissingJobEvents() {
	allBatchPods, err := eventReporter.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to reconcile missing job events because %s", err)
		return
	}
	podsWithCurrentPhaseNotReported := filterPodsWithCurrentStateNotReported(allBatchPods)

	for _, pod := range podsWithCurrentPhaseNotReported {
		if util.IsReportingPhaseRequired(pod.Status.Phase) {
			eventReporter.ReportCurrentStatus(pod)
		}
	}
}

func filterPodsWithCurrentStateNotReported(pods []*v1.Pod) []*v1.Pod {
	podsWithMissingEvent := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if !HasCurrentStateBeenReported(pod) && HasPodBeenInStateForLongerThanGivenDuration(pod, 30*time.Second) {
			podsWithMissingEvent = append(podsWithMissingEvent, pod)
		}
	}
	return podsWithMissingEvent
}

func HasCurrentStateBeenReported(pod *v1.Pod) bool {
	podPhase := pod.Status.Phase
	_, annotationPresent := pod.Annotations[string(podPhase)]
	return annotationPresent
}

func HasPodBeenInStateForLongerThanGivenDuration(pod *v1.Pod, duration time.Duration) bool {
	deadline := time.Now().Add(-duration)
	lastStatusChange, err := lastStatusChange(pod)

	if err != nil || lastStatusChange.Before(deadline) {
		return true
	}
	return false
}

func lastStatusChange(pod *v1.Pod) (time.Time, error) {
	conditions := pod.Status.Conditions

	if len(conditions) <= 0 {
		return *new(time.Time), errors.New("no state changes found, cannot determine last status change")
	}

	var maxStatusChange time.Time

	for _, condition := range conditions {
		if condition.LastTransitionTime.Time.After(maxStatusChange) {
			maxStatusChange = condition.LastTransitionTime.Time
		}
	}

	return maxStatusChange, nil
}
