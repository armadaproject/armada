package reporter

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/armada/internal/common"
	clusterContext "github.com/G-Research/armada/internal/executor/context"
	domain2 "github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
)

const batchSize = 200

type EventReporter interface {
	Report(event api.Event) error
	QueueEvent(event api.Event, callback func(error))
}

type queuedEvent struct {
	Event    api.Event
	Callback func(error)
}

type JobEventReporter struct {
	eventClient      api.EventClient
	eventBuffer      chan *queuedEvent
	eventQueued      map[string]uint8
	eventQueuedMutex sync.Mutex

	clusterContext clusterContext.ClusterContext
	stop           chan bool
}

func NewJobEventReporter(clusterContext clusterContext.ClusterContext, eventClient api.EventClient) (*JobEventReporter, chan bool) {
	stop := make(chan bool)
	reporter := &JobEventReporter{
		eventClient:      eventClient,
		clusterContext:   clusterContext,
		eventBuffer:      make(chan *queuedEvent, 1000000),
		eventQueued:      map[string]uint8{},
		eventQueuedMutex: sync.Mutex{},
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			go reporter.reportCurrentStatus(pod)
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
			go reporter.reportStatusUpdate(oldPod, newPod)
		},
	})

	go reporter.processEventQueue(stop)

	return reporter, stop
}

func (eventReporter *JobEventReporter) Report(event api.Event) error {
	return eventReporter.sendEvent(event)
}

func (eventReporter *JobEventReporter) reportStatusUpdate(old *v1.Pod, new *v1.Pod) {
	if old.Status.Phase == new.Status.Phase {
		return
	}
	eventReporter.reportCurrentStatus(new)
}

func (eventReporter *JobEventReporter) reportCurrentStatus(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}
	if util.HasCurrentStateBeenReported(pod) {
		return
	}

	event, err := CreateEventForCurrentState(pod, eventReporter.clusterContext.GetClusterId())
	if err != nil {
		log.Errorf("Failed to report event because %s", err)
		return
	}

	eventReporter.QueueEvent(event, func(err error) {
		if err != nil {
			log.Errorf("Failed to report event because %s", err)
			return
		}

		if util.IsReportingPhaseRequired(pod.Status.Phase) {
			err = eventReporter.addAnnotationToMarkStateReported(pod)
			if err != nil {
				log.Errorf("Failed to add state annotation %s to pod %s because %s", string(pod.Status.Phase), pod.Name, err)
				return
			}
		}
	})

	if pod.Status.Phase == v1.PodRunning && requiresIngressToBeReported(pod) {
		eventReporter.attemptToReportIngressInfoEvent(pod)
	}
}

func (eventReporter *JobEventReporter) QueueEvent(event api.Event, callback func(error)) {
	eventReporter.eventQueuedMutex.Lock()
	defer eventReporter.eventQueuedMutex.Unlock()
	jobId := event.GetJobId()
	eventReporter.eventQueued[jobId] = eventReporter.eventQueued[jobId] + 1
	eventReporter.eventBuffer <- &queuedEvent{event, callback}
}

func (eventReporter *JobEventReporter) processEventQueue(stop chan bool) {
	for {
		select {
		case <-stop:
			for i := len(eventReporter.eventBuffer); i > 0; i -= batchSize {
				batch := eventReporter.fillBatch()
				eventReporter.sendBatch(batch)
			}
			return
		case event := <-eventReporter.eventBuffer:
			batch := eventReporter.fillBatch(event)
			eventReporter.sendBatch(batch)
		}
	}
}

func (eventReporter *JobEventReporter) fillBatch(batch ...*queuedEvent) []*queuedEvent {
	for len(batch) < batchSize && len(eventReporter.eventBuffer) > 0 {
		batch = append(batch, <-eventReporter.eventBuffer)
	}
	return batch
}

func (eventReporter *JobEventReporter) sendBatch(batch []*queuedEvent) {
	err := eventReporter.sendEvents(batch)
	go func() {
		for _, e := range batch {
			e.Callback(err)
		}
	}()
	eventReporter.eventQueuedMutex.Lock()
	for _, e := range batch {
		id := e.Event.GetJobId()
		count := eventReporter.eventQueued[id]
		if count <= 1 {
			delete(eventReporter.eventQueued, id)
		} else {
			eventReporter.eventQueued[id] = count - 1
		}
	}
	eventReporter.eventQueuedMutex.Unlock()
}

func (eventReporter *JobEventReporter) sendEvents(events []*queuedEvent) error {
	eventMessages := []*api.EventMessage{}
	for _, e := range events {
		m, err := api.Wrap(e.Event)
		eventMessages = append(eventMessages, m)
		if err != nil {
			return err
		}
		log.Infof("Reporting event %+v", m)
	}
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, err := eventReporter.eventClient.ReportMultiple(ctx, &api.EventList{eventMessages})
	return err
}

func (eventReporter *JobEventReporter) sendEvent(event api.Event) error {
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

func (eventReporter *JobEventReporter) addAnnotationToMarkStateReported(pod *v1.Pod) error {
	annotations := make(map[string]string)
	annotationName := string(pod.Status.Phase)
	annotations[annotationName] = time.Now().String()

	return eventReporter.clusterContext.AddAnnotation(pod, annotations)
}

func (eventReporter *JobEventReporter) addAnnotationToMarkIngressReported(pod *v1.Pod) error {
	annotations := make(map[string]string)
	annotationName := domain2.IngressReported
	annotations[annotationName] = time.Now().String()

	return eventReporter.clusterContext.AddAnnotation(pod, annotations)
}

func (eventReporter *JobEventReporter) ReportMissingJobEvents() {
	allBatchPods, err := eventReporter.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to reconcile missing job events because %s", err)
		return
	}
	podsWithCurrentPhaseNotReported := filterPodsWithCurrentStateNotReported(allBatchPods)

	for _, pod := range podsWithCurrentPhaseNotReported {
		if util.IsReportingPhaseRequired(pod.Status.Phase) && !eventReporter.hasPendingEvents(pod) {
			eventReporter.reportCurrentStatus(pod)
		}
	}

	podWithIngressNotReported := util.FilterPods(allBatchPods, func(pod *v1.Pod) bool {
		return pod.Status.Phase == v1.PodRunning &&
			requiresIngressToBeReported(pod) &&
			util.HasPodBeenInStateForLongerThanGivenDuration(pod, 15*time.Second)
	})

	for _, pod := range podWithIngressNotReported {
		if !eventReporter.hasPendingEvents(pod) {
			eventReporter.attemptToReportIngressInfoEvent(pod)
		}
	}
}

func (eventReporter *JobEventReporter) attemptToReportIngressInfoEvent(pod *v1.Pod) {
	expectedNumberOfServices := util.GetExpectedNumberOfAssociatedServices(pod)
	expectedNumberOfIngresses := util.GetExpectedNumberOfAssociatedIngresses(pod)
	associatedServices, err := eventReporter.clusterContext.GetServices(pod)
	if err != nil {
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s because %s", pod.Name, err)
		return
	}
	associatedIngresses, err := eventReporter.clusterContext.GetIngresses(pod)
	if err != nil {
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s because %s", pod.Name, err)
		return
	}
	if len(associatedServices) != expectedNumberOfServices || len(associatedIngresses) != expectedNumberOfIngresses {
		log.Warnf("Not reporting JobIngressInfoEvent for pod %s because not all expected associated services "+
			"(current %d, expected %d) or ingresses (current %d, expected %d) exist yet",
			pod.Name, len(associatedServices), expectedNumberOfServices, len(associatedIngresses), expectedNumberOfIngresses)
		// Don't report ingress info until all expected ingresses exist
		return
	}

	ingressInfoEvent, err := CreateJobIngressInfoEvent(pod, eventReporter.clusterContext.GetClusterId(), associatedServices, associatedIngresses)
	if err != nil {
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s because %s", pod.Name, err)
		return
	}
	eventReporter.QueueEvent(ingressInfoEvent, func(err error) {
		if err != nil {
			log.Errorf("Failed to report event JobIngressInfoEvent for pod %s because %s", pod.Name, err)
			return
		}

		err = eventReporter.addAnnotationToMarkIngressReported(pod)
		if err != nil {
			log.Errorf("Failed to add ingress reported annotation %s to pod %s because %s", string(pod.Status.Phase), pod.Name, err)
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

func (eventReporter *JobEventReporter) hasPendingEvents(pod *v1.Pod) bool {
	eventReporter.eventQueuedMutex.Lock()
	defer eventReporter.eventQueuedMutex.Unlock()
	id := util.ExtractJobId(pod)
	return eventReporter.eventQueued[id] > 0
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
