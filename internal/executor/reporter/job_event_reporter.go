package reporter

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/tools/cache"

	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	domain2 "github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
)

type EventReporter interface {
	Report(events []EventMessage) error
	QueueEvent(event EventMessage, callback func(error))
}

type queuedEvent struct {
	Event    EventMessage
	Callback func(error)
}

type JobEventReporter struct {
	eventSender      EventSender
	eventBuffer      chan *queuedEvent
	eventQueued      map[string]uint8
	eventQueuedMutex sync.Mutex

	jobRunStateStore *job.JobRunStateStore
	clusterContext   clusterContext.ClusterContext
	clock            clock.Clock
	maxBatchSize     int
}

func NewJobEventReporter(
	clusterContext clusterContext.ClusterContext,
	jobRunState *job.JobRunStateStore,
	eventSender EventSender,
	clock clock.Clock,
	maxBatchSize int,
) (*JobEventReporter, chan bool) {
	stop := make(chan bool)
	reporter := &JobEventReporter{
		eventSender:      eventSender,
		clusterContext:   clusterContext,
		jobRunStateStore: jobRunState,
		eventBuffer:      make(chan *queuedEvent, 1000000),
		eventQueued:      map[string]uint8{},
		eventQueuedMutex: sync.Mutex{},
		clock:            clock,
		maxBatchSize:     maxBatchSize,
	}

	clusterContext.AddPodEventHandler(reporter.podEventHandler())

	go reporter.processEventQueue(stop)

	return reporter, stop
}

func (eventReporter *JobEventReporter) podEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			go eventReporter.reportCurrentStatus(pod)
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
			go eventReporter.reportStatusUpdate(oldPod, newPod)
		},
	}
}

func (eventReporter *JobEventReporter) Report(events []EventMessage) error {
	return eventReporter.eventSender.SendEvents(events)
}

func (eventReporter *JobEventReporter) reportStatusUpdate(old *v1.Pod, new *v1.Pod) {
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
		log.Errorf("Failed to report event: %v", err)
		return
	}

	eventReporter.QueueEvent(EventMessage{Event: event, JobRunId: util.ExtractJobRunId(pod)}, func(err error) {
		if err != nil {
			log.Errorf("Failed to report event: %s", err)
			return
		}

		if util.IsReportingPhaseRequired(pod.Status.Phase) {
			err = eventReporter.addAnnotationToMarkStateReported(pod)
			if err != nil {
				log.Errorf("Failed to add state annotation %s to pod %s: %v", string(pod.Status.Phase), pod.Name, err)
				return
			}
		}
	})

	if pod.Status.Phase == v1.PodRunning && requiresIngressToBeReported(pod) {
		eventReporter.attemptToReportIngressInfoEvent(pod)
	}
}

func (eventReporter *JobEventReporter) QueueEvent(event EventMessage, callback func(error)) {
	eventReporter.eventQueuedMutex.Lock()
	defer eventReporter.eventQueuedMutex.Unlock()
	jobId := event.Event.GetJobId()
	eventReporter.eventQueued[jobId] = eventReporter.eventQueued[jobId] + 1
	eventReporter.eventBuffer <- &queuedEvent{event, callback}
}

func (eventReporter *JobEventReporter) processEventQueue(stop chan bool) {
	ticker := eventReporter.clock.NewTicker(time.Second * 2)
	toSendBuffer := make([]*queuedEvent, 0, eventReporter.maxBatchSize)
	for {
		select {
		case <-stop:
			for i := len(eventReporter.eventBuffer); i > 0; i -= eventReporter.maxBatchSize {
				batch := eventReporter.fillBatch()
				eventReporter.sendBatch(batch)
			}
			return
		case event := <-eventReporter.eventBuffer:
			toSendBuffer = append(toSendBuffer, event)
			if len(toSendBuffer) >= eventReporter.maxBatchSize {
				eventReporter.sendBatch(toSendBuffer)
				toSendBuffer = nil
			}
		case <-ticker.C():
			if len(toSendBuffer) <= 0 {
				break
			}
			eventReporter.sendBatch(toSendBuffer)
			toSendBuffer = nil
		}
	}
}

func (eventReporter *JobEventReporter) fillBatch(batch ...*queuedEvent) []*queuedEvent {
	for len(batch) < eventReporter.maxBatchSize && len(eventReporter.eventBuffer) > 0 {
		batch = append(batch, <-eventReporter.eventBuffer)
	}
	return batch
}

func (eventReporter *JobEventReporter) sendBatch(batch []*queuedEvent) {
	log.Debugf("Sending batch of %d events", len(batch))
	err := eventReporter.eventSender.SendEvents(queuedEventsToEventMessages(batch))
	go func() {
		for _, e := range batch {
			e.Callback(err)
		}
	}()
	eventReporter.eventQueuedMutex.Lock()
	for _, e := range batch {
		id := e.Event.Event.GetJobId()
		count := eventReporter.eventQueued[id]
		if count <= 1 {
			delete(eventReporter.eventQueued, id)
		} else {
			eventReporter.eventQueued[id] = count - 1
		}
	}
	eventReporter.eventQueuedMutex.Unlock()
}

func queuedEventsToEventMessages(queuedEvents []*queuedEvent) []EventMessage {
	result := make([]EventMessage, 0, len(queuedEvents))
	for _, queuedEvent := range queuedEvents {
		result = append(result, queuedEvent.Event)
	}
	return result
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
		log.Errorf("Failed to reconcile missing job events: %v", err)
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
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s: %v", pod.Name, err)
		return
	}
	associatedIngresses, err := eventReporter.clusterContext.GetIngresses(pod)
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

	ingressInfoEvent, err := CreateJobIngressInfoEvent(pod, eventReporter.clusterContext.GetClusterId(), associatedServices, associatedIngresses)
	if err != nil {
		log.Errorf("Failed to report event JobIngressInfoEvent for pod %s: %v", pod.Name, err)
		return
	}
	eventReporter.QueueEvent(EventMessage{Event: ingressInfoEvent, JobRunId: util.ExtractJobRunId(pod)}, func(err error) {
		if err != nil {
			log.Errorf("Failed to report event JobIngressInfoEvent for pod %s: %v", pod.Name, err)
			return
		}

		err = eventReporter.addAnnotationToMarkIngressReported(pod)
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
