package reporter

import (
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
	clusterContext "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/util"
)

const batchSize = 200

type EventReporter interface {
	Report(event api.Event) error
	ReportCurrentStatus(pod *v1.Pod)
	ReportStatusUpdate(old *v1.Pod, new *v1.Pod)
}

type queuedEvent struct {
	Event    api.Event
	Callback func(error)
}

type JobEventReporter struct {
	eventClient      api.EventClient
	eventBuffer      chan *queuedEvent
	eventQueued      map[string]bool
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
		eventQueued:      map[string]bool{},
		eventQueuedMutex: sync.Mutex{}}

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

	go reporter.processEventQueue(stop)

	return reporter, stop
}

func (eventReporter *JobEventReporter) Report(event api.Event) error {
	return eventReporter.sendEvent(event)
}

func (eventReporter *JobEventReporter) ReportStatusUpdate(old *v1.Pod, new *v1.Pod) {
	if old.Status.Phase == new.Status.Phase {
		return
	}
	eventReporter.ReportCurrentStatus(new)
}

func (eventReporter *JobEventReporter) ReportCurrentStatus(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}

	event, err := CreateEventForCurrentState(pod, eventReporter.clusterContext.GetClusterId())
	if err != nil {
		log.Errorf("Failed to report event because %s", err)
		return
	}

	eventReporter.queueEvent(event, func(err error) {
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
}

func (eventReporter *JobEventReporter) queueEvent(event api.Event, callback func(error)) {
	eventReporter.eventQueuedMutex.Lock()
	defer eventReporter.eventQueuedMutex.Unlock()
	eventReporter.eventQueued[event.GetJobId()] = true
	eventReporter.eventBuffer <- &queuedEvent{event, callback}
}

func (eventReporter *JobEventReporter) processEventQueue(stop chan bool) {
	for {
		batch := []*queuedEvent{}
		select {
		case <-stop:
			return
		case event := <-eventReporter.eventBuffer:
			batch = append(batch, event)
			for len(batch) < batchSize && len(eventReporter.eventBuffer) > 0 {
				batch = append(batch, <-eventReporter.eventBuffer)
			}
		}
		err := eventReporter.sendEvents(batch)
		go func() {
			for _, e := range batch {
				e.Callback(err)
			}
		}()

		eventReporter.eventQueuedMutex.Lock()
		for _, e := range batch {
			delete(eventReporter.eventQueued, e.Event.GetJobId())
		}
		eventReporter.eventQueuedMutex.Unlock()
	}
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

func (eventReporter *JobEventReporter) ReportMissingJobEvents() {
	allBatchPods, err := eventReporter.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.Errorf("Failed to reconcile missing job events because %s", err)
		return
	}
	podsWithCurrentPhaseNotReported := filterPodsWithCurrentStateNotReported(allBatchPods)

	for _, pod := range podsWithCurrentPhaseNotReported {
		if util.IsReportingPhaseRequired(pod.Status.Phase) && !eventReporter.hasPendingEvents(pod) {
			eventReporter.ReportCurrentStatus(pod)
		}
	}
}

func (eventReporter *JobEventReporter) hasPendingEvents(pod *v1.Pod) bool {
	eventReporter.eventQueuedMutex.Lock()
	defer eventReporter.eventQueuedMutex.Unlock()
	id := util.ExtractJobId(pod)
	return eventReporter.eventQueued[id]
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

	if err != nil {
		log.Errorf("Problem with pod %v: %v", pod.Name, err)
		return false
	}
	return lastStatusChange.Before(deadline)
}

func lastStatusChange(pod *v1.Pod) (time.Time, error) {
	maxStatusChange := pod.CreationTimestamp.Time
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if s := containerStatus.State.Running; s != nil {
			maxStatusChange = maxTime(maxStatusChange, s.StartedAt.Time)
		}
		if s := containerStatus.State.Terminated; s != nil {
			maxStatusChange = maxTime(maxStatusChange, s.FinishedAt.Time)
		}
	}

	for _, condition := range pod.Status.Conditions {
		maxStatusChange = maxTime(maxStatusChange, condition.LastTransitionTime.Time)
	}

	if maxStatusChange.IsZero() {
		return maxStatusChange, errors.New("cannot determine last status change")
	}
	return maxStatusChange, nil
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
