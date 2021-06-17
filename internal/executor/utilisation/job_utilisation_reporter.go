package utilisation

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/armada/internal/common"
	clusterContext "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
)

type UtilisationEventReporter struct {
	clusterContext clusterContext.ClusterContext
	podUtilisation PodUtilisationService
	eventReporter  reporter.EventReporter

	podInfo           map[string]*podUtilisationInfo
	dataAccessMutex   sync.Mutex
	reportingInterval time.Duration
}

type podUtilisationInfo struct {
	lastReported   time.Time
	pod            *v1.Pod
	utilisationMax common.ComputeResources
}

func NewUtilisationEventReporter(
	clusterContext clusterContext.ClusterContext,
	podUtilisation PodUtilisationService,
	eventReporter reporter.EventReporter,
	reportingPeriod time.Duration,
) *UtilisationEventReporter {

	r := &UtilisationEventReporter{
		clusterContext:    clusterContext,
		podUtilisation:    podUtilisation,
		eventReporter:     eventReporter,
		reportingInterval: reportingPeriod,
		podInfo:           map[string]*podUtilisationInfo{},
	}

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			go r.updatePod(pod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPod, ok := newObj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", newObj)
				return
			}
			go r.updatePod(newPod)
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			go r.deletePod(pod)
		},
	})
	return r
}

func (r *UtilisationEventReporter) ReportUtilisationEvents() {
	r.dataAccessMutex.Lock()
	defer r.dataAccessMutex.Unlock()

	now := time.Now()
	reportingTime := now.Add(-r.reportingInterval)
	for _, info := range r.podInfo {
		currentUtilisation := r.podUtilisation.GetPodUtilisation(info.pod)
		info.utilisationMax.Max(currentUtilisation)
		if info.lastReported.Before(reportingTime) {
			r.reportUsage(info)
			info.lastReported = now
			info.utilisationMax = common.ComputeResources{}
		}
	}
}

func (r *UtilisationEventReporter) updatePod(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}

	r.dataAccessMutex.Lock()
	defer r.dataAccessMutex.Unlock()

	if pod.Status.Phase == v1.PodRunning {
		_, exists := r.podInfo[pod.Name]
		if !exists {
			r.podInfo[pod.Name] = &podUtilisationInfo{
				lastReported:   time.Now(),
				pod:            pod,
				utilisationMax: r.podUtilisation.GetPodUtilisation(pod),
			}
		}
	}
	if util.IsInTerminalState(pod) {
		podInfo, exists := r.podInfo[pod.Name]
		if exists {
			r.reportUsage(podInfo)
			delete(r.podInfo, pod.Name)
		}
	}
}

func (r *UtilisationEventReporter) deletePod(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}
	r.dataAccessMutex.Lock()
	defer r.dataAccessMutex.Unlock()
	delete(r.podInfo, pod.Name)
}

func (r *UtilisationEventReporter) reportUsage(info *podUtilisationInfo) {
	event := reporter.CreateJobUtilisationEvent(info.pod, info.utilisationMax, r.clusterContext.GetClusterId())
	r.queueEventWithRetry(event, 3)
}

func (r *UtilisationEventReporter) queueEventWithRetry(event api.Event, retry int) {
	var callback func(e error)
	callback = func(e error) {
		if e != nil {
			log.Errorf("failed to report utilisation: %v", e)
			retry--
			if retry > 0 {
				r.eventReporter.QueueEvent(event, callback)
			}
		}
	}
	r.eventReporter.QueueEvent(event, callback)
}
