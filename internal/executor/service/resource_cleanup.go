package service

import (
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/armadaproject/armada/internal/executor/configuration"
	clusterContext "github.com/armadaproject/armada/internal/executor/context"
	"github.com/armadaproject/armada/internal/executor/util"
)

type ResourceCleanupService struct {
	clusterContext          clusterContext.ClusterContext
	kubernetesConfiguration configuration.KubernetesConfiguration
}

func NewResourceCleanupService(
	clusterContext clusterContext.ClusterContext,
	kubernetesConfiguration configuration.KubernetesConfiguration,
) *ResourceCleanupService {
	service := &ResourceCleanupService{
		clusterContext:          clusterContext,
		kubernetesConfiguration: kubernetesConfiguration,
	}

	/*
	 The purpose of this is to remove any associated ingress and services resources as soon as the pod enters a terminal state
	 There is a limited number of nodeports + clusterIPs in a cluster and we keep pods around after they complete (especially failed pods can live a long time)
	 If we wait for the pod to be cleaned up to delete the associated ingresses + services, we risk exhausting the number of nodeports/clusterIPs available
	 - Especially in the case someone submits lots of jobs that require these resources but fail instantly

	 We do set ownerreference on the services to point to the pod.
	 So in the case the cleanup below fails, the ownerreference will ensure it is cleaned up when the pod is
	*/

	clusterContext.AddPodEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod, ok := newObj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", newObj)
				return
			}
			if util.IsManagedPod(pod) && util.IsInTerminalState(pod) && util.HasIngress(pod) {
				go service.removeAnyAssociatedIngress(pod)
			}
		},
	})

	return service
}

func (i *ResourceCleanupService) removeAnyAssociatedIngress(pod *v1.Pod) {
	log.Infof("Removing any ingresses associated with pod %s (%s)", pod.Name, pod.Namespace)
	services, err := i.clusterContext.GetServices(pod)
	if err != nil {
		log.Errorf("Failed to get associated services for pod %s (%s) because %s", pod.Name, pod.Namespace, err)
	} else {
		for _, service := range services {
			err = i.clusterContext.DeleteService(service)
			if err != nil {
				log.Errorf("Failed to remove associated ingress for pod %s (%s) because %s", pod.Namespace, pod.Namespace, err)
				continue
			}
		}
	}

	ingresses, err := i.clusterContext.GetIngresses(pod)
	if err != nil {
		log.Errorf("Failed to get associated ingresses for pod %s (%s) because %s", pod.Name, pod.Namespace, err)
	} else {
		for _, ingress := range ingresses {
			err = i.clusterContext.DeleteIngress(ingress)
			if err != nil {
				log.Errorf("Failed to remove associated ingress for pod %s (%s) because %s", pod.Namespace, pod.Namespace, err)
				continue
			}
		}
	}
}

// CleanupResources
/*
 * This function finds and delete old resources. It does this in two ways:
 *  - By deleting all expired terminated pods
 *  - Deleting non-expired terminated pods when then MaxTerminatedPods limit is exceeded
 */
func (r *ResourceCleanupService) CleanupResources() {
	pods, err := r.clusterContext.GetActiveBatchPods()
	if err != nil {
		log.WithError(err).Errorf("Skipping pod cleanup due to an error while loading active batch pods")
		return
	}

	allTerminatedPods := util.FilterPods(pods, util.IsPodFinishedAndReported)
	// Expired pods are ones who are older than configured retention period
	expiredTerminatedPods := util.FilterPods(allTerminatedPods, r.canPodBeRemoved)
	nonExpiredTerminatedPods := util.RemovePodsFromList(allTerminatedPods, expiredTerminatedPods)

	r.clusterContext.DeletePods(expiredTerminatedPods)

	if len(nonExpiredTerminatedPods) > r.kubernetesConfiguration.MaxTerminatedPods {
		numberOfPodsToDelete := len(nonExpiredTerminatedPods) - r.kubernetesConfiguration.MaxTerminatedPods
		// We get the oldest pods from queues that have the terminated pods
		// This means each queue has a "share" of terminated pods
		// so one bad queue doesn't cause everyone to lose their terminated pod logs early
		podsToDelete := getOldestPodsWithQueueFairShare(nonExpiredTerminatedPods, numberOfPodsToDelete)
		r.clusterContext.DeletePods(podsToDelete)
	}
}

func getOldestPodsWithQueueFairShare(pods []*v1.Pod, numberOfPodsLimit int) []*v1.Pod {
	if len(pods) <= numberOfPodsLimit {
		return pods
	}

	podsByQueue := groupPodsByQueueAndSortByPodAge(pods)
	podsToReturn := make([]*v1.Pod, 0, numberOfPodsLimit)
	for i := 0; i < numberOfPodsLimit; i++ {
		queueWithMostPods := getQueueWithTheMostPods(podsByQueue)
		if queueWithMostPods == "" {
			log.Warn("Getting oldest pods for queues ended early due to lack of pods")
			break
		}

		lastElementIndex := len(podsByQueue[queueWithMostPods]) - 1
		oldestPod := podsByQueue[queueWithMostPods][lastElementIndex]
		podsByQueue[queueWithMostPods] = podsByQueue[queueWithMostPods][:lastElementIndex]
		podsToReturn = append(podsToReturn, oldestPod)

	}
	return podsToReturn
}

func getQueueWithTheMostPods(podsByQueue map[string][]*v1.Pod) string {
	queueWithMostPods := ""
	for queue := range podsByQueue {
		if len(podsByQueue[queue]) > len(podsByQueue[queueWithMostPods]) {
			queueWithMostPods = queue
		}
	}
	return queueWithMostPods
}

func groupPodsByQueueAndSortByPodAge(pods []*v1.Pod) map[string][]*v1.Pod {
	podsByQueue := util.GroupByQueue(pods)
	for queue := range podsByQueue {
		sort.Slice(podsByQueue[queue], func(i, j int) bool {
			pod1 := podsByQueue[queue][i]
			pod2 := podsByQueue[queue][j]
			pod1LastStateChange, errPod1 := util.LastStatusChange(pod1)
			pod2LastStateChange, errPod2 := util.LastStatusChange(pod2)
			if errPod1 != nil {
				return false
			}
			if errPod2 != nil {
				return true
			}
			// Sort most recent time to oldest
			return pod1LastStateChange.After(pod2LastStateChange)
		})
	}

	return podsByQueue
}

func (r *ResourceCleanupService) canPodBeRemoved(pod *v1.Pod) bool {
	if !util.IsPodFinishedAndReported(pod) {
		return false
	}

	lastContainerStart := util.FindLastContainerStartTime(pod)
	if lastContainerStart.Add(r.kubernetesConfiguration.MinimumPodAge).After(time.Now()) {
		return false
	}

	if pod.Status.Phase == v1.PodFailed {
		lastChange, err := util.LastStatusChange(pod)
		if err == nil && lastChange.Add(r.kubernetesConfiguration.FailedPodExpiry).After(time.Now()) {
			return false
		}
	}
	return true
}
