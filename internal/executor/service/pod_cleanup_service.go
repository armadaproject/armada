package service

import (
	"sync"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v12 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/G-Research/k8s-batch/internal/executor/util"
)

type PodCleanupService interface {
	DeletePods(pods []*v1.Pod)
	ProcessPodsToDelete()
}

type podCleanupService struct {
	kubernetesClient kubernetes.Interface
	mutex            sync.RWMutex
	podsCache        map[string]*v1.Pod
}

func NewPodCleanupService(kubernetesClient kubernetes.Interface, podInformer v12.PodInformer) PodCleanupService {
	service := &podCleanupService{
		kubernetesClient: kubernetesClient,
		mutex:            sync.RWMutex{},
		podsCache:        map[string]*v1.Pod{},
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			service.RemoveFromCache(pod)
		},
	})

	return service
}

func (cleanupService *podCleanupService) DeletePods(pods []*v1.Pod) {
	for _, podToDelete := range pods {
		cleanupService.mutex.Lock()
		jobId := util.ExtractJobId(podToDelete)
		_, ok := cleanupService.podsCache[jobId]
		if !ok {
			cleanupService.podsCache[jobId] = podToDelete
		}
		cleanupService.mutex.Unlock()
	}
}

func (cleanupService *podCleanupService) ProcessPodsToDelete() {

	deleteOptions := createPodDeletionDeleteOptions()
	for jobId, podToDelete := range cleanupService.podsCache {
		err := cleanupService.kubernetesClient.CoreV1().Pods(podToDelete.Namespace).Delete(podToDelete.Name, &deleteOptions)
		if err != nil {
			log.Errorf("Failed to delete pod %s/%s because %s", podToDelete.Namespace, podToDelete.Name, err)
		}
		cleanupService.mutex.Lock()
		if err != nil {
			delete(cleanupService.podsCache, jobId)
		} else {
			cleanupService.podsCache[jobId] = nil
		}
		cleanupService.mutex.Unlock()
	}
}

func (cleanupService *podCleanupService) RemoveFromCache(pod *v1.Pod) {
	jobId := util.ExtractJobId(pod)
	cleanupService.mutex.Lock()
	defer cleanupService.mutex.Unlock()
	delete(cleanupService.podsCache, jobId)
}

func IsPodReadyForCleanup(pod *v1.Pod) bool {
	if util.IsInTerminalState(pod) && hasCurrentStateBeenReported(pod) {
		return true
	}
	return false
}

func createPodDeletionDeleteOptions() metav1.DeleteOptions {
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}
	return deleteOptions
}
