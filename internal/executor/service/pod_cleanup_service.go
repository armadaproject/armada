package service

import (
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
	cache            util.PodCache
}

func NewPodCleanupService(kubernetesClient kubernetes.Interface, podInformer v12.PodInformer, deletedPodCache util.PodCache) PodCleanupService {
	service := &podCleanupService{
		kubernetesClient: kubernetesClient,
		cache:            deletedPodCache,
	}

	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Errorf("Failed to process pod event due to it being an unexpected type. Failed to process %+v", obj)
				return
			}
			jobId := util.ExtractJobId(pod)
			service.cache.Delete(jobId)
		},
	})

	return service
}

func (cleanupService *podCleanupService) DeletePods(pods []*v1.Pod) {
	for _, podToDelete := range pods {
		cleanupService.cache.AddIfNotExists(podToDelete)
	}
}

func (cleanupService *podCleanupService) ProcessPodsToDelete() {

	pods := cleanupService.cache.GetAll()

	deleteOptions := createPodDeletionDeleteOptions()
	for _, podToDelete := range pods {
		if podToDelete == nil {
			continue
		}
		err := cleanupService.kubernetesClient.CoreV1().Pods(podToDelete.Namespace).Delete(podToDelete.Name, &deleteOptions)
		jobId := util.ExtractJobId(podToDelete)
		if err != nil {
			log.Errorf("Failed to delete pod %s/%s because %s", podToDelete.Namespace, podToDelete.Name, err)
			cleanupService.cache.Delete(jobId)
		} else {
			cleanupService.cache.Update(jobId, nil)
		}
	}
}

func (cleanupService *podCleanupService) RemoveFromCache(pod *v1.Pod) {
	jobId := util.ExtractJobId(pod)
	cleanupService.cache.Delete(jobId)
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
