package job

import (
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	clusterContext "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/util"
)

type IngressCleanupService struct {
	clusterContext clusterContext.ClusterContext
}

func RunIngressCleanup(clusterContext clusterContext.ClusterContext) *IngressCleanupService {
	service := &IngressCleanupService{
		clusterContext: clusterContext,
	}

	/*
	 The purpose of this is to remove any associated nodeport services as soon as the pod enters a terminal state
	 There are limited available nodeports in a cluster and we keep pods around after they complete (especially failed pods can live a long time)
	 If we wait for the pod to be cleaned up to delete the nodeport service, we risk exhausting the number of nodeports available
	 - Especially in the case someone submits lots of jobs that require nodeports but fail instantly

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

func (i *IngressCleanupService) removeAnyAssociatedIngress(pod *v1.Pod) {
	log.Infof("Removing any ingresses associated with pod %s (%s)", pod.Name, pod.Namespace)
	service, err := i.clusterContext.GetService(pod.Name, pod.Namespace)
	if err != nil {
		log.Errorf("Failed to get associated ingress for pod %s (%s) because %s", pod.Name, pod.Namespace, err)
		return
	}
	if service == nil {
		return
	}
	err = i.clusterContext.DeleteService(service)
	if err != nil {
		log.Errorf("Failed to remove associated ingress for pod %s (%s) because %s", pod.Namespace, pod.Namespace, err)
		return
	}
}
