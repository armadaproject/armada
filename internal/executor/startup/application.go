package startup

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	"github.com/G-Research/k8s-batch/internal/executor/reporter"
	"github.com/G-Research/k8s-batch/internal/executor/service"
	"github.com/G-Research/k8s-batch/internal/executor/submitter"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"os"
	"time"
)

func StartUp(config configuration.Configuration) {
	kubernetesClient, err := CreateKubernetesClientWithDefaultConfig(config.Application.InClusterDeployment)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	podEventReporter := reporter.PodEventReporter{KubernetesClient: kubernetesClient}

	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)
	podWatcher := initializePodWatcher(factory, podEventReporter)

	nodeWatcher := factory.Core().V1().Nodes()
	nodeLister := nodeWatcher.Lister()

	defer runtime.HandleCrash()
	stopper := make(chan struct{})
	defer close(stopper)
	factory.Start(stopper)

	jobSubmitter := submitter.JobSubmitter{KubernetesClient: kubernetesClient}

	kubernetesAllocationService := service.KubernetesAllocationService{
		PodLister:    podWatcher.Lister(),
		NodeLister:   nodeLister,
		JobSubmitter: jobSubmitter,
	}

	for {
		time.Sleep(5 * time.Second)

		kubernetesAllocationService.FillInSpareClusterCapacity()
	}
}

func initializePodWatcher(factory informers.SharedInformerFactory, eventReporter reporter.PodEventReporter) informer.PodInformer {
	podWatcher := factory.Core().V1().Pods()
	podWatcher.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return
			}
			eventReporter.ReportAddEvent(pod)
		},

		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok := oldObj.(*v1.Pod)
			if !ok {
				return
			}
			newPod, ok := newObj.(*v1.Pod)
			if !ok {
				return
			}
			eventReporter.ReportUpdateEvent(oldPod, newPod)
		},
	})
	return podWatcher
}
