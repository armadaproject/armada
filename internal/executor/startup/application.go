package startup

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	"github.com/G-Research/k8s-batch/internal/executor/reporter"
	"github.com/G-Research/k8s-batch/internal/executor/service"
	"github.com/G-Research/k8s-batch/internal/executor/submitter"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"log"
	"os"
	"time"
)

func StartUp(config configuration.Configuration) {
	kubernetesClient, err := CreateKubernetesClientWithDefaultConfig(config.Application.InClusterDeployment)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	conn, err := grpc.Dial(config.Armada.Url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	queueClient := api.NewAggregatedQueueClient(conn)
	usageClient := api.NewUsageClient(conn)

	var eventReporter reporter.EventReporter = reporter.New(
		kubernetesClient,
		queueClient,
		config.Events.EventReportingInterval,
		config.Events.EventReportingBatchSize,
	)

	defer runtime.HandleCrash()
	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)
	podInformer := factory.Core().V1().Pods()
	nodeLister := factory.Core().V1().Nodes().Lister()
	addPodEventHandler(podInformer, eventReporter)
	informerStopper := startInformers(factory)
	defer close(informerStopper)

	jobSubmitter := submitter.JobSubmitter{KubernetesClient: kubernetesClient}

	jobLeaseService := service.JobLeaseService{
		PodLister:    podInformer.Lister(),
		NodeLister:   nodeLister,
		JobSubmitter: jobSubmitter,
		QueueClient:  queueClient,
		ClusterId:    config.Application.ClusterId,
	}

	clusterUtilisationService := service.ClusterUtilisationService{
		PodLister:   podInformer.Lister(),
		NodeLister:  nodeLister,
		UsageClient: usageClient,
	}

	podCleanupService := service.PodCleanupService{
		PodLister:        podInformer.Lister(),
		EventReporter:    eventReporter,
		KubernetesClient: kubernetesClient,
	}

	tasks := make([]chan bool, 0)
	tasks = append(tasks, scheduleBackgroundTask(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval))
	tasks = append(tasks, scheduleBackgroundTask(jobLeaseService.RequestJobLeasesAndFillSpareClusterCapacity, config.Task.RequestNewJobsInterval))
	tasks = append(tasks, scheduleBackgroundTask(jobLeaseService.RenewJobLeases, config.Task.JobLeaseRenewalInterval))
	tasks = append(tasks, scheduleBackgroundTask(podCleanupService.ReportForgottenCompletedPods, config.Task.ForgottenCompletedPodReportingInterval))
	tasks = append(tasks, scheduleBackgroundTask(podCleanupService.DeletePodsReadyForCleanup, config.Task.PodDeletionInterval))
	defer stopTasks(tasks)

	for {
		time.Sleep(10 * time.Second)
	}
}

func scheduleBackgroundTask(task func(), interval time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			select {
			case <-time.After(interval):
			case <-stop:
				return
			}
			task()
		}
	}()

	return stop
}

func stopTasks(taskChannels []chan bool) {
	for _, channel := range taskChannels {
		channel <- true
	}
}

func addPodEventHandler(podInformer informer.PodInformer, eventReporter reporter.EventReporter) {
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				//TODO Log
				return
			}
			eventReporter.ReportAddEvent(pod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok := oldObj.(*v1.Pod)
			if !ok {
				//TODO Log
				return
			}
			newPod, ok := newObj.(*v1.Pod)
			if !ok {
				//TODO Log
				return
			}
			eventReporter.ReportUpdateEvent(oldPod, newPod)
		},
	})
}

func startInformers(factory informers.SharedInformerFactory) chan struct{} {
	stopper := make(chan struct{})
	factory.Start(stopper)
	return stopper
}
