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
	"k8s.io/client-go/informers"
	informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"log"
	"os"
	"sync"
	"time"
)

func StartUp(config configuration.Configuration) (*sync.WaitGroup, chan os.Signal) {
	kubernetesClient, err := CreateKubernetesClientWithDefaultConfig(config.Application.InClusterDeployment)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	conn, err := grpc.Dial(config.Armada.Url, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	queueClient := api.NewAggregatedQueueClient(conn)
	usageClient := api.NewUsageClient(conn)

	//TODO Decide how to gracefully stop event reporter
	var eventReporter reporter.EventReporter = reporter.New(
		kubernetesClient,
		queueClient,
		config.Events.EventReportingInterval,
		config.Events.EventReportingBatchSize,
	)

	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)
	podInformer := factory.Core().V1().Pods()
	nodeLister := factory.Core().V1().Nodes().Lister()
	addPodEventHandler(podInformer, eventReporter)
	informerStopper := startInformers(factory)

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

	wg := &sync.WaitGroup{}
	wg.Add(1)

	tasks := make([]chan bool, 0)
	tasks = append(tasks, scheduleBackgroundTask(clusterUtilisationService.ReportClusterUtilisation, config.Task.UtilisationReportingInterval, wg))
	tasks = append(tasks, scheduleBackgroundTask(jobLeaseService.RequestJobLeasesAndFillSpareClusterCapacity, config.Task.RequestNewJobsInterval, wg))
	tasks = append(tasks, scheduleBackgroundTask(jobLeaseService.RenewJobLeases, config.Task.JobLeaseRenewalInterval, wg))
	tasks = append(tasks, scheduleBackgroundTask(podCleanupService.ReportForgottenCompletedPods, config.Task.ForgottenCompletedPodReportingInterval, wg))
	tasks = append(tasks, scheduleBackgroundTask(podCleanupService.DeletePodsReadyForCleanup, config.Task.PodDeletionInterval, wg))
	defer stopTasks(tasks)

	shutdown := make(chan os.Signal, 1)

	go func() {
		shutdownSignal := <-shutdown
		fmt.Printf("Caught shutdown signal: %+v \n", shutdownSignal)

		close(informerStopper)
		conn.Close()

		fmt.Println("Shutdown complete")
		wg.Done()
		if waitForShutdownCompletion(wg, 2*time.Second) {
			fmt.Println("Graceful shutdown timed out")
		}
	}()

	return wg, shutdown
}

func waitForShutdownCompletion(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func scheduleBackgroundTask(task func(), interval time.Duration, wg *sync.WaitGroup) chan bool {
	stop := make(chan bool)

	wg.Add(1)
	go func() {
		for {
			select {
			case <-time.After(interval):
			case <-stop:
				wg.Done()
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
