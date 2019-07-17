package startup

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/configuration"
	"github.com/G-Research/k8s-batch/internal/executor/reporter"
	"github.com/G-Research/k8s-batch/internal/executor/service"
	"github.com/G-Research/k8s-batch/internal/executor/submitter"
	"github.com/G-Research/k8s-batch/internal/executor/task"
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

	var eventReporter reporter.EventReporter = reporter.New(kubernetesClient, 5*time.Second)

	defer runtime.HandleCrash()
	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)
	podInformer := factory.Core().V1().Pods()
	nodeInformer := factory.Core().V1().Nodes()
	addPodEventHandler(podInformer, eventReporter)
	informerStopper := startInformers(factory)
	defer close(informerStopper)

	jobSubmitter := submitter.JobSubmitter{KubernetesClient: kubernetesClient}

	kubernetesAllocationService := service.KubernetesAllocationService{
		PodLister:    podInformer.Lister(),
		NodeLister:   nodeInformer.Lister(),
		JobSubmitter: jobSubmitter,
	}

	if kubernetesAllocationService.PodLister != nil {

	}

	tasks := make([]*task.ScheduledTask, 0, 5)
	//var utilisationReporterTask task.ScheduledTask = task.ClusterUtilisationReporterTask{
	//	PodLister: podInformer.Lister(),
	//	Interval: config.Task.UtilisationReportingInterval,
	//}
	//tasks = append(tasks, &utilisationReporterTask)

	var forgottenPodReporterTask task.ScheduledTask = task.ForgottenCompletedPodReporterTask{
		PodLister:     podInformer.Lister(),
		EventReporter: eventReporter,
		Interval:      config.Task.ForgottenCompletedPodReportingInterval,
	}
	tasks = append(tasks, &forgottenPodReporterTask)

	var jobLeaseRenewalTask task.ScheduledTask = task.JobLeaseRenewalTask{
		PodLister: podInformer.Lister(),
		Interval:  config.Task.JobLeaseRenewalInterval,
	}
	tasks = append(tasks, &jobLeaseRenewalTask)

	var podDeletionTask task.ScheduledTask = task.PodDeletionTask{
		KubernetesClient: kubernetesClient,
		Interval:         config.Task.PodDeletionInterval,
	}
	tasks = append(tasks, &podDeletionTask)

	//var requestJobsTask  task.ScheduledTask = task.RequestJobsTask{
	//	AllocationService: kubernetesAllocationService,
	//	Interval: config.Task.RequestNewJobsInterval,
	//}
	//tasks = append(tasks, &requestJobsTask)
	taskChannels := scheduleTasks(tasks)
	defer stopTasks(taskChannels)

	for {
		time.Sleep(10 * time.Second)
	}
}

func scheduleTasks(tasks []*task.ScheduledTask) []chan bool {
	taskChannels := make([]chan bool, 0, len(tasks))
	for _, scheduledTask := range tasks {
		stop := make(chan bool)
		t := *scheduledTask

		go func() {
			for {
				select {
				case <-time.After(t.GetInterval()):
				case <-stop:
					return
				}
				t.Execute()
			}
		}()

		taskChannels = append(taskChannels, stop)
	}

	return taskChannels
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
