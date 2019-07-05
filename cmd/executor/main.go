package main

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/reporter"
	"github.com/G-Research/k8s-batch/internal/service"
	"github.com/G-Research/k8s-batch/internal/startup"
	"github.com/G-Research/k8s-batch/internal/submitter"
	"github.com/oklog/ulid"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	v12 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"math/rand"
	"os"
	"strings"
	"time"
)

func main() {
	kubernetesClient, err := startup.LoadDefaultKubernetesClient()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	//tweakOptionsFunc := func(options *metav1.ListOptions) {
	//	options.LabelSelector = "node-role.startup.io/master"
	//}
	//tweakOptions := informers.WithTweakListOptions(tweakOptionsFunc)
	//factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, tweakOptions)

	podEventReporter := reporter.PodEventReporter{ KubernetesClient: kubernetesClient }

	factory := informers.NewSharedInformerFactoryWithOptions(kubernetesClient, 0)
	podWatcher := initializePodWatcher(factory, podEventReporter)

	nodeWatcher := factory.Core().V1().Nodes()
	nodeLister := nodeWatcher.Lister()

	defer runtime.HandleCrash()
	stopper := make(chan struct{})
	defer close(stopper)
	factory.Start(stopper)

	jobSubmitter := submitter.JobSubmitter{KubernetesClient:kubernetesClient}

	kubernetesAllocationService := service.KubernetesAllocationService{
		PodLister: podWatcher.Lister(),
		NodeLister: nodeLister,
		JobSubmitter: jobSubmitter,
	}

	for {
		time.Sleep(5 * time.Second)

		kubernetesAllocationService.FillInSpareClusterCapacity()
	}
}

func createPod(kubernetesClient kubernetes.Interface) {
	terminationGracePeriod := int64(0)

	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)

	pod := v1.Pod {
		ObjectMeta: metav1.ObjectMeta {
			Name: "test-" + strings.ToLower(ulid.MustNew(ulid.Timestamp(t), entropy).String()),
		},
		Spec: v1.PodSpec {
			TerminationGracePeriodSeconds: &terminationGracePeriod,
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container {
				{
					Name: "sleeper",
					Image: "mcr.microsoft.com/dotnet/core/runtime:2.2",
					ImagePullPolicy: v1.PullIfNotPresent,
					Args: []string {
						"sleep", "20s",
					},
				},
			},
		},
	}

	result, err := kubernetesClient.CoreV1().Pods("default").Create(&pod)

	if err != nil {
		fmt.Printf("Failed creating pod %s\n", pod.ObjectMeta.Name)
	} else {
		fmt.Printf("Created pod %s\n", result.ObjectMeta.Name)
	}
}

func initializePodWatcher(factory informers.SharedInformerFactory, eventReporter reporter.PodEventReporter) v12.PodInformer {
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
