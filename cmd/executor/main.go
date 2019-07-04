package main

import (
	"fmt"
	"github.com/oklog/ulid"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"math/rand"
	"strings"
	"time"
)

func main() {
	//config, err := rest.InClusterConfig()

	kubeconfig := "/home/jamesmu/.kube/kind-config-kind"

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)

	//rules := clientcmd.NewDefaultClientConfigLoadingRules()
	//overrides := &clientcmd.ConfigOverrides{}
	//config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()

	if err != nil {
		fmt.Println("Error loading config")
	}

	clientset, err := kubernetes.NewForConfig(config)

	if err != nil {
		fmt.Println("Error loading kubernetes client")
	}

	//tweakOptionsFunc := func(options *metav1.ListOptions) {
	//	options.LabelSelector = "node-role.kubernetes.io/master"
	//}
	//tweakOptions := informers.WithTweakListOptions(tweakOptionsFunc)
	//factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, tweakOptions)

	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0)

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

	result, err := clientset.CoreV1().Pods("default").Create(&pod)

	if err != nil {
		fmt.Printf("Failed creating pod %s\n", pod.ObjectMeta.Name)
	} else {
		fmt.Printf("Created pod %s\n", result.ObjectMeta.Name)
	}


	//createdPod, err := clientset.CoreV1().Pods("default").Create()


	podWatcher := factory.Core().V1().Pods()

	stopper := make(chan struct{})
	defer close(stopper)


	defer runtime.HandleCrash()
	podWatcher.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Println("this is not a pod")
				return
			}
			fmt.Println("Added " + pod.ObjectMeta.Name + " status " + string(pod.Status.Phase))
		},

		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok := oldObj.(*v1.Pod)
			newPod, ok := newObj.(*v1.Pod)
			if !ok {
				log.Println("this is not a pod")
				return
			}
			if oldPod.Status.Phase != newPod.Status.Phase {
				fmt.Println("Updated " + newPod.ObjectMeta.Name  + " to status " + string(newPod.Status.Phase))
				if strings.HasPrefix(pod.Name, "test") && (newPod.Status.Phase == v1.PodSucceeded || newPod.Status.Phase == v1.PodFailed) {
					err := clientset.CoreV1().Pods(newPod.Namespace).Delete(newPod.Name, nil)

					if err != nil {
						log.Println("Failed deleting " + pod.Name)
						return
					}
				}
			} else {
				fmt.Println("Updated " + newPod.ObjectMeta.Name + " with no change to status " + string(newPod.Status.Phase))
			}
		},

		DeleteFunc:func(obj interface{}) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				log.Println("this is not a pod")
				return
			}

			fmt.Println("Deleted " + pod.ObjectMeta.Name)
		},
	})

	startingPosition := podWatcher.Informer().LastSyncResourceVersion()
	fmt.Println("Starting position " + startingPosition)

	podCache:= podWatcher.Lister()

	nodeInformer := factory.Core().V1().Nodes().Lister()
	factory.Start(stopper)

	for {
		time.Sleep(5 * time.Second)
		_, err := nodeInformer.List(labels.Everything())
		if err != nil {
			fmt.Println("Error getting node information")
		}

		allPods, err := podCache.List(labels.Everything())
		if err != nil {
			fmt.Println("Error getting pod information")
		}
		//totalCpu := resource.Quantity{}
		//for _, pod := range allPods {
		//	fmt.Println(pod.Spec.Containers[0].Resources.Requests.Cpu().AsDec())
		//	podCpu := pod.Spec.Containers[0].Resources.Requests.Cpu()
		//	totalCpu.Add(*podCpu)
		//}
		//
		//fmt.Println(totalCpu.AsDec())


		fmt.Printf("Total count of pods is %d\n", len(allPods))
		//for _, node := range nodes {
		//	fmt.Println("Node " + node.Name)
		//	fmt.Printf("Node %d\n", node.Status.Allocatable.Cpu().AsDec())
		//}
	}
}
