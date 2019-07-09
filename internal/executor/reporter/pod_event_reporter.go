package reporter

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"log"
	"strings"
)

type PodEventReporter struct {
	KubernetesClient kubernetes.Interface
}

func (eventReporter PodEventReporter) ReportAddEvent(pod *v1.Pod) {
	fmt.Println("Added " + pod.ObjectMeta.Name + " status " + string(pod.Status.Phase))
}

func (eventReporter PodEventReporter) ReportUpdateEvent(oldPod *v1.Pod, newPod *v1.Pod) {
	kubernetesClient := eventReporter.KubernetesClient

	if oldPod.Status.Phase != newPod.Status.Phase {
		fmt.Println("Updated " + newPod.ObjectMeta.Name  + " to status " + string(newPod.Status.Phase))
		if strings.HasPrefix(newPod.Name, "test") && IsInTerminalState(newPod) {
			err := kubernetesClient.CoreV1().Pods(newPod.Namespace).Delete(newPod.Name, nil)
			if err != nil {
				log.Println("Failed deleting " + newPod.Name)
				return
			}
		}
	} else {
		fmt.Println("Updated " + newPod.ObjectMeta.Name + " with no change to status " + string(newPod.Status.Phase))
	}
}

func IsInTerminalState(pod *v1.Pod) bool {
	podPhase := pod.Status.Phase
	if podPhase == v1.PodSucceeded || podPhase == v1.PodFailed {
		return true
	}
	return false
}
