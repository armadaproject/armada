package reporter

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"time"
)

type EventReporter interface {
	ReportEvent(pod *v1.Pod)
	ReportUpdateEvent(old *v1.Pod, new *v1.Pod)
}

type JobEventReporter struct {
	KubernetesClient kubernetes.Interface
	EventClient      api.EventClient
}

func (eventReporter JobEventReporter) ReportEvent(pod *v1.Pod) {
	eventReporter.report(pod)
}

func (eventReporter JobEventReporter) ReportUpdateEvent(old *v1.Pod, new *v1.Pod) {
	if old.Status.Phase == new.Status.Phase {
		fmt.Printf("Skipping update on pod %s, as update didn't change the pods current phase \n", new.Name)
		return
	}
	eventReporter.report(new)
}

func (eventReporter JobEventReporter) report(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}

	event, err := createJobEventMessage(pod)
	if err != nil {
		fmt.Printf("Failed to report event because %s \n", err)
	}

	//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	//defer cancel()
	//_, err = eventReporter.EventClient.Report(ctx, event)
	//
	//if err != nil {
	//	fmt.Printf("Failed to report event because %s \n", err)
	//}
	fmt.Printf("Reporting event %s \n", event.String())

	eventReporter.addStateChangeAnnotation(pod)
}

func createJobEventMessage(pod *v1.Pod) (*api.EventMessage, error) {
	phase := pod.Status.Phase

	switch phase {
	case v1.PodPending:
		pending := api.JobPendingEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		pendingMessage := api.EventMessage_Pending{
			Pending: &pending,
		}

		message := api.EventMessage{
			Events: &pendingMessage,
		}

		return &message, nil

	case v1.PodRunning:
		running := api.JobRunningEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		runningMessage := api.EventMessage_Running{
			Running: &running,
		}

		message := api.EventMessage{
			Events: &runningMessage,
		}

		return &message, nil

	case v1.PodFailed:
		failed := api.JobFailedEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		failedMessage := api.EventMessage_Failed{
			Failed: &failed,
		}

		message := api.EventMessage{
			Events: &failedMessage,
		}

		return &message, nil

	case v1.PodSucceeded:
		succeeded := api.JobSucceededEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		succeededMessage := api.EventMessage_Succeeded{
			Succeeded: &succeeded,
		}

		message := api.EventMessage{
			Events: &succeededMessage,
		}

		return &message, nil

	default:
		return new(api.EventMessage), errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
	}
}

func (eventReporter JobEventReporter) addStateChangeAnnotation(pod *v1.Pod) {
	annotations := make(map[string]string)
	annotationName := string(pod.Status.Phase)

	annotations[annotationName] = time.Now().String()

	patch := domain.Patch{
		MetaData: metav1.ObjectMeta{
			Annotations: annotations,
		},
	}

	eventReporter.patchPod(pod, &patch)
}

func (eventReporter JobEventReporter) patchPod(pod *v1.Pod, patch *domain.Patch) {
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		fmt.Printf("Failure marshalling patch for pod %s because: %s \n", pod.Name, err)
	}
	_, err = eventReporter.KubernetesClient.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		fmt.Printf("Error updating pod with %s for %s because: %s \n", patchBytes, pod.Name, err)
	}
}
