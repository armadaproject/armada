package reporter

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/model"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"strconv"
	"sync"
	"time"
)

type EventReporter interface {
	ReportAddEvent(pod *v1.Pod)
	ReportUpdateEvent(old *v1.Pod, new *v1.Pod)
	ReportCompletedEvent(pod *v1.Pod)
}

type JobEventReporter struct {
	kubernetesClient kubernetes.Interface
	podEvents        []*v1.Pod
	mux              sync.Mutex
	//TODO API CLIENT
}

func New(kubernetesClient kubernetes.Interface, reportingInterval time.Duration) *JobEventReporter {
	reporter := JobEventReporter{
		kubernetesClient: kubernetesClient,
		podEvents:        make([]*v1.Pod, 0, 100),
	}

	go func() {
		for {
			reporter.processEvents()
			time.Sleep(reportingInterval)
		}
	}()

	return &reporter
}

func (jobEventReporter *JobEventReporter) processEvents() {
	chunkSize := 100
	numberOfEvents := jobEventReporter.queueLength()

	for numberOfEvents > 0 {
		//TODO write min function
		numberOfEventsToReport := chunkSize
		if numberOfEventsToReport > numberOfEvents {
			numberOfEventsToReport = numberOfEvents
		}

		events := jobEventReporter.peekQueue(numberOfEventsToReport)

		//TODO perform API calls here and retrying etc
		for _, pod := range events {
			jobStatus, err := getJobStatus(pod)
			if err != nil {
				fmt.Printf("Error reporting job pod for %s because: %s \n", pod.Name, err)
			}

			fmt.Printf("Reporting pod for %s with status %d \n", pod.Name, jobStatus)

			//TODO find a more efficient way to update labels (you should be able to partially patch rather than send the whole pod)
			if IsInTerminalState(pod) {
				pod.ObjectMeta.Labels[domain.ReadyForCleanup] = strconv.FormatBool(true)

				patchBytes, err := json.Marshal(pod)
				if err != nil {
					fmt.Printf("Failure marshalling patch for pod %s because: %s \n", pod.Name, err)
				}
				_, err = jobEventReporter.kubernetesClient.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.MergePatchType, patchBytes)
				if err != nil {
					fmt.Printf("Error updating label for pod for %s because: %s \n", pod.Name, err)
				}
			}
		}

		jobEventReporter.dequeueEvents(numberOfEventsToReport)
		numberOfEvents -= numberOfEventsToReport
	}
}

func (jobEventReporter *JobEventReporter) ReportAddEvent(pod *v1.Pod) {
	jobEventReporter.queueEvent(pod)
}

func (jobEventReporter *JobEventReporter) ReportUpdateEvent(old *v1.Pod, new *v1.Pod) {
	if old.Status.Phase == new.Status.Phase {
		fmt.Printf("Skipping update on pod %s, as update didn't change the pods current phase \n", new.Name)
		return
	}
	jobEventReporter.queueEvent(new)
}

func (jobEventReporter *JobEventReporter) ReportCompletedEvent(pod *v1.Pod) {
	jobEventReporter.queueEvent(pod)
}

func (jobEventReporter *JobEventReporter) queueEvent(pod *v1.Pod) {
	if !IsManagedPod(pod) {
		return
	}

	jobEventReporter.mux.Lock()

	jobEventReporter.podEvents = append(jobEventReporter.podEvents, pod.DeepCopy())

	jobEventReporter.mux.Unlock()

	fmt.Printf("Queueing event for pod %s \n", pod.Name)
}

func (jobEventReporter *JobEventReporter) dequeueEvents(numberOfEvents int) {
	jobEventReporter.mux.Lock()

	jobEventReporter.podEvents = jobEventReporter.podEvents[numberOfEvents:]

	jobEventReporter.mux.Unlock()
}

func (jobEventReporter *JobEventReporter) queueLength() int {
	jobEventReporter.mux.Lock()
	defer jobEventReporter.mux.Unlock()

	return len(jobEventReporter.podEvents)
}

//This can only be accessed by the internal go routine, so therefore doesn't need locking
func (jobEventReporter *JobEventReporter) peekQueue(numberOfEvents int) []*v1.Pod {
	return jobEventReporter.podEvents[:numberOfEvents]
}

func IsManagedPod(pod *v1.Pod) bool {
	if _, ok := pod.Labels[domain.JobId]; !ok {
		return false
	}

	return true
}

func getJobStatus(pod *v1.Pod) (model.JobStatus, error) {
	phase := pod.Status.Phase

	switch phase {
	case v1.PodPending:
		return model.Pending, nil
	case v1.PodRunning:
		return model.Running, nil
	case v1.PodSucceeded:
		return model.Succeeded, nil
	case v1.PodFailed:
		return model.Failed, nil
	default:
		return *new(model.JobStatus), errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
	}
}
