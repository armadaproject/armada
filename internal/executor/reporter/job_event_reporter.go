package reporter

import (
	"encoding/json"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func New(kubernetesClient kubernetes.Interface, reportingInterval time.Duration, reportingBatchSize int) *JobEventReporter {
	reporter := JobEventReporter{
		kubernetesClient: kubernetesClient,
		podEvents:        make([]*v1.Pod, 0, 100),
	}

	go func() {
		for {
			reporter.processEvents(reportingBatchSize)
			time.Sleep(reportingInterval)
		}
	}()

	return &reporter
}

func (jobEventReporter *JobEventReporter) processEvents(reportingBatchSize int) {
	numberOfEventsLeftToProcess := jobEventReporter.queueLength()

	for numberOfEventsLeftToProcess > 0 {
		currentEventBatchSize := util.Min(reportingBatchSize, numberOfEventsLeftToProcess)

		events := jobEventReporter.peekQueue(currentEventBatchSize)

		//TODO perform API calls here and retrying etc
		for _, pod := range events {
			jobStatus, err := util.ExtractJobStatus(pod)
			if err != nil {
				fmt.Printf("Error reporting job pod for %s because: %s \n", pod.Name, err)
			}

			fmt.Printf("Reporting pod for %s with status %d \n", pod.Name, jobStatus)

			if util.IsInTerminalState(pod) {
				labels := util.DeepCopy(pod.Labels)
				labels[domain.ReadyForCleanup] = strconv.FormatBool(true)

				patch := domain.Patch{
					MetaData: metav1.ObjectMeta{
						Labels: labels,
					},
				}

				patchBytes, err := json.Marshal(patch)
				if err != nil {
					fmt.Printf("Failure marshalling patch for pod %s because: %s \n", pod.Name, err)
				}
				_, err = jobEventReporter.kubernetesClient.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.StrategicMergePatchType, patchBytes)
				if err != nil {
					fmt.Printf("Error updating label for pod for %s because: %s \n", pod.Name, err)
				}
			}
		}

		jobEventReporter.dequeueEvents(currentEventBatchSize)
		numberOfEventsLeftToProcess -= currentEventBatchSize
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
	if !util.IsManagedPod(pod) {
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
