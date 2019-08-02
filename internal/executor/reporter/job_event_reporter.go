package reporter

import (
	"context"
	"encoding/json"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	log "github.com/sirupsen/logrus"
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
		return
	}
	eventReporter.report(new)
}

func (eventReporter JobEventReporter) report(pod *v1.Pod) {
	if !util.IsManagedPod(pod) {
		return
	}

	event, err := util.CreateEventMessageForCurrentState(pod)
	if err != nil {
		log.Errorf("Failed to report event because %s", err)
		return
	}

	log.Infof("Reporting event %+v", event)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = eventReporter.EventClient.Report(ctx, event)

	if err != nil {
		log.Errorf("Failed to report event because %s", err)
		return
	}

	err = eventReporter.addAnnotationToMarkStateReported(pod)
	if err != nil {
		log.Errorf("Failed to add stats reported annotation to pod %s because %s", pod.Name, err)
		return
	}
}

func (eventReporter JobEventReporter) addAnnotationToMarkStateReported(pod *v1.Pod) error {
	stateReportedPatch := createPatchToMarkCurrentStateReported(pod)
	return eventReporter.patchPod(pod, stateReportedPatch)
}

func createPatchToMarkCurrentStateReported(pod *v1.Pod) *domain.Patch {
	annotations := make(map[string]string)
	annotationName := string(pod.Status.Phase)

	annotations[annotationName] = time.Now().String()

	patch := domain.Patch{
		MetaData: metav1.ObjectMeta{
			Annotations: annotations,
		},
	}
	return &patch
}

func (eventReporter JobEventReporter) patchPod(pod *v1.Pod, patch *domain.Patch) error {
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = eventReporter.KubernetesClient.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.StrategicMergePatchType, patchBytes)
	if err != nil {
		return err
	}
	return nil
}
