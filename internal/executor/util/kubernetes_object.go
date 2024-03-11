package util

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
)

func CreatePodFromExecutorApiJob(job *executorapi.JobRunLease, defaults *configuration.PodDefaults) (*v1.Pod, error) {
	podSpec, err := getPodSpec(job)
	if err != nil {
		return nil, err
	}
	jobId, err := armadaevents.UlidStringFromProtoUuid(job.Job.JobId)
	if err != nil {
		return nil, err
	}
	runId, err := armadaevents.UuidStringFromProtoUuid(job.JobRunId)
	if err != nil {
		return nil, err
	}

	labels := util.MergeMaps(job.Job.ObjectMeta.Labels, map[string]string{
		domain.JobId:     jobId,
		domain.JobRunId:  runId,
		domain.Queue:     job.Queue,
		domain.PodNumber: strconv.Itoa(0),
		domain.PodCount:  strconv.Itoa(1),
	})
	annotation := util.MergeMaps(job.Job.ObjectMeta.Annotations, map[string]string{
		domain.JobSetId: job.Jobset,
		domain.Owner:    job.User,
	})

	applyDefaults(podSpec, defaults)
	setRestartPolicyNever(podSpec)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        common.PodNamePrefix + jobId + "-" + strconv.Itoa(0),
			Labels:      labels,
			Annotations: annotation,
			Namespace:   job.Job.ObjectMeta.Namespace,
		},
		Spec: *podSpec,
	}

	return pod, nil
}

func CreateOwnerReference(pod *v1.Pod) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       pod.Name,
		UID:        pod.UID,
	}
}

func ExtractIngresses(job *executorapi.JobRunLease, pod *v1.Pod, executorIngressConfig *configuration.IngressConfiguration) []*networking.Ingress {
	result := make([]*networking.Ingress, 0, 10)

	for _, additionalObject := range job.Job.Objects {
		switch typed := additionalObject.Object.(type) {
		case *armadaevents.KubernetesObject_Ingress:
			labels := util.MergeMaps(additionalObject.ObjectMeta.Labels, map[string]string{
				domain.JobId:     pod.Labels[domain.JobId],
				domain.JobRunId:  pod.Labels[domain.JobRunId],
				domain.Queue:     pod.Labels[domain.Queue],
				domain.PodNumber: pod.Labels[domain.PodNumber],
			})
			annotations := executorIngressConfig.Annotations
			annotations = util.MergeMaps(annotations, additionalObject.ObjectMeta.Annotations)
			annotations = util.MergeMaps(annotations, map[string]string{
				domain.JobSetId: job.Jobset,
				domain.Owner:    job.User,
			})
			result = append(result, &networking.Ingress{
				ObjectMeta: metav1.ObjectMeta{
					Name:        additionalObject.ObjectMeta.Name,
					Labels:      labels,
					Annotations: annotations,
					Namespace:   additionalObject.ObjectMeta.Namespace,
				},
				Spec: *typed.Ingress,
			})
		}
	}

	return result
}

func ExtractServices(job *executorapi.JobRunLease, pod *v1.Pod) []*v1.Service {
	result := make([]*v1.Service, 0, 10)

	for _, additionalObject := range job.Job.Objects {
		switch typed := additionalObject.Object.(type) {
		case *armadaevents.KubernetesObject_Service:
			labels := util.MergeMaps(additionalObject.ObjectMeta.Labels, map[string]string{
				domain.JobId:     pod.Labels[domain.JobId],
				domain.JobRunId:  pod.Labels[domain.JobRunId],
				domain.Queue:     pod.Labels[domain.Queue],
				domain.PodNumber: pod.Labels[domain.PodNumber],
			})
			annotations := additionalObject.ObjectMeta.Annotations
			annotations = util.MergeMaps(annotations, map[string]string{
				domain.JobSetId: job.Jobset,
				domain.Owner:    job.User,
			})

			service := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:        additionalObject.ObjectMeta.Name,
					Labels:      labels,
					Annotations: annotations,
					Namespace:   additionalObject.ObjectMeta.Namespace,
				},
				Spec: *typed.Service,
			}

			// TODO Once migrated  fully executor api - consider adding jobRunId here
			service.Spec.Selector = map[string]string{
				domain.JobId:     pod.Labels[domain.JobId],
				domain.Queue:     pod.Labels[domain.Queue],
				domain.PodNumber: pod.Labels[domain.PodNumber],
			}
			result = append(result, service)
		}
	}

	return result
}

func getPodSpec(job *executorapi.JobRunLease) (*v1.PodSpec, error) {
	if job == nil || job.Job == nil || job.Job.MainObject == nil {
		return nil, fmt.Errorf("no podspec found in the main object - jobs must specify a podspec")
	}
	switch typed := job.Job.MainObject.Object.(type) {
	case *armadaevents.KubernetesMainObject_PodSpec:
		return typed.PodSpec.PodSpec, nil
	}
	return nil, fmt.Errorf("no podspec found in the main object - jobs must specify a podspec")
}

func applyDefaults(spec *v1.PodSpec, defaults *configuration.PodDefaults) {
	if defaults == nil {
		return
	}
	if defaults.SchedulerName != "" && spec.SchedulerName == "" {
		spec.SchedulerName = defaults.SchedulerName
	}
}

func setRestartPolicyNever(podSpec *v1.PodSpec) {
	podSpec.RestartPolicy = v1.RestartPolicyNever
}
