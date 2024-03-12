package conversion

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func SubmitJobFromApiRequest(req *api.JobSubmitRequest, jobReq *api.JobSubmitRequestItem, owner string) *armadaevents.SubmitJob {
	jobId := util.NewULID()
	priority := eventutil.LogSubmitPriorityFromApiPriority(jobReq.GetPriority())
	mainObject, objects := submitObjectsFromApiJobReq(req, jobReq, jobId, owner)
	return &armadaevents.SubmitJob{
		JobId:           armadaevents.MustProtoUuidFromUlidString(jobId),
		DeduplicationId: jobReq.GetClientId(),
		Priority:        priority,
		ObjectMeta: &armadaevents.ObjectMeta{
			Namespace:   jobReq.GetNamespace(),
			Annotations: jobReq.GetAnnotations(),
			Labels:      jobReq.GetLabels(),
		},
		MainObject:      mainObject,
		Objects:         objects,
		Scheduler:       jobReq.Scheduler,
		QueueTtlSeconds: jobReq.QueueTtlSeconds,
	}
}

// submitObjectsFromApiJob extracts all objects from an API job request for inclusion in a log job.
func submitObjectsFromApiJobReq(req *api.JobSubmitRequest, jobReq *api.JobSubmitRequestItem, jobId string, owner string) (*armadaevents.KubernetesMainObject, []*armadaevents.KubernetesObject) {
	// Objects part of the job in addition to the main object.
	objects := make([]*armadaevents.KubernetesObject, 0, len(jobReq.Services)+len(jobReq.Ingress)+1)

	// Each job has a main object associated with it, which determines when the job exits.
	mainPodSpec := jobReq.GetMainPodSpec()

	mainObject := &armadaevents.KubernetesMainObject{
		Object: &armadaevents.KubernetesMainObject_PodSpec{
			PodSpec: &armadaevents.PodSpecWithAvoidList{
				PodSpec: mainPodSpec,
			},
		},
	}

	services, ingresses := GenerateIngresses(req, jobReq, jobId, owner)

	for _, service := range services {
		objects = append(objects, &armadaevents.KubernetesObject{
			ObjectMeta: LogObjectMetaFromK8sObjectMeta(&service.ObjectMeta),
			Object: &armadaevents.KubernetesObject_Service{
				Service: &service.Spec,
			},
		})
	}
	for _, ingress := range ingresses {
		objects = append(objects, &armadaevents.KubernetesObject{
			ObjectMeta: LogObjectMetaFromK8sObjectMeta(&ingress.ObjectMeta),
			Object: &armadaevents.KubernetesObject_Ingress{
				Ingress: &ingress.Spec,
			},
		})
	}

	return mainObject, objects
}

func LogObjectMetaFromK8sObjectMeta(meta *metav1.ObjectMeta) *armadaevents.ObjectMeta {
	return &armadaevents.ObjectMeta{
		ExecutorId:   "", // Not part of the k8s ObjectMeta.
		Namespace:    meta.GetNamespace(),
		Name:         meta.GetName(),
		KubernetesId: string(meta.GetUID()), // The type returned by GetUID is an alias of string.
		Annotations:  meta.GetAnnotations(),
		Labels:       meta.GetLabels(),
	}
}
