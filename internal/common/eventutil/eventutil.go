package eventutil

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/executor/domain"
	executorutil "github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// UnmarshalEventSequence returns an EventSequence object contained in a byte buffer
// after validating that the resulting EventSequence is valid.
func UnmarshalEventSequence(ctx context.Context, payload []byte) (*armadaevents.EventSequence, error) {
	sequence := &armadaevents.EventSequence{}
	err := proto.Unmarshal(payload, sequence)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.JobSetName == "" {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetName",
			Value:   "",
			Message: "JobSetName not provided",
		}
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.Queue == "" {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   "",
			Message: "Queue name not provided",
		}
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.Groups == nil {
		sequence.Groups = make([]string, 0)
	}

	if sequence.Events == nil {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "Events",
			Value:   nil,
			Message: "no events in sequence",
		}
		err = errors.WithStack(err)
		return nil, err
	}
	return sequence, nil
}

// ApiJobsFromLogSubmitJobs converts a slice of log jobs to API jobs.
func ApiJobsFromLogSubmitJobs(userId string, groups []string, queueName string, jobSetName string, time time.Time, es []*armadaevents.SubmitJob) ([]*api.Job, error) {
	jobs := make([]*api.Job, len(es), len(es))
	for i, e := range es {
		job, err := ApiJobFromLogSubmitJob(userId, groups, queueName, jobSetName, time, e)
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}
	return jobs, nil
}

// ApiJobFromLogSubmitJob converts a SubmitJob log message into an api.Job struct, which is used by Armada internally.
func ApiJobFromLogSubmitJob(ownerId string, groups []string, queueName string, jobSetName string, time time.Time, e *armadaevents.SubmitJob) (*api.Job, error) {

	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	// We only support PodSpecs as main object.
	mainObject, ok := e.MainObject.Object.(*armadaevents.KubernetesMainObject_PodSpec)
	if !ok {
		return nil, errors.Errorf("expected *PodSpecWithAvoidList, but got %v", e.MainObject.Object)
	}
	podSpec := mainObject.PodSpec.PodSpec

	// The job submit message contains a bag of additional k8s objects to create as part of the job.
	// Currently, these must be of type pod spec, service spec, or ingress spec. These are stored in a single slice.
	// Here, we separate them by type for inclusion in the API job.
	k8sServices := make([]*v1.Service, 0)
	k8sIngresses := make([]*networking.Ingress, 0)
	k8sPodSpecs := make([]*v1.PodSpec, 0)
	k8sPodSpecs = append(k8sPodSpecs, podSpec)
	for _, object := range e.Objects {
		k8sObjectMeta := *K8sObjectMetaFromLogObjectMeta(object.GetObjectMeta())
		switch o := object.Object.(type) {
		case *armadaevents.KubernetesObject_Service:
			k8sServices = append(k8sServices, &v1.Service{
				ObjectMeta: k8sObjectMeta,
				Spec:       *o.Service,
			})
		case *armadaevents.KubernetesObject_Ingress:
			k8sIngresses = append(k8sIngresses, &networking.Ingress{
				ObjectMeta: k8sObjectMeta,
				Spec:       *o.Ingress,
			})
		case *armadaevents.KubernetesObject_PodSpec:
			k8sPodSpecs = append(k8sPodSpecs, o.PodSpec.PodSpec)
		default:
			return nil, &armadaerrors.ErrInvalidArgument{
				Name:    "Objects",
				Value:   o,
				Message: "unsupported k8s object",
			}
		}
	}

	// If there's exactly one podSpec, put it in the PodSpec field, otherwise put all of them in the PodSpecs field.
	// Because API jobs must specify either PodSpec or PodSpecs, this ensures that the job resulting from the conversion
	// API job -> log job -> API job is equal to the original job.
	podSpec = nil
	var podSpecs []*v1.PodSpec
	if len(k8sPodSpecs) == 1 {
		podSpec = k8sPodSpecs[0]
	} else {
		podSpecs = k8sPodSpecs
	}

	return &api.Job{
		Id:       jobId,
		ClientId: e.DeduplicationId,
		Queue:    queueName,
		JobSetId: jobSetName,

		Namespace:   e.ObjectMeta.Namespace,
		Labels:      e.ObjectMeta.Labels,
		Annotations: e.ObjectMeta.Annotations,

		K8SIngress: k8sIngresses,
		K8SService: k8sServices,

		Priority: float64(e.Priority),

		PodSpec:                  podSpec,
		PodSpecs:                 podSpecs,
		Created:                  time,
		Owner:                    ownerId,
		QueueOwnershipUserGroups: groups,
	}, nil
}

// LogSubmitJobFromApiJob converts an API job to a log job.
// Note that PopulateK8sServicesIngresses must be called first if job.Services and job.Ingress
// is to be included in the resulting log job, since the log job can only include k8s objects
// (i.e., not the API-specific job.Services or job.Ingress).
func LogSubmitJobFromApiJob(job *api.Job) (*armadaevents.SubmitJob, error) {
	if job.PodSpec != nil && len(job.PodSpecs) != 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpecs",
			Value:   job.PodSpecs,
			Message: "Both PodSpec and PodSpecs are set",
		})
	}

	jobId, err := armadaevents.ProtoUuidFromUlidString(job.GetId())
	if err != nil {
		return nil, err
	}

	priority, err := LogSubmitPriorityFromApiPriority(job.GetPriority())
	if err != nil {
		return nil, err
	}

	mainObject, objects, err := LogSubmitObjectsFromApiJob(job)
	if err != nil {
		return nil, err
	}

	return &armadaevents.SubmitJob{
		JobId:           jobId,
		DeduplicationId: job.GetClientId(),
		Priority:        priority,
		ObjectMeta: &armadaevents.ObjectMeta{
			ExecutorId:  "", // Not set by the job
			Namespace:   job.GetNamespace(),
			Annotations: job.GetAnnotations(),
			Labels:      job.GetLabels(),
		},
		MainObject: mainObject,
		Objects:    objects,
	}, nil
}

// LogSubmitObjectsFromApiJob extracts all objects from an API job for inclusion in a log job.
//
// To extract services and ingresses, PopulateK8sServicesIngresses must be called on the job first
// to convert API-specific job objects to proper K8s objects.
func LogSubmitObjectsFromApiJob(job *api.Job) (*armadaevents.KubernetesMainObject, []*armadaevents.KubernetesObject, error) {

	// Objects part of the job in addition to the main object.
	objects := make([]*armadaevents.KubernetesObject, 0, len(job.Services)+len(job.Ingress)+len(job.PodSpecs))

	// Each job has a main object associated with it, which determines when the job exits.
	// If provided, use job.PodSpec as the main object. Otherwise, try to use job.PodSpecs[0].
	mainPodSpec := job.PodSpec
	additionalPodSpecs := job.PodSpecs
	if additionalPodSpecs == nil {
		additionalPodSpecs = make([]*v1.PodSpec, 0)
	}
	if mainPodSpec == nil && len(additionalPodSpecs) > 0 {
		mainPodSpec = additionalPodSpecs[0]
		additionalPodSpecs = additionalPodSpecs[1:]
	}

	// Job must contain at least one podspec.
	if mainPodSpec == nil {
		err := errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpec",
			Value:   nil,
			Message: "job doesn't contain any podspecs",
		})
		return nil, nil, err
	}

	mainObject := &armadaevents.KubernetesMainObject{
		Object: &armadaevents.KubernetesMainObject_PodSpec{
			PodSpec: &armadaevents.PodSpecWithAvoidList{
				PodSpec: mainPodSpec,
			},
		},
	}

	// Collect all additional objects.
	for _, podSpec := range additionalPodSpecs {
		objects = append(objects, &armadaevents.KubernetesObject{
			Object: &armadaevents.KubernetesObject_PodSpec{
				PodSpec: &armadaevents.PodSpecWithAvoidList{
					PodSpec: podSpec,
				},
			},
		})
	}
	for _, service := range job.K8SService {
		objects = append(objects, &armadaevents.KubernetesObject{
			ObjectMeta: LogObjectMetaFromK8sObjectMeta(&service.ObjectMeta),
			Object: &armadaevents.KubernetesObject_Service{
				Service: &service.Spec,
			},
		})
	}
	for _, ingress := range job.K8SIngress {
		objects = append(objects, &armadaevents.KubernetesObject{
			ObjectMeta: LogObjectMetaFromK8sObjectMeta(&ingress.ObjectMeta),
			Object: &armadaevents.KubernetesObject_Ingress{
				Ingress: &ingress.Spec,
			},
		})
	}

	return mainObject, objects, nil
}

// PopulateK8sServicesIngresses converts the API-specific service and ingress object into K8s objects
// and stores those in the job object.
func PopulateK8sServicesIngresses(job *api.Job, ingressConfig *configuration.IngressConfiguration) error {
	services, ingresses, err := K8sServicesIngressesFromApiJob(job, ingressConfig)
	if err != nil {
		return err
	}
	job.K8SService = services
	job.K8SIngress = ingresses
	return nil
}

// K8sServicesIngressesFromApiJob converts job.Services and job.Ingress to k8s services and ingresses.
func K8sServicesIngressesFromApiJob(job *api.Job, ingressConfig *configuration.IngressConfiguration) ([]*v1.Service, []*networking.Ingress, error) {

	// GenerateIngresses (below) looks into the pod to set names for the services/ingresses.
	// Hence, we use the same code as is later used by the executor to create the pod to be submitted.
	// Note that we only create the pod here to pass it to GenerateIngresses.
	// TODO: This only works for a single pod; I think we should create services/ingresses for each pod in the request (Albin).
	pod := executorutil.CreatePod(job, &configuration.PodDefaults{}, 0)
	pod.Annotations = util.MergeMaps(pod.Annotations, map[string]string{
		domain.HasIngress:               "true",
		domain.AssociatedServicesCount:  fmt.Sprintf("%d", len(job.Services)),
		domain.AssociatedIngressesCount: fmt.Sprintf("%d", len(job.Ingress)),
	})

	// Create k8s objects from the data embedded in the request.
	// GenerateIngresses expects a job object and a pod because it looks into those for optimisations.
	// For example, it deletes services/ingresses for which there are no corresponding ports exposed in the PodSpec.
	// Note that the user may submit several pods, but we only pass in one of them as a separate argument.
	// I think this may result in Armada deleting services/ingresses needed for pods other than the first one
	// - Albin
	services, ingresses := executorutil.GenerateIngresses(job, pod, ingressConfig)

	return services, ingresses, nil
}

// LogSubmitPriorityFromApiPriority returns the uint32 representation of the priority included with a submitted job,
// or an error if the conversion fails.
func LogSubmitPriorityFromApiPriority(priority float64) (uint32, error) {
	if priority < 1 {
		return 0, &armadaerrors.ErrInvalidArgument{
			Name:    "priority",
			Value:   priority,
			Message: "priority must be larger than or equal to 1",
		}
	}
	if priority > math.MaxUint32 {
		priority = math.MaxUint32
	}
	priority = math.Round(priority)
	return uint32(priority), nil
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

func K8sObjectMetaFromLogObjectMeta(meta *armadaevents.ObjectMeta) *metav1.ObjectMeta {
	return &metav1.ObjectMeta{
		Namespace:   meta.GetNamespace(),
		Name:        meta.GetName(),
		UID:         types.UID(meta.GetKubernetesId()),
		Annotations: meta.GetAnnotations(),
		Labels:      meta.GetLabels(),
	}
}
