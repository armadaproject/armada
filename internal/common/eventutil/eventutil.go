package eventutil

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/domain"
	executorutil "github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
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
			Message: fmt.Sprintf("JobSetName not provided for sequence %s", ShortSequenceString(sequence)),
		}
		err = errors.WithStack(err)
		return nil, err
	}

	if sequence.Queue == "" {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   "",
			Message: fmt.Sprintf("Queue name not provided for sequence %s", ShortSequenceString(sequence)),
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

// ShortSequenceString returns a short string representation of an events sequence.
// To be used for logging, for example.
func ShortSequenceString(sequence *armadaevents.EventSequence) string {
	s := ""
	for _, event := range sequence.Events {
		jobId, _ := armadaevents.JobIdFromEvent(event)
		jobIdString, err := armadaevents.UlidStringFromProtoUuid(jobId)
		if err != nil {
			jobIdString = ""
		}
		s += fmt.Sprintf("[%T (job %s)] ", event.Event, jobIdString)
	}
	return s
}

// ApiJobsFromLogSubmitJobs converts a slice of log jobs to API jobs.
func ApiJobsFromLogSubmitJobs(
	userId string,
	groups []string,
	queueName string,
	jobSetName string,
	time time.Time,
	es []*armadaevents.SubmitJob,
) ([]*api.Job, error) {
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

	if e == nil || e.MainObject == nil || e.MainObject.Object == nil {
		return nil, errors.Errorf("SubmitJob or one of its member pointers is nil")
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
	priority := LogSubmitPriorityFromApiPriority(job.GetPriority())
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
		Scheduler:  job.Scheduler,
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
func LogSubmitPriorityFromApiPriority(priority float64) uint32 {
	if priority < 0 {
		priority = 0
	}
	if priority > math.MaxUint32 {
		priority = math.MaxUint32
	}
	priority = math.Round(priority)
	return uint32(priority)
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

func EventSequencesFromApiEvents(msgs []*api.EventMessage) ([]*armadaevents.EventSequence, error) {
	// Each sequence may only contain events for a specific combination of (queue, jobSet, userId).
	// Because each API event may contain different (queue, jobSet, userId), we map each event to separate sequences.
	sequences := make([]*armadaevents.EventSequence, 0, len(msgs))
	for _, msg := range msgs {
		sequence, err := EventSequenceFromApiEvent(msg)
		if err != nil {
			return nil, err
		}
		sequences = append(sequences, sequence)
	}

	// Reduce the sequences to the smallest number possible.
	return CompactEventSequences(sequences), nil
}

// CompactEventSequences converts a []*armadaevents.EventSequence into a []*armadaevents.EventSequence of minimal length.
// In particular, it moves events with equal (queue, jobSetName, userId, groups) into a single sequence
// when doing so is possible without changing the order of events within job sets.
//
// For example, three sequences [A, B, C], [D, E], [F, G]
// could result in the following two sequences [A, B, C, F, G], [D, E],
// if sequence 1 and 3 share the same (queue, jobSetName, userId, groups)
// and if the sequence [D, E] is for a different job set.
func CompactEventSequences(sequences []*armadaevents.EventSequence) []*armadaevents.EventSequence {
	// We may change ordering between job sets but not within job sets.
	// To ensure the order of the resulting compacted sequences is deterministic,
	// store a slice of all unique jobSetNames in the order they occur in sequences.
	numSequences := 0
	jobSetNames := make([]string, 0)
	sequencesFromJobSetName := make(map[string][]*armadaevents.EventSequence)
	for _, sequence := range sequences {
		if sequence == nil || len(sequence.Events) == 0 {
			continue // Skip empty sequences.
		}
		// Consider sequences within the same jobSet for compaction.
		if jobSetSequences, ok := sequencesFromJobSetName[sequence.JobSetName]; ok {
			// This first if clause should never trigger.
			if len(jobSetSequences) == 0 {
				numSequences++
				sequencesFromJobSetName[sequence.JobSetName] = append(jobSetSequences, sequence)
			} else {
				// Merge events in sequence into the last sequence for this jobSet if (queue, jobSetName, userId, groups) are equal.
				lastSequence := jobSetSequences[len(jobSetSequences)-1]
				if lastSequence != nil &&
					sequence.Queue == lastSequence.Queue &&
					sequence.UserId == lastSequence.UserId &&
					groupsEqual(sequence.Groups, lastSequence.Groups) {

					lastSequence.Events = append(lastSequence.Events, sequence.Events...)
				} else {
					numSequences++
					sequencesFromJobSetName[sequence.JobSetName] = append(jobSetSequences, sequence)
				}
			}
		} else { // Create a new slice the first time we see a jobSet.
			numSequences++
			jobSetNames = append(jobSetNames, sequence.JobSetName)
			sequencesFromJobSetName[sequence.JobSetName] = []*armadaevents.EventSequence{sequence}
		}
	}

	// Flatten the map to return a slice of sequences.
	sequences = make([]*armadaevents.EventSequence, 0, numSequences)
	for _, jobSetName := range jobSetNames {
		if jobSetSequences, ok := sequencesFromJobSetName[jobSetName]; ok {
			sequences = append(sequences, jobSetSequences...)
		} else {
			log.Errorf("no sequence found for jobSetName %s; this should never happen", jobSetName)
		}
	}

	return sequences
}

func groupsEqual(g1, g2 []string) bool {
	if len(g1) == 0 && len(g2) == 0 {
		// []string{} and nil are considered equal.
		return true
	}
	if len(g1) != len(g2) {
		return false
	}
	for i := range g1 {
		if g1[i] != g2[i] {
			return false
		}
	}
	return true
}

// LimitSequencesByteSize calls LimitSequenceByteSize for each of the provided sequences
// and returns all resulting sequences.
func LimitSequencesByteSize(sequences []*armadaevents.EventSequence, sizeInBytes uint, strict bool) ([]*armadaevents.EventSequence, error) {
	rv := make([]*armadaevents.EventSequence, 0, len(sequences))
	for _, sequence := range sequences {
		limitedSequences, err := LimitSequenceByteSize(sequence, sizeInBytes, strict)
		if err != nil {
			return nil, err
		}
		rv = append(rv, limitedSequences...)
	}
	return rv, nil
}

// LimitSequenceByteSize returns a slice of sequences produced by breaking up sequence.Events
// into separate sequences, each of which is at most MAX_SEQUENCE_SIZE_IN_BYTES bytes in size.
func LimitSequenceByteSize(sequence *armadaevents.EventSequence, sizeInBytes uint, strict bool) ([]*armadaevents.EventSequence, error) {
	// Compute the size of the sequence without events.
	events := sequence.Events
	sequence.Events = make([]*armadaevents.EventSequence_Event, 0)
	headerSize := uint(proto.Size(sequence))
	sequence.Events = events

	// var currentSequence *armadaevents.EventSequence
	sequences := make([]*armadaevents.EventSequence, 0, 1)
	lastSequenceEventSize := uint(0)
	for _, event := range sequence.Events {
		eventSize := uint(proto.Size(event))
		if eventSize+headerSize > sizeInBytes && strict {
			return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:  "sequence",
				Value: sequence,
				Message: fmt.Sprintf(
					"sequence header is of size %d and sequence contains an event of size %d bytes, but the sequence size limit is %d",
					headerSize,
					eventSize,
					sizeInBytes,
				),
			})
		}
		if len(sequences) == 0 || lastSequenceEventSize+eventSize+headerSize > sizeInBytes {
			sequences = append(sequences, &armadaevents.EventSequence{
				Queue:      sequence.Queue,
				JobSetName: sequence.JobSetName,
				UserId:     sequence.UserId,
				Groups:     sequence.Groups,
				Events:     nil,
			})
			lastSequenceEventSize = 0
		}
		lastSequence := sequences[len(sequences)-1]
		lastSequence.Events = append(lastSequence.Events, event)
		lastSequenceEventSize += eventSize
	}
	return sequences, nil
}

// EventSequenceFromApiEvent converts an api.EventMessage into the corresponding Pulsar event
// and returns an EventSequence containing this single event.
// We map API events to sequences one-to-one because each API event may contain different (queue, jobSet, userId),
// which must be common to all events in a sequence.
func EventSequenceFromApiEvent(msg *api.EventMessage) (sequence *armadaevents.EventSequence, err error) {
	sequence = &armadaevents.EventSequence{}

	switch m := msg.Events.(type) {
	case *api.EventMessage_Submitted:
		// Do nothing; the Pulsar submitted message is generated by the Pulsar API endpoint.
	case *api.EventMessage_Queued:
		// Do nothing; there's no corresponding Pulsar message.
	case *api.EventMessage_DuplicateFound:
		sequence.Queue = m.DuplicateFound.Queue
		sequence.JobSetName = m.DuplicateFound.JobSetId

		newJobId, err := armadaevents.ProtoUuidFromUlidString(m.DuplicateFound.JobId)
		if err != nil {
			return nil, err
		}
		oldJobId, err := armadaevents.ProtoUuidFromUlidString(m.DuplicateFound.OriginalJobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.DuplicateFound.Created,
			Event: &armadaevents.EventSequence_Event_JobDuplicateDetected{
				JobDuplicateDetected: &armadaevents.JobDuplicateDetected{
					NewJobId: newJobId,
					OldJobId: oldJobId,
				},
			},
		})
	case *api.EventMessage_Leased:
		sequence.Queue = m.Leased.Queue
		sequence.JobSetName = m.Leased.JobSetId

		// Message has no KubernetesId; use the all-zeros id.
		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Leased.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Leased.Created,
			Event: &armadaevents.EventSequence_Event_JobRunLeased{
				JobRunLeased: &armadaevents.JobRunLeased{
					RunId:      LegacyJobRunId(),
					JobId:      jobId,
					ExecutorId: m.Leased.ClusterId,
				},
			},
		})
	case *api.EventMessage_LeaseReturned:
		sequence.Queue = m.LeaseReturned.Queue
		sequence.JobSetName = m.LeaseReturned.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.LeaseReturned.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.LeaseReturned.KubernetesId)
		if err != nil {
			// Because LeaseReturned may be generated before the job is running, the KubernetesId may be missing.
			// In this scenario, we make up an empty id.
			runId = LegacyJobRunId()
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.LeaseReturned.Created,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true, // EventMessage_LeaseReturned indicates a pod could not be scheduled.
							Reason: &armadaevents.Error_PodLeaseReturned{
								PodLeaseReturned: &armadaevents.PodLeaseReturned{
									ObjectMeta: &armadaevents.ObjectMeta{
										ExecutorId:   m.LeaseReturned.ClusterId,
										KubernetesId: m.LeaseReturned.KubernetesId,
									},
									PodNumber:    m.LeaseReturned.PodNumber,
									Message:      m.LeaseReturned.Reason,
									RunAttempted: m.LeaseReturned.RunAttempted,
								},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_LeaseExpired:
		sequence.Queue = m.LeaseExpired.Queue
		sequence.JobSetName = m.LeaseExpired.JobSetId

		// Message has no KubernetesId; use the all-zeros id.
		jobId, err := armadaevents.ProtoUuidFromUlidString(m.LeaseExpired.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.LeaseExpired.Created,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: LegacyJobRunId(),
					JobId: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true, // EventMessage_LeaseExpired indicates a failed job run.
							Reason: &armadaevents.Error_LeaseExpired{
								LeaseExpired: &armadaevents.LeaseExpired{},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_Pending:
		sequence.Queue = m.Pending.Queue
		sequence.JobSetName = m.Pending.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Pending.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.Pending.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Pending.Created,
			Event: &armadaevents.EventSequence_Event_JobRunAssigned{
				JobRunAssigned: &armadaevents.JobRunAssigned{
					RunId: runId,
					JobId: jobId,
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								KubernetesId: m.Pending.KubernetesId,
								Name:         m.Pending.PodName,
								Namespace:    m.Pending.PodNamespace,
								ExecutorId:   m.Pending.ClusterId,
							},
							Info: &armadaevents.KubernetesResourceInfo_PodInfo{
								PodInfo: &armadaevents.PodInfo{
									PodNumber: m.Pending.PodNumber,
								},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_Running:
		sequence.Queue = m.Running.Queue
		sequence.JobSetName = m.Running.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Running.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.Running.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Running.Created,
			Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunId: runId,
					JobId: jobId,
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								Namespace:    m.Running.PodNamespace,
								Name:         m.Running.PodName,
								KubernetesId: m.Running.KubernetesId,
								ExecutorId:   m.Running.ClusterId,
								// TODO: These should be included.
								Annotations: nil,
								Labels:      nil,
							},
							Info: &armadaevents.KubernetesResourceInfo_PodInfo{
								PodInfo: &armadaevents.PodInfo{
									NodeName:  m.Running.NodeName,
									PodNumber: m.Running.PodNumber,
								},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_UnableToSchedule:
		sequence.Queue = m.UnableToSchedule.Queue
		sequence.JobSetName = m.UnableToSchedule.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.UnableToSchedule.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.UnableToSchedule.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.UnableToSchedule.Created,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true, // EventMessage_UnableToSchedule indicates a failed job.
							Reason: &armadaevents.Error_PodUnschedulable{
								PodUnschedulable: &armadaevents.PodUnschedulable{
									ObjectMeta: &armadaevents.ObjectMeta{
										ExecutorId:   m.UnableToSchedule.ClusterId,
										Namespace:    m.UnableToSchedule.PodNamespace,
										Name:         m.UnableToSchedule.PodName,
										KubernetesId: m.UnableToSchedule.KubernetesId,
										Annotations:  nil,
										Labels:       nil,
									},
									Message:   m.UnableToSchedule.Reason,
									NodeName:  m.UnableToSchedule.NodeName,
									PodNumber: m.UnableToSchedule.PodNumber,
								},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_Failed:
		sequence.Queue = m.Failed.Queue
		sequence.JobSetName = m.Failed.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Failed.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.Failed.KubernetesId)
		if err != nil {
			// If a job fails without ever being assigned to a node, there won't be a KubernetesId.
			runId = LegacyJobRunId()
		}

		// EventMessage_Failed contains one error for each container.
		// Convert each of these to the corresponding Pulsar error.
		containerErrors := make([]*armadaevents.ContainerError, 0, len(m.Failed.ContainerStatuses))
		for _, st := range m.Failed.ContainerStatuses {
			containerError := &armadaevents.ContainerError{
				ExitCode: st.ExitCode,
				Message:  st.Message,
				Reason:   st.Reason,
				ObjectMeta: &armadaevents.ObjectMeta{
					ExecutorId:   m.Failed.ClusterId,
					Namespace:    m.Failed.PodNamespace,
					Name:         st.Name,
					KubernetesId: "", // only the id of the pod is stored in the failed message
				},
			}

			// Legacy messages encode the reason as an enum, whereas Pulsar uses objects.
			switch st.Cause {
			case api.Cause_DeadlineExceeded:
				containerError.KubernetesReason = armadaevents.KubernetesReason_DeadlineExceeded
			case api.Cause_Error:
				containerError.KubernetesReason = armadaevents.KubernetesReason_AppError
			case api.Cause_Evicted:
				containerError.KubernetesReason = armadaevents.KubernetesReason_Evicted
			case api.Cause_OOM:
				containerError.KubernetesReason = armadaevents.KubernetesReason_OOM
			default:
				log.Warnf("Unknown cause %s on container %s", st.Cause, st.Name)
			}

			containerErrors = append(containerErrors, containerError)
		}

		podError := &armadaevents.PodError{
			ObjectMeta: &armadaevents.ObjectMeta{
				ExecutorId:   m.Failed.ClusterId,
				Namespace:    m.Failed.PodNamespace,
				Name:         m.Failed.PodName,
				KubernetesId: m.Failed.KubernetesId,
			},
			Message:         m.Failed.Reason,
			NodeName:        m.Failed.NodeName,
			PodNumber:       m.Failed.PodNumber,
			ContainerErrors: containerErrors,
		}

		switch m.Failed.Cause {
		case api.Cause_DeadlineExceeded:
			podError.KubernetesReason = armadaevents.KubernetesReason_DeadlineExceeded
		case api.Cause_Error:
			podError.KubernetesReason = armadaevents.KubernetesReason_AppError
		case api.Cause_Evicted:
			podError.KubernetesReason = armadaevents.KubernetesReason_Evicted
		case api.Cause_OOM:
			podError.KubernetesReason = armadaevents.KubernetesReason_OOM
		default:
			log.Warnf("Unknown cause %s for job %s", m.Failed.Cause, m.Failed.JobId)
		}

		// Event indicating the job run failed.
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Failed.Created,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason: &armadaevents.Error_PodError{
								PodError: podError,
							},
						},
					},
				},
			},
		})

		// Event indicating that the job as a whole failed.
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Failed.Created,
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason: &armadaevents.Error_PodError{
								PodError: podError,
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_Succeeded:
		sequence.Queue = m.Succeeded.Queue
		sequence.JobSetName = m.Succeeded.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Succeeded.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.Succeeded.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Succeeded.Created,
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId: runId,
					JobId: jobId,
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								Namespace:    m.Succeeded.PodNamespace,
								Name:         m.Succeeded.PodName,
								KubernetesId: m.Succeeded.KubernetesId,
								// TODO: These should be included.
								Annotations: nil,
								Labels:      nil,
							},
							Info: &armadaevents.KubernetesResourceInfo_PodInfo{
								PodInfo: &armadaevents.PodInfo{
									NodeName:  m.Succeeded.NodeName,
									PodNumber: m.Succeeded.PodNumber,
								},
							},
						},
					},
				},
			},
		})
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Succeeded.Created,
			Event: &armadaevents.EventSequence_Event_JobSucceeded{
				JobSucceeded: &armadaevents.JobSucceeded{
					JobId: jobId,
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								Namespace:    m.Succeeded.PodNamespace,
								Name:         m.Succeeded.PodName,
								KubernetesId: m.Succeeded.KubernetesId,
								// TODO: These should be included.
								Annotations: nil,
								Labels:      nil,
							},
							Info: &armadaevents.KubernetesResourceInfo_PodInfo{
								PodInfo: &armadaevents.PodInfo{
									NodeName:  m.Succeeded.NodeName,
									PodNumber: m.Succeeded.PodNumber,
								},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_Reprioritized:
		sequence.Queue = m.Reprioritized.Queue
		sequence.JobSetName = m.Reprioritized.JobSetId
		sequence.UserId = m.Reprioritized.Requestor
		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Reprioritized.JobId)
		if err != nil {
			return nil, err
		}
		priority := LogSubmitPriorityFromApiPriority(m.Reprioritized.NewPriority)
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Reprioritized.Created,
			Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
				ReprioritisedJob: &armadaevents.ReprioritisedJob{
					JobId:    jobId,
					Priority: priority,
				},
			},
		})
	case *api.EventMessage_Cancelling:
		// Do nothing; there's no corresponding Pulsar message.
	case *api.EventMessage_Cancelled:
		sequence.Queue = m.Cancelled.Queue
		sequence.JobSetName = m.Cancelled.JobSetId
		sequence.UserId = m.Cancelled.Requestor

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Cancelled.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Cancelled.Created,
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{
					JobId: jobId,
				},
			},
		})
	case *api.EventMessage_Terminated:
		sequence.Queue = m.Terminated.Queue
		sequence.JobSetName = m.Terminated.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Terminated.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.Terminated.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Terminated.Created,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason: &armadaevents.Error_PodTerminated{
								PodTerminated: &armadaevents.PodTerminated{
									ObjectMeta: &armadaevents.ObjectMeta{
										ExecutorId:   m.Terminated.ClusterId,
										Namespace:    m.Terminated.PodNamespace,
										Name:         m.Terminated.PodName,
										KubernetesId: m.Terminated.KubernetesId,
									},
									PodNumber: m.Terminated.PodNumber,
									Message:   m.Terminated.Reason,
								},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_Utilisation:
		sequence.Queue = m.Utilisation.Queue
		sequence.JobSetName = m.Utilisation.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Utilisation.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.Utilisation.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.Utilisation.Created,
			Event: &armadaevents.EventSequence_Event_ResourceUtilisation{
				ResourceUtilisation: &armadaevents.ResourceUtilisation{
					RunId: runId,
					JobId: jobId,
					ResourceInfo: &armadaevents.KubernetesResourceInfo{
						ObjectMeta: &armadaevents.ObjectMeta{
							ExecutorId:   m.Utilisation.ClusterId,
							KubernetesId: m.Utilisation.KubernetesId,
							Namespace:    m.Utilisation.PodNamespace,
							Name:         m.Utilisation.PodName,
						},
						Info: &armadaevents.KubernetesResourceInfo_PodInfo{
							PodInfo: &armadaevents.PodInfo{
								NodeName:  m.Utilisation.NodeName,
								PodNumber: m.Utilisation.PodNumber,
							},
						},
					},
					MaxResourcesForPeriod: m.Utilisation.MaxResourcesForPeriod,
					TotalCumulativeUsage:  m.Utilisation.TotalCumulativeUsage,
				},
			},
		})
	case *api.EventMessage_IngressInfo:
		// Later, ingress info should be bundled with the JobRunRunning message.
		// For now, we create a special message that exists only for compatibility with the legacy messages.

		sequence.Queue = m.IngressInfo.Queue
		sequence.JobSetName = m.IngressInfo.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.IngressInfo.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.IngressInfo.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: &m.IngressInfo.Created,
			Event: &armadaevents.EventSequence_Event_StandaloneIngressInfo{
				StandaloneIngressInfo: &armadaevents.StandaloneIngressInfo{
					RunId: runId,
					JobId: jobId,
					ObjectMeta: &armadaevents.ObjectMeta{
						ExecutorId:   m.IngressInfo.ClusterId,
						Namespace:    m.IngressInfo.PodNamespace, // We assume the ingress was created with the same namespace as the pod
						KubernetesId: m.IngressInfo.KubernetesId,
					},
					IngressAddresses: m.IngressInfo.IngressAddresses,
					NodeName:         m.IngressInfo.NodeName,
					PodName:          m.IngressInfo.PodName,
					PodNumber:        m.IngressInfo.PodNumber,
					PodNamespace:     m.IngressInfo.PodNamespace,
				},
			},
		})
	case *api.EventMessage_Reprioritizing:
		// Do nothing; there's no corresponding Pulsar message.
	case *api.EventMessage_Updated:
		// Do nothing; we're not allowing arbitrary job updates.
	case *api.EventMessage_Preempted:
		sequence.Queue = m.Preempted.Queue
		sequence.JobSetName = m.Preempted.JobSetId

		preemptedJobId, err := armadaevents.ProtoUuidFromUlidString(m.Preempted.JobId)
		if err != nil {
			return nil, err
		}
		preemptedRunId, err := armadaevents.ProtoUuidFromUuidString(m.Preempted.RunId)
		if err != nil {
			return nil, err
		}

		jobRunPreempted := &armadaevents.JobRunPreempted{
			PreemptedJobId: preemptedJobId,
			PreemptedRunId: preemptedRunId,
		}

		if m.Preempted.PreemptiveJobId != "" {
			preemptiveJobId, err := armadaevents.ProtoUuidFromUlidString(m.Preempted.PreemptiveJobId)
			if err != nil {
				return nil, err
			}
			jobRunPreempted.PreemptiveJobId = preemptiveJobId
		}
		if m.Preempted.PreemptiveRunId != "" {
			preemptiveRunId, err := armadaevents.ProtoUuidFromUuidString(m.Preempted.PreemptiveRunId)
			if err != nil {
				return nil, err
			}
			jobRunPreempted.PreemptiveRunId = preemptiveRunId
		}

		event := &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: jobRunPreempted,
		}
		sequenceEvent := &armadaevents.EventSequence_Event{
			Created: &m.Preempted.Created,
			Event:   event,
		}
		sequence.Events = append(sequence.Events, sequenceEvent)
	default:
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "msg",
			Value:   msg,
			Message: "received unsupported api message",
		}
		err = errors.WithStack(err)
		return nil, err
	}

	return sequence, nil
}

// LEGACY_RUN_ID is used for messages for which we can't use the kubernetesId.
const LEGACY_RUN_ID = "00000000-0000-0000-0000-000000000000"

func LegacyJobRunId() *armadaevents.Uuid {
	jobRunId, err := armadaevents.ProtoUuidFromUuidString(LEGACY_RUN_ID)
	if err != nil {
		panic(err)
	}
	return jobRunId
}
