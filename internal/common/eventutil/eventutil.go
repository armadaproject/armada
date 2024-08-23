package eventutil

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// UnmarshalEventSequence returns an EventSequence object contained in a byte buffer
// after validating that the resulting EventSequence is valid.
func UnmarshalEventSequence(ctx *armadacontext.Context, payload []byte) (*armadaevents.EventSequence, error) {
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

// ApiJobFromLogSubmitJob converts a SubmitJob log message into an api.Job struct, which is used by Armada internally.
func ApiJobFromLogSubmitJob(ownerId string, groups []string, queueName string, jobSetName string, time time.Time, e *armadaevents.SubmitJob) (*api.Job, error) {
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

	// Compute the overall resource requirements necessary for scheduling.
	schedulingResourceRequirements := api.SchedulingResourceRequirementsFromPodSpec(podSpec)

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
		Id:       e.JobIdStr,
		ClientId: e.DeduplicationId,
		Queue:    queueName,
		JobSetId: jobSetName,

		Namespace:   e.ObjectMeta.Namespace,
		Labels:      e.ObjectMeta.Labels,
		Annotations: e.ObjectMeta.Annotations,

		K8SIngress: k8sIngresses,
		K8SService: k8sServices,

		Priority: float64(e.Priority),

		PodSpec:                        podSpec,
		PodSpecs:                       podSpecs,
		SchedulingResourceRequirements: &schedulingResourceRequirements,

		Created:                  protoutil.ToTimestamp(time),
		Owner:                    ownerId,
		QueueOwnershipUserGroups: groups,
	}, nil
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

func LimitSequencesEventMessageCount(sequences []*armadaevents.EventSequence, maxEventsPerSequence int) []*armadaevents.EventSequence {
	rv := make([]*armadaevents.EventSequence, 0, len(sequences))
	for _, sequence := range sequences {
		if len(sequence.Events) > maxEventsPerSequence {
			splitEventMessages := slices.PartitionToMaxLen(sequence.Events, maxEventsPerSequence)

			for _, eventMessages := range splitEventMessages {
				rv = append(rv, &armadaevents.EventSequence{
					Queue:      sequence.Queue,
					JobSetName: sequence.JobSetName,
					UserId:     sequence.UserId,
					Groups:     sequence.Groups,
					Events:     eventMessages,
				})
			}

		} else {
			rv = append(rv, sequence)
		}
	}
	return rv
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

// This is an (over)estimate of the byte overhead used to represent the list EventSequence.Events
// We need this get a safe estimate for the headerSize in LimitSequenceByteSize
// We cannot simply rely on proto.Size on an EventSequence with an empty Event list,
// as proto is smart enough to realise it is empty and just nils it out for 0 bytes
const sequenceEventListOverheadSizeBytes = 100

// LimitSequenceByteSize returns a slice of sequences produced by breaking up sequence.Events into separate sequences
// If strict is true, each sequence will be at most sizeInBytes bytes in size
// If strict is false, sizeInBytes can be exceeded by at most the size of a single sequence.Event
func LimitSequenceByteSize(sequence *armadaevents.EventSequence, sizeInBytes uint, strict bool) ([]*armadaevents.EventSequence, error) {
	// Compute the size of the sequence without events.
	events := sequence.Events
	sequence.Events = make([]*armadaevents.EventSequence_Event, 0)
	headerSize := uint(proto.Size(sequence)) + sequenceEventListOverheadSizeBytes
	sequence.Events = events

	sequences := make([]*armadaevents.EventSequence, 0, 1)
	lastSequenceEventSize := uint(0)
	for _, event := range sequence.Events {
		eventSize := uint(proto.Size(event))
		if eventSize+headerSize > sizeInBytes && strict {
			return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:  "sequence",
				Value: sequence,
				Message: fmt.Sprintf(
					"event of %d bytes is too large, when combined with a header of size %d is larger than the sequence size limit of %d",
					eventSize,
					headerSize,
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
