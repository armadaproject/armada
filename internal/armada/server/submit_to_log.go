package server

import (
	"context"
	"crypto/sha256"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/events"
	executorconfig "github.com/G-Research/armada/internal/executor/configuration"
	executorutil "github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

// Server that accepts API calls according to the original Armada submit API
// and publishes messages to Pulsar based on those calls.
// TODO: Consider returning a list of message ids of the messages generated
type PulsarSubmitServer struct {
	api.UnimplementedSubmitServer
	Producer        pulsar.Producer
	Permissions     authorization.PermissionChecker
	QueueRepository repository.QueueRepository
	// Fall back to the legacy submit server for queue administration endpoints.
	SubmitServer *SubmitServer
	// Used to create k8s objects from the submitted ingresses and services.
	IngressConfig *executorconfig.IngressConfiguration
}

// TODO: Add input validation to make sure messages can be inserted to the database.
// TODO: Check job size and reject jobs that could never be scheduled. Maybe by querying the scheduler for its limits.
func (srv *PulsarSubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {

	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, err
	}

	// Prepare an event sequence to be submitted to the log
	sequence := &events.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		JobSetHash: hashFromJobSetName(req.JobSetId),
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*events.EventSequence_Event, len(req.JobRequestItems), len(req.JobRequestItems)),
	}

	// Populate the events in the sequence
	responses := make([]*api.JobSubmitResponseItem, len(req.JobRequestItems), len(req.JobRequestItems))
	for i, r := range req.JobRequestItems {
		jobId := uuid.New().String()
		responses[i] = &api.JobSubmitResponseItem{}
		responses[i].JobId = jobId

		// Create k8s objects from the data embedded in the request.
		// GenerateIngresses expects a job object, but only uses a subset of its fields.
		// Hence, we create a job object with the needed fields populated.
		job := &api.Job{
			Ingress:     r.Ingress,
			Services:    r.Services,
			Labels:      r.Labels,
			Annotations: r.Annotations,
			JobSetId:    req.JobSetId,
			Owner:       userId,
			Namespace:   r.Namespace,
		}
		pod := &v1.Pod{
			Spec: *r.PodSpec,
		}
		pod.Labels = map[string]string{} // TODO: Do we need to put something here?
		services, ingresses := executorutil.GenerateIngresses(job, pod, srv.IngressConfig)

		// Move those objects into a list that can be submitted to the log.
		objects := make([]*events.KubernetesObject, 0, len(services)+len(ingresses))
		for _, service := range services {
			// Default to using the ObjectMeta details provided in the job.
			objectMeta := &events.ObjectMeta{
				Namespace:   r.Namespace,
				Annotations: r.Annotations,
				Labels:      r.Labels,
			}

			// Override the defaults with any info provided from executorutil.GenerateIngresses.
			if service.ObjectMeta.Namespace != "" {
				objectMeta.Namespace = service.ObjectMeta.Namespace
			}
			if service.ObjectMeta.Annotations != nil {
				objectMeta.Annotations = service.ObjectMeta.Annotations
			}
			if service.ObjectMeta.Labels != nil {
				objectMeta.Labels = service.ObjectMeta.Labels
			}

			objects = append(objects, &events.KubernetesObject{
				ObjectMeta: objectMeta,
				Object: &events.KubernetesObject_Service{
					Service: &service.Spec,
				},
			})
		}
		for _, ingress := range ingresses {
			// Default to using the ObjectMeta details provided in the job.
			objectMeta := &events.ObjectMeta{
				Namespace:   r.Namespace,
				Annotations: r.Annotations,
				Labels:      r.Labels,
			}

			// Override the defaults with any info provided from executorutil.GenerateIngresses.
			if ingress.ObjectMeta.Namespace != "" {
				objectMeta.Namespace = ingress.ObjectMeta.Namespace
			}
			if ingress.ObjectMeta.Annotations != nil {
				objectMeta.Annotations = ingress.ObjectMeta.Annotations
			}
			if ingress.ObjectMeta.Labels != nil {
				objectMeta.Labels = ingress.ObjectMeta.Labels
			}

			objects = append(objects, &events.KubernetesObject{
				ObjectMeta: objectMeta,
				Object: &events.KubernetesObject_Ingress{
					Ingress: &ingress.Spec,
				},
			})
		}

		// Each job has a main object associated with it, which determines when the job exits.
		// If provided, use r.PodSpec as the main object. Otherwise, try to use r.PodSpecs[0].
		mainPodSpec := r.PodSpec
		additionalPodSpecs := r.PodSpecs
		if additionalPodSpecs == nil {
			additionalPodSpecs = make([]*v1.PodSpec, 0)
		}
		if mainPodSpec == nil && len(additionalPodSpecs) > 0 {
			mainPodSpec = additionalPodSpecs[0]
			additionalPodSpecs = additionalPodSpecs[1:]
		}
		mainObject := &events.KubernetesMainObject{
			Object: &events.KubernetesMainObject_PodSpec{
				PodSpec: &events.PodSpecWithAvoidList{
					PodSpec: mainPodSpec,
				},
			},
		}

		// Add any additional pod specs into the list of additional objects.
		for _, podSpec := range additionalPodSpecs {
			objects = append(objects, &events.KubernetesObject{
				Object: &events.KubernetesObject_PodSpec{
					PodSpec: &events.PodSpecWithAvoidList{
						PodSpec: podSpec,
					},
				},
			})
		}

		// Fully formed job creation event to be added to the sequence.
		submitJob := &events.SubmitJob{
			JobId:           jobId,
			DeduplicationId: r.ClientId,
			Priority:        r.Priority,
			ObjectMeta: &events.ObjectMeta{
				Namespace:   r.Namespace,
				Annotations: r.Labels,
				Labels:      r.Annotations,
			},
			MainObject: mainObject,
			Objects:    objects,
		}
		sequence.Events[i] = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_SubmitJob{
				SubmitJob: submitJob,
			},
		}
	}

	payload, err := proto.Marshal(sequence)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal event sequence")
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId, ok := requestid.FromContext(ctx)
	if !ok {
		requestId = "missing"
	}
	_, err = srv.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: map[string]string{requestid.MetadataKey: requestId},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send message")
	}

	return &api.JobSubmitResponse{JobResponseItems: responses}, nil
}

func (srv *PulsarSubmitServer) CancelJobs(ctx context.Context, req *api.JobCancelRequest) (*api.CancellationResult, error) {
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	sequence := &events.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		JobSetHash: hashFromJobSetName(req.JobSetId),
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*events.EventSequence_Event, 1, 1),
	}
	sequence.Events[1] = &events.EventSequence_Event{
		Event: &events.EventSequence_Event_CancelJob{
			CancelJob: &events.CancelJob{JobId: req.JobId},
		},
	}

	payload, err := proto.Marshal(sequence)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal event sequence")
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId, ok := requestid.FromContext(ctx)
	if !ok {
		requestId = "missing"
	}
	_, err = srv.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: map[string]string{requestid.MetadataKey: requestId},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send message")
	}

	return &api.CancellationResult{
		CancelledIds: []string{req.JobId},
	}, nil
}

func (srv *PulsarSubmitServer) ReprioritizeJobs(ctx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.ReprioritizeAnyJobs, queue.PermissionVerbReprioritize)
	if err != nil {
		return nil, err
	}

	// TODO: What gets put in the results?
	results := make(map[string]string)
	sequence := &events.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		JobSetHash: hashFromJobSetName(req.JobSetId),
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*events.EventSequence_Event, len(req.JobIds), len(req.JobIds)),
	}
	for i, jobId := range req.JobIds {
		sequence.Events[i] = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_ReprioritiseJob{
				ReprioritiseJob: &events.ReprioritiseJob{
					JobId:    jobId,
					Priority: req.NewPriority,
				},
			},
		}
		results[jobId] = "" // TODO: what do we put here?
	}

	payload, err := proto.Marshal(sequence)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal event sequence")
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId, ok := requestid.FromContext(ctx)
	if !ok {
		requestId = "missing"
	}
	_, err = srv.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: map[string]string{requestid.MetadataKey: requestId},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send message")
	}

	return &api.JobReprioritizeResponse{
		ReprioritizationResults: results,
	}, nil
}

func hashFromJobSetName(jobSetName string) []byte {
	hash := sha256.Sum256([]byte(jobSetName))
	return hash[:]
}

// Authorize authorizes a user request to submit a state transition message to the log.
// User information used for authorization is extracted from the provided context.
// Checks that the user has either anyPerm (e.g., permissions.SubmitAnyJobs) or perm (e.g., PermissionVerbSubmit) for this queue.
// Returns the userId and groups extracted from the context.
func (srv *PulsarSubmitServer) Authorize(ctx context.Context, queueName string, anyPerm permission.Permission, perm queue.PermissionVerb) (userId string, groups []string, err error) {
	principal := authorization.GetPrincipal(ctx)
	userId = principal.GetName()
	q, err := srv.QueueRepository.GetQueue(queueName)
	if err != nil {
		return
	}
	if !srv.Permissions.UserHasPermission(ctx, anyPerm) {
		if !principalHasQueuePermissions(principal, q, perm) {
			// TODO: Add an error to armadaerrors and use that instead. Use sibling errors instead of a list of reasons.
			err = &armadaerrors.ErrNoPermission{
				Principal:  principal.GetName(),
				Permission: string(perm),
				Action:     string(perm) + " for queue " + q.Name,
				Message:    "",
			}
			err = errors.WithStack(err)
			return
		}
	}

	// Armada impersonates the principal that submitted the job when interacting with k8s.
	// If the principal doesn't itself have sufficient perms, we check if it's part of any groups that do, and add those.
	// This is an optimisation to avoid passing around groups unnecessarily.
	principalSubject := queue.PermissionSubject{
		Name: userId,
		Kind: queue.PermissionSubjectKindUser,
	}
	if !q.HasPermission(principalSubject, perm) {
		for _, subject := range queue.NewPermissionSubjectsFromOwners(nil, principal.GetGroupNames()) {
			if q.HasPermission(subject, perm) {
				groups = append(groups, subject.Name)
			}
		}
	}
	return
}

// principalHasQueuePermissions returns true if the principal has permissions to perform some action,
// as specified by the provided verb, for a specific queue, and false otherwise.
func principalHasQueuePermissions(principal authorization.Principal, q queue.Queue, verb queue.PermissionVerb) bool {
	subjects := queue.PermissionSubjects{}
	for _, group := range principal.GetGroupNames() {
		subjects = append(subjects, queue.PermissionSubject{
			Name: group,
			Kind: queue.PermissionSubjectKindGroup,
		})
	}
	subjects = append(subjects, queue.PermissionSubject{
		Name: principal.GetName(),
		Kind: queue.PermissionSubjectKindUser,
	})

	for _, subject := range subjects {
		if q.HasPermission(subject, verb) {
			return true
		}
	}

	return false
}

// Fallback methods. Calls into an embedded server.SubmitServer.
func (srv *PulsarSubmitServer) CreateQueue(ctx context.Context, req *api.Queue) (*types.Empty, error) {
	return srv.SubmitServer.CreateQueue(ctx, req)
}
func (srv *PulsarSubmitServer) UpdateQueue(ctx context.Context, req *api.Queue) (*types.Empty, error) {
	return srv.SubmitServer.UpdateQueue(ctx, req)
}
func (srv *PulsarSubmitServer) DeleteQueue(ctx context.Context, req *api.QueueDeleteRequest) (*types.Empty, error) {
	return srv.SubmitServer.DeleteQueue(ctx, req)
}
func (srv *PulsarSubmitServer) GetQueue(ctx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	return srv.SubmitServer.GetQueue(ctx, req)
}
func (srv *PulsarSubmitServer) GetQueueInfo(ctx context.Context, req *api.QueueInfoRequest) (*api.QueueInfo, error) {
	return srv.GetQueueInfo(ctx, req)
}

// SubmitApiEvent converts an api.EventMessage into Pulsar state transition messages and publishes those to Pulsar.
func (srv *PulsarSubmitServer) SubmitApiEvent(ctx context.Context, apiEvent *api.EventMessage) error {
	sequence, err := PulsarSequenceFromApiEvent(apiEvent)
	if err != nil {
		return err
	}

	payload, err := proto.Marshal(sequence)
	if err != nil {
		err = errors.WithStack(err)
		return err
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId, ok := requestid.FromContext(ctx)
	if !ok {
		requestId = "missing"
	}
	_, err = srv.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: map[string]string{requestid.MetadataKey: requestId},
	})
	if err != nil {
		err = errors.WithStack(err)
		return err
	}

	return nil
}

// PulsarSequenceFromApiEvent converts an api.EventMessage into the corresponding Pulsar event
// and returns an EventSequence containing this single event.
//
// The following is a list of API messages. Those marked with x are handled by this function.
//	*EventMessage_Submitted
//	*EventMessage_Queued
//	*EventMessage_DuplicateFound
// x	*EventMessage_Leased
// x	*EventMessage_LeaseReturned
// x	*EventMessage_LeaseExpired
// x	*EventMessage_Pending
// x	*EventMessage_Running
// x	*EventMessage_UnableToSchedule
// x	*EventMessage_Failed
// x	*EventMessage_Succeeded
//	*EventMessage_Reprioritized
//	*EventMessage_Cancelling
//	*EventMessage_Cancelled
// x	*EventMessage_Terminated
//	*EventMessage_Utilisation
//	*EventMessage_IngressInfo
//	*EventMessage_Reprioritizing
//	*EventMessage_Updated
//
// The following is a list of Pulsar events. Those marked with x may be returned by this function.
//	*EventSequence_Event_JobSucceeded
// x	*EventSequence_Event_JobFailed
//	*EventSequence_Event_JobRejected
// x	*EventSequence_Event_JobRunLeased
// x	*EventSequence_Event_JobRunAssigned
// x	*EventSequence_Event_JobRunRunning
// x	*EventSequence_Event_JobRunReturned
// x	*EventSequence_Event_JobRunSucceeded
// x	*EventSequence_Event_JobRunFailed
func PulsarSequenceFromApiEvent(msg *api.EventMessage) (sequence *events.EventSequence, err error) {
	sequence = &events.EventSequence{}
	var event *events.EventSequence_Event

	switch m := msg.Events.(type) {
	case *api.EventMessage_Leased:
		sequence.Queue = m.Leased.Queue
		sequence.JobSetName = m.Leased.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunLeased{
				JobRunLeased: &events.JobRunLeased{
					RunId:      "legacy", // TODO: what do we do about this?
					JobId:      m.Leased.JobId,
					ExecutorId: m.Leased.ClusterId,
				},
			},
		}
	case *api.EventMessage_LeaseReturned:
		sequence.Queue = m.LeaseReturned.Queue
		sequence.JobSetName = m.LeaseReturned.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunFailed{
				JobRunFailed: &events.JobRunFailed{
					RunId: "",
					JobId: m.LeaseReturned.JobId,
					// TODO: Create specific reason types instead of using UnknownReason.
					Reason: &events.JobRunFailed_UnknownReason_{
						UnknownReason: &events.JobRunFailed_UnknownReason{
							Reason: m.LeaseReturned.Reason,
						},
					},
				},
			},
		}
	case *api.EventMessage_LeaseExpired:
		sequence.Queue = m.LeaseExpired.Queue
		sequence.JobSetName = m.LeaseExpired.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunReturned{
				JobRunReturned: &events.JobRunReturned{
					RunId:  "legacy", // TODO: what do we do about this?
					JobId:  m.LeaseExpired.JobId,
					Reason: &events.JobRunReturned_LeaseExpired_{},
				},
			},
		}
	case *api.EventMessage_Pending:
		sequence.Queue = m.Pending.Queue
		sequence.JobSetName = m.Pending.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunAssigned{
				JobRunAssigned: &events.JobRunAssigned{
					RunId: "legacy", // TODO: what do we do about this?
					JobId: m.Pending.JobId,
					RunInfo: &events.JobRunAssigned_PodRunInfo{
						// TODO: Consider changing these
						PodRunInfo: &events.PodRunInfo{
							ClusterId:    m.Pending.ClusterId, // TODO: Should be executor id
							KubernetesId: m.Pending.KubernetesId,
							NodeName:     "", // TODO: Remove
							PodNumber:    m.Pending.PodNumber,
							PodName:      m.Pending.PodName,
							PodNamespace: m.Pending.PodNamespace,
						},
					},
				},
			},
		}
	case *api.EventMessage_Running:
		sequence.Queue = m.Running.Queue
		sequence.JobSetName = m.Running.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunRunning{
				JobRunRunning: &events.JobRunRunning{
					RunId: "legacy", // TODO: what do we do about this?
					JobId: m.Running.JobId,
					RunInfo: &events.JobRunRunning_PodRunInfo{
						// TODO: Consider changing these
						PodRunInfo: &events.PodRunInfo{
							ClusterId:    m.Running.ClusterId, // TODO: Should be executor id
							KubernetesId: m.Running.KubernetesId,
							NodeName:     m.Running.NodeName,
							PodNumber:    m.Running.PodNumber,
							PodName:      m.Running.PodName,
							PodNamespace: m.Running.PodNamespace,
						},
					},
				},
			},
		}
	case *api.EventMessage_UnableToSchedule:
		sequence.Queue = m.UnableToSchedule.Queue
		sequence.JobSetName = m.UnableToSchedule.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunFailed{
				JobRunFailed: &events.JobRunFailed{
					RunId: "legacy", // TODO: what do we do about this?
					JobId: m.UnableToSchedule.JobId,
					Reason: &events.JobRunFailed_JobUnschedulable_{
						JobUnschedulable: &events.JobRunFailed_JobUnschedulable{
							ClusterId:    m.UnableToSchedule.ClusterId,
							Reason:       m.UnableToSchedule.Reason,
							KubernetesId: m.UnableToSchedule.KubernetesId,
							NodeName:     m.UnableToSchedule.NodeName,
							PodNumber:    m.UnableToSchedule.PodNumber,
							PodName:      m.UnableToSchedule.PodName,
							PodNamespace: m.UnableToSchedule.PodNamespace,
						},
					},
				},
			},
		}
	case *api.EventMessage_Failed:
		sequence.Queue = m.Failed.Queue
		sequence.JobSetName = m.Failed.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunFailed{
				JobRunFailed: &events.JobRunFailed{
					RunId: "legacy", // TODO: what do we do about this?
					JobId: m.Failed.JobId,
					Reason: &events.JobRunFailed_ApplicationFailed_{
						// TODO: Fill in details
						ApplicationFailed: &events.JobRunFailed_ApplicationFailed{
							Code:  0,
							Error: "",
						},
					},
				},
			},
		}
	case *api.EventMessage_Succeeded:
		sequence.Queue = m.Succeeded.Queue
		sequence.JobSetName = m.Succeeded.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &events.JobRunSucceeded{
					RunId: "legacy", // TODO: what do we do about this?
					JobId: m.Succeeded.JobId,
				},
			},
		}
	case *api.EventMessage_Terminated:
		sequence.Queue = m.Terminated.Queue
		sequence.JobSetName = m.Terminated.JobSetId
		event = &events.EventSequence_Event{
			Event: &events.EventSequence_Event_JobFailed{
				JobFailed: &events.JobFailed{
					JobId: m.Terminated.JobId,
					Reason: &events.JobFailed_MaxRunsExceeded_{
						MaxRunsExceeded: &events.JobFailed_MaxRunsExceeded{},
					},
				},
			},
		}
	default:
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "msg",
			Value:   msg,
			Message: "received unsupported api message",
		}
		err = errors.WithStack(err)
		return nil, err
	}

	sequence.Events = []*events.EventSequence_Event{event}
	return sequence, nil
}
