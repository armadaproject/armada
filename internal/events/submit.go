package events

import (
	"context"
	"crypto/sha256"
	"log"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
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
	// Used to create k8s objects from the submitted ingresses and services.
	// TODO: This needs to be populated. The executor loads config from disk to do so.
	IngressConfig *executorconfig.IngressConfiguration
	queueRepository repository.QueueRepository
	permissions authorization.PermissionChecker
}

// TODO: Add input validation to make sure messages can be inserted to the database.
// TODO: Check job size and reject jobs that could never be scheduled. Maybe by querying the scheduler for its limits.
func (srv *PulsarSubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, err
	}

	// Prepare an event sequence to be submitted to the log
	sequence := EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		JobSetHash: hashFromJobSetName(req.JobSetId),
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*EventSequence_Event, len(req.JobRequestItems), len(req.JobRequestItems)),
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
		objects := make([]*KubernetesObject, 0, len(services)+len(ingresses))
		for _, service := range services {
			objects = append(objects, &KubernetesObject{
				Object: &KubernetesObject_Service{Service: service},
			})
		}
		for _, ingress := range ingresses {
			objects = append(objects, &KubernetesObject{
				Object: &KubernetesObject_Ingress{Ingress: ingress},
			})
		}

		// Main object associated with the job.
		// TODO: Consider using podspecs instead here and create a separate API that uses podspec instead.
		mainObject := &KubernetesMainObject{
			Object: &KubernetesMainObject_PodSpec{
				PodSpec: &PodSpecWithAvoidList{
					PodSpec: r.PodSpec,
				},
			},
		}

		// Fully formed job creation event to be added to the sequence.
		submitJob := &SubmitJob{
			JobId:           jobId,
			DeduplicationId: r.ClientId,
			Priority:        r.Priority,
			Namespace:       r.Namespace,
			Labels:          r.Labels,
			Annotations:     r.Annotations,
			MainObject:      mainObject,
			Objects:         objects,
		}
		sequence.Events[i] = &EventSequence_Event{
			Event: &EventSequence_Event_SubmitJob{
				SubmitJob: submitJob,
			},
		}
	}
	// TODO: Submit sequence to the log.
	return &api.JobSubmitResponse{JobResponseItems: responses}, nil
}

func (srv *PulsarSubmitServer) CancelJobs(ctx context.Context, req *api.JobCancelRequest) (*api.CancellationResult, error) {
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	sequence := EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		JobSetHash: hashFromJobSetName(req.JobSetId),
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*EventSequence_Event, 1, 1),
	}
	sequence.Events[1] = &EventSequence_Event{
		Event: &EventSequence_Event_CancelJob{
			CancelJob: &CancelJob{JobId: req.JobId},
		},
	}
	log.Printf("CancelJobs: %v", sequence)
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
	sequence := EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		JobSetHash: hashFromJobSetName(req.JobSetId),
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*EventSequence_Event, len(req.JobIds), len(req.JobIds)),
	}
	for i, jobId := range req.JobIds {
		sequence.Events[i] = &EventSequence_Event{
			Event: &EventSequence_Event_ReprioritiseJob{
				ReprioritiseJob: &ReprioritiseJob{
					JobId:    jobId,
					Priority: req.NewPriority,
				},
			},
		}
		results[jobId] = "" // TODO: what do we put here?
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
	q, err := srv.queueRepository.GetQueue(queueName)
	if err != nil {
		return
	}
	if !srv.permissions.UserHasPermission(ctx, anyPerm) {
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