package server

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/requestid"
	executorconfig "github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/pgkeyvalue"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/G-Research/armada/pkg/client/queue"
)

// Id used for runs for which one was not generated by the system
// (currently used for all runs, since Armada does not generate run ids).
const LEGACY_RUN_ID = "00000000000000000000000000"

// PulsarSubmitServer is a service that accepts API calls according to the original Armada submit API
// and publishes messages to Pulsar based on those calls.
// TODO: Consider returning a list of message ids of the messages generated
// TODO: Include job set as the message key for each message
type PulsarSubmitServer struct {
	api.UnimplementedSubmitServer
	Producer        pulsar.Producer
	Permissions     authorization.PermissionChecker
	QueueRepository repository.QueueRepository
	// Fall back to the legacy submit server for queue administration endpoints.
	SubmitServer *SubmitServer
	// Used to create k8s objects from the submitted ingresses and services.
	IngressConfig *executorconfig.IngressConfiguration
	KVStore       *pgkeyvalue.PGKeyValueStore
}

// TODO: Add input validation to make sure messages can be inserted to the database.
// TODO: Check job size and reject jobs that could never be scheduled. Maybe by querying the scheduler for its limits.
func (srv *PulsarSubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {

	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, err
	}

	// If enabled, filter out duplicate job submissions.
	if srv.KVStore != nil {
		jobRequestItems, err := srv.removeDuplicateSubmissions(ctx, req.Queue, req.JobRequestItems)
		if err != nil {
			return nil, err
		}
		req.JobRequestItems = jobRequestItems
	}

	// Prepare an event sequence to be submitted to the log
	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, len(req.JobRequestItems), len(req.JobRequestItems)),
	}

	// Create legacy API jobs from the requests.
	// We use the legacy code for the conversion to ensure that behavior doesn't change.
	apiJobs, err := srv.SubmitServer.createJobs(req, userId, groups)
	if err != nil {
		return nil, err
	}

	// Convert the API jobs to log jobs.
	responses := make([]*api.JobSubmitResponseItem, len(req.JobRequestItems), len(req.JobRequestItems))
	for i, apiJob := range apiJobs {
		responses[i] = &api.JobSubmitResponseItem{
			JobId: apiJob.GetId(),
		}

		if apiJob.PodSpec == nil && len(apiJob.PodSpecs) == 0 {
			return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "PodSpec",
				Value:   apiJob.PodSpec,
				Message: "Job does not contain at least one PodSpec",
			})
		}

		// We only support jobs with a single PodSpec, and it must be set to r.PodSpec.
		if apiJob.PodSpec == nil && len(apiJob.PodSpecs) == 1 {
			apiJob.PodSpec = apiJob.PodSpecs[0]
			apiJob.PodSpecs = nil
		}

		// I'm not convinced that the code to create services/ingresses when multiple pods are submitted is correct.
		// In particular, we do not create a full set of services/ingresses for each pod.
		// Hence, we return an error until we can make sure that the code is correct.
		// The next error is redundant with this one, but we leave both since we may wish to remove this one.
		// - Albin
		if len(apiJob.PodSpecs) > 0 {
			return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "PodSpecs",
				Value:   apiJob.PodSpecs,
				Message: "Jobs with multiple pods are not supported",
			})
		}

		// Although the code for submitting to Pulsar (below) supports setting both r.PodSpec and r.PodSpecs,
		// the executor code does not (e.g., executorutil.CreatePod). We may be able to merge them,
		// but we should do more testing to make sure it's safe before we allow it.
		// - Albin
		if len(apiJob.PodSpecs) > 0 && apiJob.PodSpec != nil {
			return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "PodSpec",
				Value:   apiJob.PodSpec,
				Message: "PodSpec must be nil if PodSpecs is provided (i.e., these are exclusive)",
			})
		}

		// Users submit API-specific service and ingress objects.
		// However, the log only accepts proper k8s objects.
		// Hence, the API-specific objects must be converted to proper k8s objects.
		err = eventutil.PopulateK8sServicesIngresses(apiJob, srv.IngressConfig)
		if err != nil {
			return nil, err
		}

		// The log accept a different type of job.
		logJob, err := eventutil.LogSubmitJobFromApiJob(apiJob)
		if err != nil {
			return nil, err
		}

		// Try converting the log job back to an API job to make sure there are no errors.
		// The log consumer will do this again; we do it here to ensure that any errors are noticed immediately.
		_, err = eventutil.ApiJobFromLogSubmitJob(userId, groups, req.Queue, req.JobSetId, time.Now(), logJob)
		if err != nil {
			return nil, err
		}

		sequence.Events[i] = &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: logJob,
			},
		}
	}

	payload, err := proto.Marshal(sequence)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to marshal event sequence")
	}

	// Create events marking the jobs as submitted
	err = reportSubmitted(srv.SubmitServer.eventStore, apiJobs)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "[SubmitJobs] error getting submitted report: %s", err)
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId := requestid.FromContextOrMissing(ctx)
	msg := &pulsar.ProducerMessage{
		Payload:    payload,
		Properties: map[string]string{requestid.MetadataKey: requestId},
		Key:        req.JobSetId,
	}
	pulsarrequestid.AddToMessage(msg, requestId)
	_, err = srv.Producer.Send(ctx, msg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send message")
	}

	return &api.JobSubmitResponse{JobResponseItems: responses}, nil
}

// removeDuplicateSubmissions filters out job submit request items with previously seen client ids.
// Client ids are namespaced by queue.
func (srv *PulsarSubmitServer) removeDuplicateSubmissions(ctx context.Context, queue string, items []*api.JobSubmitRequestItem) ([]*api.JobSubmitRequestItem, error) {

	// For storage efficiency, we store hashes instead of user-provided strings.
	// For computational efficiency, we create a lookup table to avoid computing the same hash twice.
	// Client ids are namespaced by queue. Hence, we hash the client id together with the queue.
	jobSubmitRequestItems := make([]*api.JobSubmitRequestItem, 0, len(items))
	combinedHashData := make([]byte, 40)
	queueHash := sha1.Sum([]byte(queue))
	for i, b := range queueHash {
		combinedHashData[i] = b
	}
	combinedHhashFromClientId := make(map[string][20]byte)
	for _, item := range items {

		// Empty ClientId indicates no deduplication.
		if item.ClientId == "" {
			jobSubmitRequestItems = append(jobSubmitRequestItems, item)
			continue
		}

		// Otherwise hash the ClientId (or get it from the table, if possible).
		combinedHash, ok := combinedHhashFromClientId[item.ClientId]
		if !ok {
			clientIdHash := sha1.Sum([]byte(item.ClientId))

			// Compute the combined hash.
			for i, b := range clientIdHash {
				combinedHashData[i+20] = b
			}
			combinedHash = sha1.Sum(combinedHashData)
			combinedHhashFromClientId[item.ClientId] = combinedHash
		}

		// Check if we've seen the hash before.
		// ok=true indicates insertion was successful,
		// whereas ok=false indicates the key already exists
		// (i.e., this submission is a duplicate).
		dedupKey := fmt.Sprintf("%x", combinedHash)
		ok, err := srv.KVStore.AddKey(ctx, dedupKey)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if ok {
			jobSubmitRequestItems = append(jobSubmitRequestItems, item)
		}
	}
	return jobSubmitRequestItems, nil
}

func (srv *PulsarSubmitServer) CancelJobs(ctx context.Context, req *api.JobCancelRequest) (*api.CancellationResult, error) {
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	if req.Queue == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   req.Queue,
			Message: "queue is empty",
		}
	}
	if req.JobSetId == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetId",
			Value:   req.JobSetId,
			Message: "JobSetId is empty",
		}
	}

	var cancelledIds []string

	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, 1, 1),
	}

	// Empty JobId indicates that all jobs in the job set should be cancelled.
	if req.JobId == "" {
		sequence.Events[0] = &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_CancelJobSet{
				CancelJobSet: &armadaevents.CancelJobSet{},
			},
		}

		cancelledIds = []string{fmt.Sprintf("all jobs in job set %s", req.JobSetId)}
	} else {
		jobId, _, err := parseJobRunIds(req.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events[0] = &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: armadaevents.ProtoUuidFromUlid(jobId)},
			},
		}

		cancelledIds = []string{req.JobId} // indicates no error
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
		Key:        req.JobSetId,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send message")
	}

	return &api.CancellationResult{
		CancelledIds: cancelledIds,
	}, nil
}

func (srv *PulsarSubmitServer) ReprioritizeJobs(ctx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.ReprioritizeAnyJobs, queue.PermissionVerbReprioritize)
	if err != nil {
		return nil, err
	}

	if req.Queue == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   req.Queue,
			Message: "queue is empty",
		}
	}
	if req.JobSetId == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetId",
			Value:   req.JobSetId,
			Message: "JobSetId is empty",
		}
	}

	priority, err := eventutil.LogSubmitPriorityFromApiPriority(req.NewPriority)
	if err != nil {
		return nil, err
	}

	// results maps job ids to strings containing error messages.
	results := make(map[string]string)

	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, len(req.JobIds), len(req.JobIds)),
	}

	// No job ids implicitly indicates that all jobs in the job set should be re-prioritised.
	if len(req.JobIds) == 0 {
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_ReprioritiseJobSet{
				ReprioritiseJobSet: &armadaevents.ReprioritiseJobSet{
					Priority: priority,
				},
			},
		})

		results[fmt.Sprintf("all jobs in job set %s", req.JobSetId)] = ""
	}

	// Otherwise, only the specified jobs should be re-prioritised.
	for i, jobIdString := range req.JobIds {

		jobId, _, err := parseJobRunIds(jobIdString)
		if err != nil {
			results[jobIdString] = err.Error()
			continue
		}

		sequence.Events[i] = &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
				ReprioritiseJob: &armadaevents.ReprioritiseJob{
					JobId:    armadaevents.ProtoUuidFromUlid(jobId),
					Priority: priority,
				},
			},
		}

		results[jobIdString] = "" // empty string indicates no error
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
		Key:        req.JobSetId,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to send message")
	}

	return &api.JobReprioritizeResponse{
		ReprioritizationResults: results,
	}, nil
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
	return srv.SubmitServer.GetQueueInfo(ctx, req)
}

// SubmitApiEvent converts an api.EventMessage into Pulsar state transition messages and publishes those to Pulsar.
func (srv *PulsarSubmitServer) SubmitApiEvent(ctx context.Context, apiEvent *api.EventMessage) error {

	// For now, we submit legacy utilisation messages directly to the log.
	// We include a property in the Pulsar message, which the consumer can use to detect these messages.
	//
	// If it's not a utilisation message, we convert the message to a proper log event message.
	var messageType string
	var jobSetName string
	var payload []byte
	var err error
	if m, ok := (apiEvent.Events).(*api.EventMessage_Utilisation); ok {
		payload, err = proto.Marshal(apiEvent)
		if err != nil {
			err = errors.WithStack(err)
			return err
		}
		messageType = armadaevents.PULSAR_UTILISATION_MESSAGE
		jobSetName = m.Utilisation.JobSetId
	} else {
		sequence, err := PulsarSequenceFromApiEvent(apiEvent)
		if err != nil {
			return err
		}

		// If no events were created, exit here.
		if len(sequence.Events) == 0 {
			return nil
		}

		payload, err = proto.Marshal(sequence)
		if err != nil {
			err = errors.WithStack(err)
			return err
		}

		messageType = armadaevents.PULSAR_CONTROL_MESSAGE
		jobSetName = sequence.JobSetName
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId := requestid.FromContextOrMissing(ctx)

	_, err = srv.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: payload,
		Properties: map[string]string{
			requestid.MetadataKey:                     requestId,
			armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: messageType,
		},
		Key: jobSetName,
	})
	if err != nil {
		err = errors.WithStack(err)
		return err
	}

	return nil
}

// PulsarSequenceFromApiEvent converts an api.EventMessage into the corresponding Pulsar event
// and returns an EventSequence containing this single event.
func PulsarSequenceFromApiEvent(msg *api.EventMessage) (sequence *armadaevents.EventSequence, err error) {
	sequence = &armadaevents.EventSequence{}

	switch m := msg.Events.(type) {
	case *api.EventMessage_Submitted:
		// Do nothing; the Pulsar submitted message is generated by the Pulsar API endpoint.
	case *api.EventMessage_Queued:
		// Do nothing; there's no corresponding Pulsar message.
	case *api.EventMessage_DuplicateFound:
		sequence.Queue = m.DuplicateFound.Queue
		sequence.JobSetName = m.DuplicateFound.JobSetId

		newJobId, _, err := parseJobRunIds(m.DuplicateFound.JobId)
		if err != nil {
			return nil, err
		}
		oldJobId, _, err := parseJobRunIds(m.DuplicateFound.OriginalJobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobDuplicateDetected{
				JobDuplicateDetected: &armadaevents.JobDuplicateDetected{
					NewJobId: armadaevents.ProtoUuidFromUlid(newJobId),
					OldJobId: armadaevents.ProtoUuidFromUlid(oldJobId),
				},
			},
		})
	case *api.EventMessage_Leased:
		sequence.Queue = m.Leased.Queue
		sequence.JobSetName = m.Leased.JobSetId

		jobId, runId, err := parseJobRunIds(m.Leased.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunLeased{
				JobRunLeased: &armadaevents.JobRunLeased{
					RunId:      armadaevents.ProtoUuidFromUlid(runId),
					JobId:      armadaevents.ProtoUuidFromUlid(jobId),
					ExecutorId: m.Leased.ClusterId,
				},
			},
		})
	case *api.EventMessage_LeaseReturned:
		sequence.Queue = m.LeaseReturned.Queue
		sequence.JobSetName = m.LeaseReturned.JobSetId

		jobId, runId, err := parseJobRunIds(m.LeaseReturned.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: armadaevents.ProtoUuidFromUlid(runId),
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
					Errors: []*armadaevents.Error{
						{
							Terminal: true, // EventMessage_LeaseReturned indicates a failed job run.
							Reason: &armadaevents.Error_PodError{
								PodError: &armadaevents.PodError{
									ObjectMeta: &armadaevents.ObjectMeta{
										ExecutorId:   m.LeaseReturned.ClusterId,
										Namespace:    "",
										Name:         "",
										KubernetesId: m.LeaseReturned.KubernetesId,
										Annotations:  nil,
										Labels:       nil,
									},
									PodNumber: m.LeaseReturned.PodNumber,
									Message:   m.LeaseReturned.Reason,
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

		jobId, runId, err := parseJobRunIds(m.LeaseExpired.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: armadaevents.ProtoUuidFromUlid(runId),
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
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

		jobId, runId, err := parseJobRunIds(m.Pending.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunAssigned{
				JobRunAssigned: &armadaevents.JobRunAssigned{
					RunId: armadaevents.ProtoUuidFromUlid(runId),
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
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

		jobId, runId, err := parseJobRunIds(m.Running.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunId: armadaevents.ProtoUuidFromUlid(runId),
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								Namespace:    m.Running.NodeName,
								Name:         m.Running.PodName,
								KubernetesId: m.Running.KubernetesId,
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

		jobId, _, err := parseJobRunIds(m.UnableToSchedule.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
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

		jobId, _, err := parseJobRunIds(m.Failed.JobId)
		if err != nil {
			return nil, err
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
					KubernetesId: "", // missing from api message
				},
			}

			// Legacy messages encode the reason as an enum, whereas Pulsar uses objects.
			switch m.Failed.Cause {
			case api.Cause_DeadlineExceeded:
				containerError.KubernetesReason = &armadaevents.ContainerError_DeadlineExceeded_{}
			case api.Cause_Error:
				containerError.KubernetesReason = &armadaevents.ContainerError_Error{}
			case api.Cause_Evicted:
				containerError.KubernetesReason = &armadaevents.ContainerError_Evicted_{}
			case api.Cause_OOM:
				containerError.KubernetesReason = &armadaevents.ContainerError_DeadlineExceeded_{}
			default:
				return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
					Name:    "Cause",
					Value:   m.Failed.Cause,
					Message: "Unknown cause",
				})
			}

			containerErrors = append(containerErrors, containerError)
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason: &armadaevents.Error_PodError{
								PodError: &armadaevents.PodError{
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
								},
							},
						},
					},
				},
			},
		})
	case *api.EventMessage_Succeeded:
		sequence.Queue = m.Succeeded.Queue
		sequence.JobSetName = m.Succeeded.JobSetId

		jobId, runId, err := parseJobRunIds(m.Succeeded.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId: armadaevents.ProtoUuidFromUlid(runId),
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
				},
			},
		})
	case *api.EventMessage_Reprioritized:
		sequence.Queue = m.Reprioritized.Queue
		sequence.JobSetName = m.Reprioritized.JobSetId

		jobId, _, err := parseJobRunIds(m.Reprioritized.JobId)
		if err != nil {
			return nil, err
		}

		priority, err := eventutil.LogSubmitPriorityFromApiPriority(m.Reprioritized.NewPriority)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
				ReprioritisedJob: &armadaevents.ReprioritisedJob{
					JobId:    armadaevents.ProtoUuidFromUlid(jobId),
					Priority: priority,
				},
			},
		})
	case *api.EventMessage_Cancelling:
		// Do nothing; there's no corresponding Pulsar message.
	case *api.EventMessage_Cancelled:
		sequence.Queue = m.Cancelled.Queue
		sequence.JobSetName = m.Cancelled.JobSetId

		jobId, _, err := parseJobRunIds(m.Cancelled.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
				},
			},
		})
	case *api.EventMessage_Terminated:
		// EventMessage_Terminated is generated when lease renewal fails. One such event is generated per pod.
		// Hence, we translate these to PodError with an empty ContainerErrors.

		sequence.Queue = m.Terminated.Queue
		sequence.JobSetName = m.Terminated.JobSetId

		jobId, _, err := parseJobRunIds(m.Terminated.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason: &armadaevents.Error_PodError{
								PodError: &armadaevents.PodError{
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
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "msg",
			Value:   msg,
			Message: "utilisation messages should be handled by the caller of this function",
		}
		err = errors.WithStack(err)
		return nil, err
	case *api.EventMessage_IngressInfo:
		// Later, ingress info should be bundled with the JobRunRunning message.
		// For now, we create a special message that exists only for compatibility with the legacy messages.

		sequence.Queue = m.IngressInfo.Queue
		sequence.JobSetName = m.IngressInfo.JobSetId

		jobId, runId, err := parseJobRunIds(m.IngressInfo.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_StandaloneIngressInfo{
				StandaloneIngressInfo: &armadaevents.StandaloneIngressInfo{
					RunId: armadaevents.ProtoUuidFromUlid(runId),
					JobId: armadaevents.ProtoUuidFromUlid(jobId),
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

func parseJobId(jobId string) (ulid.ULID, error) {
	id, err := ulid.Parse(jobId)
	if err != nil {
		err = errors.WithStack(err)
	}
	return id, err
}

// parseJobRunIds is a convenience function for converting a string to a ULID and
// then returning the default legacy id for runs without an included id.
// Returns an ErrInvalidArgument if parsing either id fails.
func parseJobRunIds(jobIdString string) (ulid.ULID, ulid.ULID, error) {
	jobId, err := ulid.Parse(jobIdString)
	if err != nil {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "jobId",
			Value:   jobIdString,
			Message: err.Error(),
		}
		err = errors.WithStack(err)
		return jobId, jobId, err
	}

	runId, err := ulid.Parse(LEGACY_RUN_ID)
	if err != nil {
		err = &armadaerrors.ErrInvalidArgument{
			Name:    "runId",
			Value:   LEGACY_RUN_ID,
			Message: err.Error(),
		}
		err = errors.WithStack(err)
		return jobId, runId, err
	}

	return jobId, runId, nil
}
