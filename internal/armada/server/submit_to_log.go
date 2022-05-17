package server

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
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
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/pgkeyvalue"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/G-Research/armada/pkg/client/queue"
)

// Id used for messages for which we can't use the kubernetesId.
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
	// Used for job submission deduplication.
	KVStore *pgkeyvalue.PGKeyValueStore
}

// TODO: Add input validation to make sure messages can be inserted to the database.
// TODO: Check job size and reject jobs that could never be scheduled. Maybe by querying the scheduler for its limits.
func (srv *PulsarSubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {

	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, err
	}

	// Prepare an event sequence to be submitted to the log
	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, 0, len(req.JobRequestItems)),
	}

	// Create legacy API jobs from the requests.
	// We use the legacy code for the conversion to ensure that behavior doesn't change.
	apiJobs, err := srv.SubmitServer.createJobs(req, userId, groups)
	if err != nil {
		return nil, err
	}

	// Armada checks for duplicate job submissions if a ClientId (i.e., a deuplication id) is provided.
	// Deduplication is based on storing the combined hash of the ClientId and queue.
	// For storage efficiency, we store hashes instead of user-provided strings.
	// For computational efficiency, we create a lookup table to avoid computing the same hash twice.
	combinedHashData := make([]byte, 40)
	queueHash := sha1.Sum([]byte(req.Queue))
	for i, b := range queueHash {
		combinedHashData[i] = b
	}
	combinedHashFromClientId := make(map[string][20]byte)
	jobDuplicateFoundEvents := make([]*api.JobDuplicateFoundEvent, 0)

	// Convert the API jobs to log jobs.
	responses := make([]*api.JobSubmitResponseItem, len(req.JobRequestItems), len(req.JobRequestItems))
	for i, apiJob := range apiJobs {
		responses[i] = &api.JobSubmitResponseItem{
			JobId: apiJob.GetId(),
		}

		// If a ClientId (i.e., a deuplication id) is provided,
		// check for previous job submissions with the ClientId for this queue.
		// If we find a duplicate, insert the previous jobId in the corresponding response
		// and generate a job duplicate found event.
		if apiJob.ClientId != "" && srv.KVStore != nil {

			// Hash the ClientId together with the queue (or get it from the table, if possible).
			combinedHash, ok := combinedHashFromClientId[apiJob.ClientId]
			if !ok {
				clientIdHash := sha1.Sum([]byte(apiJob.ClientId))

				// Compute the combined hash.
				for i, b := range clientIdHash {
					combinedHashData[i+20] = b
				}
				combinedHash = sha1.Sum(combinedHashData)
				combinedHashFromClientId[apiJob.ClientId] = combinedHash
			}

			// Check if we've seen the hash before.
			// ok=true indicates insertion was successful,
			// whereas ok=false indicates the key already exists
			// (i.e., this submission is a duplicate).
			dedupKey := fmt.Sprintf("%x", combinedHash)
			ok, err := srv.KVStore.Add(ctx, dedupKey, []byte(apiJob.GetId()))
			if err != nil {
				return nil, errors.WithStack(err)
			}
			if !ok { // duplicate found
				originalJobId, err := srv.KVStore.Get(ctx, dedupKey)
				if err != nil {
					return nil, errors.WithStack(err)
				}

				jobDuplicateFoundEvents = append(jobDuplicateFoundEvents, &api.JobDuplicateFoundEvent{
					JobId:         responses[i].JobId,
					Queue:         req.Queue,
					JobSetId:      req.JobSetId,
					Created:       time.Now(),
					OriginalJobId: string(originalJobId),
				})
				responses[i].JobId = string(originalJobId)

				// The job shouldn't be submitted twice. Move on to the next job.
				continue
			}
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
		//
		// We use an empty ingress config here.
		// The executor applies executor-specific information later.
		// We only need this here because we're re-using code that was previously called by the executor.
		err = eventutil.PopulateK8sServicesIngresses(apiJob, &configuration.IngressConfiguration{})
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

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: logJob,
			},
		})
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

	err = reportDuplicateFoundEvents(srv.SubmitServer.eventStore, jobDuplicateFoundEvents)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error reporting duplicates: %s", err)
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	if len(sequence.Events) > 0 {
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
	}

	return &api.JobSubmitResponse{JobResponseItems: responses}, nil
}

func (srv *PulsarSubmitServer) CancelJobs(ctx context.Context, req *api.JobCancelRequest) (*api.CancellationResult, error) {

	// If either queue or jobSetId is missing, we need to get those from Redis.
	// This must be done before checking auth, since the auth check expects a queue.
	// If both queue and jobSetId are provided, we assume that those are correct
	// to make it possible to cancel jobs that have been submitted but not written to Redis yet.
	if req.JobId != "" && (req.Queue == "" || req.JobSetId == "") {
		jobs, err := srv.SubmitServer.jobRepository.GetJobsByIds([]string{req.JobId})
		if err != nil {
			return nil, err
		}
		if len(jobs) == 0 {
			return nil, &armadaerrors.ErrNotFound{
				Type:  "job",
				Value: req.JobId,
			}
		}
		if len(jobs) != 1 { // Internal error; should never happen.
			return nil, fmt.Errorf("expected 1 job result, but got %v", jobs)
		}
		queue := jobs[0].Job.GetQueue()
		jobSetId := jobs[0].Job.GetJobSetId()

		// We allow clients to submit requests only containing a job id.
		// For these requests, we need to populate the queue and jobSetId fields.
		if req.Queue == "" {
			req.Queue = queue
		}
		if req.JobSetId == "" {
			req.JobSetId = jobSetId
		}

		// If both a job id and queue or jobsetId is provided, return ErrNotFound if they don't match,
		// since the job could not be found for the provided queue/jobSetId.
		if req.Queue != queue {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   req.JobId,
				Message: fmt.Sprintf("job not found in queue %s", req.Queue),
			}
		}
		if req.JobSetId != jobSetId {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   req.JobId,
				Message: fmt.Sprintf("job not found in job set %s", req.JobSetId),
			}
		}
	}

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
		jobId, err := armadaevents.ProtoUuidFromUlidString(req.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events[0] = &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: jobId},
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

		jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdString)
		if err != nil {
			results[jobIdString] = err.Error()
			continue
		}

		sequence.Events[i] = &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_ReprioritiseJob{
				ReprioritiseJob: &armadaevents.ReprioritiseJob{
					JobId:    jobId,
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
	sequence, err := PulsarSequenceFromApiEvent(apiEvent)
	if err != nil {
		return err
	}

	// If no events were created, exit here.
	if len(sequence.Events) == 0 {
		return nil
	}

	payload, err := proto.Marshal(sequence)
	if err != nil {
		return errors.WithStack(err)
	}

	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId := requestid.FromContextOrMissing(ctx)

	_, err = srv.Producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: payload,
		Properties: map[string]string{
			requestid.MetadataKey:                     requestId,
			armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
		},
		Key: sequence.JobSetName,
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

		newJobId, err := armadaevents.ProtoUuidFromUlidString(m.DuplicateFound.JobId)
		if err != nil {
			return nil, err
		}
		oldJobId, err := armadaevents.ProtoUuidFromUlidString(m.DuplicateFound.OriginalJobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
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
			Event: &armadaevents.EventSequence_Event_JobRunLeased{
				JobRunLeased: &armadaevents.JobRunLeased{
					RunId:      legacyJobRunId(),
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
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: jobId,
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

		// Message has no KubernetesId; use the all-zeros id.
		jobId, err := armadaevents.ProtoUuidFromUlidString(m.LeaseExpired.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: legacyJobRunId(),
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
			Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunId: runId,
					JobId: jobId,
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

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.UnableToSchedule.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.UnableToSchedule.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
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
					KubernetesId: "", // only the id of the pod is stored in the failed message
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
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: jobId,
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

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Succeeded.JobId)
		if err != nil {
			return nil, err
		}

		runId, err := armadaevents.ProtoUuidFromUuidString(m.Succeeded.KubernetesId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId: runId,
					JobId: jobId,
				},
			},
		})
	case *api.EventMessage_Reprioritized:
		sequence.Queue = m.Reprioritized.Queue
		sequence.JobSetName = m.Reprioritized.JobSetId

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Reprioritized.JobId)
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

		jobId, err := armadaevents.ProtoUuidFromUlidString(m.Cancelled.JobId)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{
					JobId: jobId,
				},
			},
		})
	case *api.EventMessage_Terminated:
		// EventMessage_Terminated is generated when lease renewal fails. One such event is generated per pod.
		// Hence, we translate these to PodError with an empty ContainerErrors.

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
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: jobId,
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

func legacyJobRunId() *armadaevents.Uuid {
	jobRunId, err := armadaevents.ProtoUuidFromUlidString(LEGACY_RUN_ID)
	if err != nil {
		panic(err)
	}
	return jobRunId
}
