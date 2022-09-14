package server

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/validation"
	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/requestid"
	commonvalidation "github.com/G-Research/armada/internal/common/validation"
	"github.com/G-Research/armada/internal/executor/configuration"
	"github.com/G-Research/armada/internal/pgkeyvalue"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/G-Research/armada/pkg/client/queue"
)

// PulsarSubmitServer is a service that accepts API calls according to the original Armada submit API
// and publishes messages to Pulsar based on those calls.
// TODO: Consider returning a list of message ids of the messages generated
// TODO: Include job set as the message key for each message
type PulsarSubmitServer struct {
	api.UnimplementedSubmitServer
	Producer        pulsar.Producer
	Permissions     authorization.PermissionChecker
	QueueRepository repository.QueueRepository
	// Maximum size of Pulsar messages
	MaxAllowedMessageSize uint
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
	// We use the legacy code for the conversion to ensure that behaviour doesn't change.
	apiJobs, err := srv.SubmitServer.createJobs(req, userId, groups)
	if err != nil {
		return nil, err
	}

	// Convert the API jobs to log jobs.
	responses := make([]*api.JobSubmitResponseItem, len(req.JobRequestItems), len(req.JobRequestItems))

	originalIds, err := srv.getOriginalJobIds(ctx, apiJobs)
	if err != nil {
		return nil, err
	}
	jobDuplicateFoundEvents := make([]*api.JobDuplicateFoundEvent, 0)
	for i, apiJob := range apiJobs {
		responses[i] = &api.JobSubmitResponseItem{
			JobId: apiJob.GetId(),
		}

		// If a ClientId (i.e., a deduplication id) is provided,
		// check for previous job submissions with the ClientId for this queue.
		// If we find a duplicate, insert the previous jobId in the corresponding response
		// and generate a job duplicate found event.
		originalId, found := originalIds[apiJob.GetId()]
		if apiJob.ClientId != "" && originalId != apiJob.GetId() {
			if found {
				jobDuplicateFoundEvents = append(jobDuplicateFoundEvents, &api.JobDuplicateFoundEvent{
					JobId:         responses[i].JobId,
					Queue:         req.Queue,
					JobSetId:      req.JobSetId,
					Created:       time.Now(),
					OriginalJobId: originalIds[apiJob.GetId()],
				})
				responses[i].JobId = originalIds[apiJob.GetId()]
				// The job shouldn't be submitted twice. Move on to the next job.
				continue
			} else {
				log.Warnf(
					"ClientId %s was supplied for job %s but no original jobId could be found.  Deduplication will not be applied",
					apiJob.ClientId,
					apiJob.GetId())
			}
		}

		if err := commonvalidation.ValidateApiJob(apiJob, srv.SubmitServer.schedulingConfig.Preemption); err != nil {
			return nil, err
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

	// Check if the job can be scheduled on any executor,
	// to avoid having users wait for a job that may never be scheduled
	allClusterSchedulingInfo, err := srv.SubmitServer.schedulingInfoRepository.GetClusterSchedulingInfo()
	if err != nil {
		err = errors.WithMessage(err, "error getting scheduling info")
		return nil, err
	}
	if ok, err := validateJobsCanBeScheduled(apiJobs, allClusterSchedulingInfo); !ok {
		if err != nil {
			return nil, errors.WithMessagef(err, "can't schedule job for user %s", userId)
		} else {
			return nil, errors.Errorf("can't schedule job for user %s", userId)
		}
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

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence})

	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
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
				Message: fmt.Sprintf("job not found in queue %s, try waiting or setting queue/jobSetId explicitly", req.Queue),
			}
		}
		if req.JobSetId != jobSetId {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   req.JobId,
				Message: fmt.Sprintf("job not found in job set %s, try waiting or setting queue/jobSetId explicitly", req.JobSetId),
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

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence})

	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &api.CancellationResult{
		CancelledIds: cancelledIds,
	}, nil
}

func (srv *PulsarSubmitServer) CancelJobSet(ctx context.Context, req *api.JobSetCancelRequest) (*types.Empty, error) {
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

	err := validation.ValidateJobSetFilter(req.Filter)
	if err != nil {
		return nil, err
	}

	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	ids, err := srv.SubmitServer.jobRepository.GetJobSetJobIds(req.Queue, req.JobSetId, createJobSetFilter(req.Filter))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting job IDs: %s", err)
	}

	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, 0, len(ids)),
	}

	for _, id := range ids {
		jobId, err := armadaevents.ProtoUuidFromUlidString(id)
		if err != nil {
			return nil, err
		}

		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: jobId},
			},
		})
	}

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence})

	if err != nil {
		log.WithError(err).Error("failed to send cancel job messages to pulsar")
		return nil, status.Error(codes.Internal, "failed to send cancel job messages to pulsar")
	}

	return &types.Empty{}, err
}

func (srv *PulsarSubmitServer) ReprioritizeJobs(ctx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	// If either queue or jobSetId is missing, we get the job set and queue associated
	// with the first job id in the request.
	//
	// This must be done before checking auth, since the auth check expects a queue.
	// If both queue and jobSetId are provided, we assume that those are correct
	// to make it possible to cancel jobs that have been submitted but not written to Redis yet.
	if len(req.JobIds) > 0 && (req.Queue == "" || req.JobSetId == "") {
		firstJobId := req.JobIds[0]

		jobs, err := srv.SubmitServer.jobRepository.GetJobsByIds([]string{firstJobId})
		if err != nil {
			return nil, err
		}
		if len(jobs) == 0 {
			return nil, &armadaerrors.ErrNotFound{
				Type:  "job",
				Value: firstJobId,
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
				Value:   firstJobId,
				Message: fmt.Sprintf("job not found in queue %s, try waiting or setting queue/jobSetId explicitly", req.Queue),
			}
		}
		if req.JobSetId != jobSetId {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   firstJobId,
				Message: fmt.Sprintf("job not found in job set %s, try waiting or setting queue/jobSetId explicitly", req.JobSetId),
			}
		}
	}

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

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence})

	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &api.JobReprioritizeResponse{
		ReprioritizationResults: results,
	}, nil
}

// Authorize authorises a user request to submit a state transition message to the log.
// User information used for authorization is extracted from the provided context.
// Checks that the user has either anyPerm (e.g., permissions.SubmitAnyJobs) or perm (e.g., PermissionVerbSubmit) for this queue.
// Returns the userId and groups extracted from the context.
func (srv *PulsarSubmitServer) Authorize(
	ctx context.Context,
	queueName string,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) (userId string, groups []string, err error) {
	principal := authorization.GetPrincipal(ctx)
	userId = principal.GetName()
	groups = principal.GetGroupNames()
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

func (srv *PulsarSubmitServer) CreateQueues(ctx context.Context, req *api.QueueList) (*api.BatchQueueCreateResponse, error) {
	return srv.SubmitServer.CreateQueues(ctx, req)
}

func (srv *PulsarSubmitServer) UpdateQueue(ctx context.Context, req *api.Queue) (*types.Empty, error) {
	return srv.SubmitServer.UpdateQueue(ctx, req)
}

func (srv *PulsarSubmitServer) UpdateQueues(ctx context.Context, req *api.QueueList) (*api.BatchQueueUpdateResponse, error) {
	return srv.SubmitServer.UpdateQueues(ctx, req)
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

// SubmitApiEvents converts several api.EventMessage into Pulsar state transition messages and publishes those to Pulsar.
func (srv *PulsarSubmitServer) SubmitApiEvents(ctx context.Context, apiEvents []*api.EventMessage) error {
	// Because (queue, userId, jobSetId) may differ between events,
	// several sequences may be necessary.
	sequences, err := eventutil.EventSequencesFromApiEvents(apiEvents)
	if err != nil {
		return err
	}
	if len(sequences) == 0 {
		return nil
	}

	return srv.publishToPulsar(ctx, sequences)
}

// SubmitApiEvent converts an api.EventMessage into Pulsar state transition messages and publishes those to Pulsar.
func (srv *PulsarSubmitServer) SubmitApiEvent(ctx context.Context, apiEvent *api.EventMessage) error {
	sequence, err := eventutil.EventSequenceFromApiEvent(apiEvent)
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

// PublishToPulsar sends pulsar messages async
func (srv *PulsarSubmitServer) publishToPulsar(ctx context.Context, sequences []*armadaevents.EventSequence) error {
	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId := requestid.FromContextOrMissing(ctx)

	// Reduce the number of sequences to send to the minimum possible,
	// and then break up any sequences larger than srv.MaxAllowedMessageSize.
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, int(srv.MaxAllowedMessageSize), true)
	if err != nil {
		return err
	}

	// Send each sequence async. Collect any errors via ch.
	ch := make(chan error, len(sequences))
	defer close(ch)
	for _, sequence := range sequences {
		payload, err := proto.Marshal(sequence)
		if err != nil {
			return errors.WithStack(err)
		}

		srv.Producer.SendAsync(
			ctx,
			&pulsar.ProducerMessage{
				Payload: payload,
				Properties: map[string]string{
					requestid.MetadataKey:                     requestId,
					armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
				},
				Key: sequence.JobSetName,
			},
			// Callback on send.
			func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				ch <- err
			},
		)
	}

	// Flush queued messages and wait until persisted.
	err = srv.Producer.Flush()
	if err != nil {
		return errors.WithStack(err)
	}

	// Collect any errors experienced by the async send and return.
	var result *multierror.Error
	for range sequences {
		result = multierror.Append(result, <-ch)
	}
	return result.ErrorOrNil()
}

// getOriginalJobIds returns the mapping between jobId and originalJobId.  If the job (or more specifically the clientId
// on the job) has not been seen before then jobId -> jobId.  If the job has been seen before then jobId -> originalJobId
// Note that if srv.KVStore is nil then this function simply returns jobId -> jobId
func (srv *PulsarSubmitServer) getOriginalJobIds(ctx context.Context, apiJobs []*api.Job) (map[string]string, error) {
	// If we don't have a KV store, then just return original mappings
	if srv.KVStore == nil {
		ret := make(map[string]string, len(apiJobs))
		for _, apiJob := range apiJobs {
			ret[apiJob.GetId()] = apiJob.GetId()
		}
		return ret, nil
	}

	hash := func(queue string, clientId string) [20]byte {
		combined := fmt.Sprintf("%s:%s", queue, clientId)
		return sha1.Sum([]byte(combined))
	}

	// Armada checks for duplicate job submissions if a ClientId (i.e., a deduplication id) is provided.
	// Deduplication is based on storing the combined hash of the ClientId and queue.
	// For storage efficiency, we store hashes instead of user-provided strings.
	kvs := make([]*pgkeyvalue.KeyValue, 0, len(apiJobs))
	for _, apiJob := range apiJobs {
		if apiJob.ClientId != "" {
			clientIdHash := hash(apiJob.Queue, apiJob.ClientId)
			kvs = append(kvs, &pgkeyvalue.KeyValue{
				Key:   fmt.Sprintf("%x", clientIdHash),
				Value: []byte(apiJob.GetId()),
			})
		}
	}

	// If we have any client Ids add them to store
	if len(kvs) > 0 {
		addedKvs, err := srv.KVStore.LoadOrStoreBatch(ctx, kvs)
		if err != nil {
			return nil, err
		}
		ret := make(map[string]string, len(addedKvs))
		for _, apiJob := range apiJobs {
			if apiJob.ClientId != "" {
				clientIdHash := hash(apiJob.Queue, apiJob.ClientId)
				originalJobId := addedKvs[fmt.Sprintf("%x", clientIdHash)]
				ret[apiJob.GetId()] = string(originalJobId)
			}
		}
		return ret, nil
	}

	return nil, nil
}
