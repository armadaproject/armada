package server

import (
	"context"
	"crypto/sha1"
	"fmt"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/internal/scheduler"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/validation"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	commonvalidation "github.com/armadaproject/armada/internal/common/validation"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
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
	// Used to check at job submit time if the job could ever be scheduled on either legacy or pulsar schedulers
	PulsarSchedulerSubmitChecker *scheduler.SubmitChecker
	LegacySchedulerSubmitChecker *scheduler.SubmitChecker
	// Flag to control if we enable sending messages to the pulsar scheduler
	PulsarSchedulerEnabled bool
	// Probability of using the pulsar scheduler.  Has no effect if PulsarSchedulerEnabled is false
	ProbabilityOdfUsingPulsarScheduler float64
	Rand                               *rand.Rand
	GangIdAnnotation                   string
}

func (srv *PulsarSubmitServer) SubmitJobs(ctx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, err
	}

	// Prepare an event sequence to be submitted to the log
	pulsarSchedulerEvents := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, 0, len(req.JobRequestItems)),
	}

	legacySchedulerEvents := &armadaevents.EventSequence{
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
	if err := commonvalidation.ValidateApiJobs(apiJobs, *srv.SubmitServer.schedulingConfig); err != nil {
		return nil, err
	}

	schedulersByJobId, err := srv.assignScheduler(apiJobs)
	if err != nil {
		return nil, err
	}

	// Convert the API jobs to log jobs.
	responses := make([]*api.JobSubmitResponseItem, len(req.JobRequestItems), len(req.JobRequestItems))

	originalIds, err := srv.getOriginalJobIds(ctx, apiJobs)
	if err != nil {
		return nil, err
	}

	for i, apiJob := range apiJobs {
		eventTime := time.Now()
		assignedScheduler, ok := schedulersByJobId[apiJob.Scheduler]
		if !ok {
			// This should never happen as if we can't find a scheduler we would have errored earlier
			return nil, errors.Errorf("Didn't allocate a scheduler for job %s", apiJob.Id)
		}

		es := legacySchedulerEvents
		if assignedScheduler == schedulers.Pulsar {
			es = pulsarSchedulerEvents
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

		responses[i] = &api.JobSubmitResponseItem{
			JobId: apiJob.GetId(),
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

		es.Events = append(es.Events, &armadaevents.EventSequence_Event{
			Created: &eventTime,
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: logJob,
			},
		})

		// If a ClientId (i.e., a deduplication id) is provided,
		// check for previous job submissions with the ClientId for this queue.
		// If we find a duplicate, insert the previous jobId in the corresponding response
		// and generate a job duplicate found event.
		originalId, found := originalIds[apiJob.GetId()]
		if apiJob.ClientId != "" && originalId != apiJob.GetId() {
			if found && originalId != "" {
				oldJobId, err := armadaevents.ProtoUuidFromUlidString(originalIds[apiJob.GetId()])
				if err != nil {
					return nil, status.Error(codes.Internal, "error marshalling oldJobId")
				}
				es.Events = append(es.Events, &armadaevents.EventSequence_Event{
					Created: &eventTime,
					Event: &armadaevents.EventSequence_Event_JobDuplicateDetected{
						JobDuplicateDetected: &armadaevents.JobDuplicateDetected{
							NewJobId: logJob.JobId,
							OldJobId: oldJobId,
						},
					},
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
	}

	if len(pulsarSchedulerEvents.Events) > 0 {
		err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{pulsarSchedulerEvents}, schedulers.Pulsar)
		if err != nil {
			log.WithError(err).Error("failed send pulsar scheduler events to Pulsar")
			return nil, status.Error(codes.Internal, "Failed to send message")
		}
	}

	if len(legacySchedulerEvents.Events) > 0 {
		err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{legacySchedulerEvents}, schedulers.Legacy)
		if err != nil {
			log.WithError(err).Error("failed send legacy scheduler events to Pulsar")
			return nil, status.Error(codes.Internal, "Failed to send message")
		}
	}

	return &api.JobSubmitResponse{JobResponseItems: responses}, nil
}

func (srv *PulsarSubmitServer) CancelJobs(ctx context.Context, req *api.JobCancelRequest) (*api.CancellationResult, error) {
	// separate code path for multiple jobs
	if len(req.JobIds) > 0 {
		return srv.cancelJobsByIdsQueueJobset(ctx, req.JobIds, req.Queue, req.JobSetId)
	}

	// Another separate code path for cancelling an entire job set
	// TODO: We should deprecate this and move people over to CancelJobSet()
	if req.JobId == "" {
		log.Warnf("CancelJobs called for queue=%s and jobset=%s but with empty job id. Redirecting to CancelJobSet()", req.Queue, req.JobSetId)
		_, err := srv.CancelJobSet(ctx, &api.JobSetCancelRequest{
			Queue:    req.Queue,
			JobSetId: req.JobSetId,
		})
		if err != nil {
			return nil, err
		}
		return &api.CancellationResult{
			CancelledIds: []string{req.JobId}, // we return an empty string here which seems a bit nonsensical- but that's what the old code did!
		}, nil
	}

	// resolve the queue and jobset of the job: we can't trust what the user has given us
	resolvedQueue, resolvedJobset, err := srv.resolveQueueAndJobsetForJob(req.JobId)
	if err != nil {
		return nil, err
	}

	// If both a job id and queue or jobsetId is provided, return ErrNotFound if they don't match,
	// since the job could not be found for the provided queue/jobSetId.
	if req.Queue != "" && req.Queue != resolvedQueue {
		return nil, &armadaerrors.ErrNotFound{
			Type:    "job",
			Value:   req.JobId,
			Message: fmt.Sprintf("job not found in queue %s, try waiting", req.Queue),
		}
	}
	if req.JobSetId != "" && req.JobSetId != resolvedJobset {
		return nil, &armadaerrors.ErrNotFound{
			Type:    "job",
			Value:   req.JobId,
			Message: fmt.Sprintf("job not found in job set %s, try waiting", req.JobSetId),
		}
	}

	userId, groups, err := srv.Authorize(ctx, resolvedQueue, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}

	sequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, 1, 1),
	}

	jobId, err := armadaevents.ProtoUuidFromUlidString(req.JobId)
	if err != nil {
		return nil, err
	}

	sequence.Events[0] = &armadaevents.EventSequence_Event{
		Event: &armadaevents.EventSequence_Event_CancelJob{
			CancelJob: &armadaevents.CancelJob{JobId: jobId},
		},
	}

	// we can send the message to cancel to both schedulers. If the scheduler it doesn't belong to it'll be a no-op
	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence}, schedulers.All)

	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}

	return &api.CancellationResult{
		CancelledIds: []string{req.JobId}, // indicates no error
	}, nil
}

// Assumes all Job IDs are in the queue and job set provided
func (srv *PulsarSubmitServer) cancelJobsByIdsQueueJobset(ctx context.Context, jobIds []string, q, jobSet string) (*api.CancellationResult, error) {
	if q == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   "",
			Message: "Queue cannot be empty when cancelling multiple jobs",
		}
	}
	if jobSet == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Jobset",
			Value:   "",
			Message: "Jobset cannot be empty when cancelling multiple jobs",
		}
	}
	userId, groups, err := srv.Authorize(ctx, q, permissions.CancelAnyJobs, queue.PermissionVerbCancel)
	if err != nil {
		return nil, err
	}
	var cancelledIds []string
	sequence, cancelledIds := eventSequenceForJobIds(jobIds, q, jobSet, userId, groups)
	// send the message to both schedulers because jobs may be on either
	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence}, schedulers.All)
	if err != nil {
		log.WithError(err).Error("failed send to Pulsar")
		return nil, status.Error(codes.Internal, "Failed to send message")
	}
	return &api.CancellationResult{
		CancelledIds: cancelledIds,
	}, nil
}

// Returns event sequence along with all valid job ids in the sequence
func eventSequenceForJobIds(jobIds []string, q, jobSet, userId string, groups []string) (*armadaevents.EventSequence, []string) {
	sequence := &armadaevents.EventSequence{
		Queue:      q,
		JobSetName: jobSet,
		UserId:     userId,
		Groups:     groups,
		Events:     []*armadaevents.EventSequence_Event{},
	}
	var validIds []string
	for _, jobIdStr := range jobIds {
		jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdStr)
		if err != nil {
			log.WithError(err).Errorf("could not convert job id to uuid: %s", jobIdStr)
			continue
		}
		validIds = append(validIds, jobIdStr)
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: jobId},
			},
		})
	}
	return sequence, validIds
}

func (srv *PulsarSubmitServer) CancelJobSet(ctx context.Context, req *api.JobSetCancelRequest) (*types.Empty, error) {
	if req.Queue == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   req.Queue,
			Message: "queue cannot be empty when cancelling a jobset",
		}
	}
	if req.JobSetId == "" {
		return nil, &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetId",
			Value:   req.JobSetId,
			Message: "jobsetId cannot be empty when cancelling a jobset",
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

	// We don't know if the jobs are allocated to the legacy scheduler or the new scheduler.  We therefore send messages to both
	ids, err := srv.SubmitServer.jobRepository.GetJobSetJobIds(req.Queue, req.JobSetId, createJobSetFilter(req.Filter))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting job IDs: %s", err)
	}

	legacySchedulerSequence := &armadaevents.EventSequence{
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

		legacySchedulerSequence.Events = append(legacySchedulerSequence.Events, &armadaevents.EventSequence_Event{
			Created: pointer.Now(),
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: jobId},
			},
		})
	}

	pulsarSchedulerSequence := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events: []*armadaevents.EventSequence_Event{
			{
				Created: pointer.Now(),
				Event: &armadaevents.EventSequence_Event_CancelJobSet{
					CancelJobSet: &armadaevents.CancelJobSet{
						// TODO: fill in states
					},
				},
			},
		},
	}

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{legacySchedulerSequence}, schedulers.Legacy)
	if err != nil {
		log.WithError(err).Error("failed to send cancel job messages to pulsar")
		return nil, status.Error(codes.Internal, "failed to send cancel job messages to pulsar")
	}

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{pulsarSchedulerSequence}, schedulers.Pulsar)
	if err != nil {
		log.WithError(err).Error("failed to send cancel jobset message to pulsar")
		return nil, status.Error(codes.Internal, "failed to send cancel jobset message to pulsar")
	}

	return &types.Empty{}, err
}

func (srv *PulsarSubmitServer) ReprioritizeJobs(ctx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	// If either queue or jobSetId is missing, we get the job set and queue associated
	// with the first job id in the request.
	//
	// This must be done before checking auth, since the auth check expects a queue.
	if len(req.JobIds) > 0 && (req.Queue == "" || req.JobSetId == "") {
		firstJobId := req.JobIds[0]

		resolvedQueue, resolvedJobset, err := srv.resolveQueueAndJobsetForJob(firstJobId)
		if err != nil {
			return nil, err
		}

		// If both a job id and queue or jobsetId is provided, return ErrNotFound if they don't match,
		// since the job could not be found for the provided queue/jobSetId.
		// If both a job id and queue or jobsetId is provided, return ErrNotFound if they don't match,
		// since the job could not be found for the provided queue/jobSetId.
		if req.Queue != "" && req.Queue != resolvedQueue {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   firstJobId,
				Message: fmt.Sprintf("job not found in queue %s, try waiting", req.Queue),
			}
		}
		if req.JobSetId != "" && req.JobSetId != resolvedJobset {
			return nil, &armadaerrors.ErrNotFound{
				Type:    "job",
				Value:   firstJobId,
				Message: fmt.Sprintf("job not found in job set %s, try waiting", req.JobSetId),
			}
		}
		req.Queue = resolvedQueue
		req.JobSetId = resolvedJobset
	}

	// TODO: this is incorrect we only validate the permissions on the first job but the other jobs may belong to different queues
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
		JobSetName: req.JobSetId, // TODO: this is incorrect- the jobs may be for different jobsets
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

	// can send the message to both schedulers
	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence}, schedulers.All)

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
			err = &armadaerrors.ErrUnauthorized{
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

// PublishToPulsar sends pulsar messages async
func (srv *PulsarSubmitServer) publishToPulsar(ctx context.Context, sequences []*armadaevents.EventSequence, scheduler schedulers.Scheduler) error {
	// Reduce the number of sequences to send to the minimum possible,
	// and then break up any sequences larger than srv.MaxAllowedMessageSize.
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, srv.MaxAllowedMessageSize, true)
	if err != nil {
		return err
	}
	return pulsarutils.PublishSequences(ctx, srv.Producer, sequences, scheduler)
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

func (srv *PulsarSubmitServer) assignScheduler(jobs []*api.Job) (map[string]schedulers.Scheduler, error) {
	// when assigning jobs to a scheduler, all the jobs in a gang have to go on the same scheduler
	groups := groupJobsByAnnotation(srv.GangIdAnnotation, jobs)
	assignedSchedulers := make(map[string]schedulers.Scheduler, len(jobs))
	for _, group := range groups {
		schedulableOnLegacyScheduler, legacyMsg := srv.LegacySchedulerSubmitChecker.CheckApiJobs(group)
		schedulableOnPulsarScheduler := false
		pulsarMsg := ""
		if srv.PulsarSchedulerEnabled {
			schedulableOnPulsarScheduler, pulsarMsg = srv.PulsarSchedulerSubmitChecker.CheckApiJobs(group)
		}

		// Not schedulable anywhere!
		if !schedulableOnLegacyScheduler && !schedulableOnPulsarScheduler {
			msg := fmt.Sprintf("Could not schedule on legacy scheduler because: %s", legacyMsg)
			if srv.PulsarSchedulerEnabled {
				msg = fmt.Sprintf("%s\nCould not schedule on pulsar scheduler because: %s", msg, pulsarMsg)
			}
			return nil, errors.New(msg)
		}

		r := srv.Rand.Float64()
		var assignedScheduler schedulers.Scheduler
		if jobs[0].Scheduler == "pulsar" { // explicitly to pulsar.  I'm only checking the first job here, but as this is a debug option should be fine
			assignedScheduler = schedulers.Pulsar
		} else if jobs[0].Scheduler == "legacy" { // explicitly to legacy.  Again only check first job
			assignedScheduler = schedulers.Legacy
		} else if schedulableOnPulsarScheduler && !schedulableOnLegacyScheduler { // only schedulable on pulsar
			assignedScheduler = schedulers.Pulsar
		} else if schedulableOnLegacyScheduler && !schedulableOnPulsarScheduler { // only schedulable on legacy
			assignedScheduler = schedulers.Legacy
		} else if r < srv.ProbabilityOdfUsingPulsarScheduler { // probabilistic routing to pulsar
			assignedScheduler = schedulers.Pulsar
		} else { // probabilistic routing to legacy
			assignedScheduler = schedulers.Legacy
		}
		for _, job := range group {
			assignedSchedulers[job.Id] = assignedScheduler
		}
	}
	return assignedSchedulers, nil
}

func groupJobsByAnnotation(annotation string, jobs []*api.Job) [][]*api.Job {
	rv := make(map[string][]*api.Job)
	for _, job := range jobs {
		groupId := uuid.NewString()
		if len(job.Annotations) == 0 {
			rv[groupId] = append(rv[groupId], job)
		} else {
			value := job.Annotations[annotation]
			if value == "" {
				rv[groupId] = append(rv[groupId], job)
			}
			rv[value] = append(rv[value], job)
		}
	}
	return maps.Values(rv)
}

func (srv *PulsarSubmitServer) resolveQueueAndJobsetForJob(jobId string) (string, string, error) {
	// Check the legacy scheduler first
	jobs, err := srv.SubmitServer.jobRepository.GetJobsByIds([]string{jobId})
	if err != nil {
		return "", "", err
	}
	if len(jobs) >= 1 {
		return jobs[0].Job.GetQueue(), jobs[0].Job.GetJobSetId(), nil
	}

	// now check the pulsar scheduler
	if srv.PulsarSchedulerEnabled {
		jobDetails, err := srv.SubmitServer.jobRepository.GetPulsarSchedulerJobDetails(jobId)
		if err != nil {
			return "", "", err
		}
		if jobDetails != nil {
			return jobDetails.Queue, jobDetails.JobSet, nil
		}
	}

	return "", "", &armadaerrors.ErrNotFound{
		Type:  "job",
		Value: jobId,
	}
}
