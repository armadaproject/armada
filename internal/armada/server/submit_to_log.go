package server

import (
	"context"
	"crypto/sha1"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/validation"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pointer"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/internal/common/util"
	commonvalidation "github.com/armadaproject/armada/internal/common/validation"
	executorConfig "github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// PulsarSubmitServer is a service that accepts API calls according to the original Armada submit API
// and publishes messages to Pulsar based on those calls.
// TODO: Consider returning a list of message ids of the messages generated
type PulsarSubmitServer struct {
	Producer         pulsar.Producer
	QueueRepository  repository.QueueRepository
	JobRepository    repository.JobRepository
	SubmissionConfig configuration.SubmissionConfig
	// Maximum size of Pulsar messages
	MaxAllowedMessageSize uint
	// Used for job submission deduplication.
	KVStore *pgkeyvalue.PGKeyValueStore
	// Used to check at job submit time if the job could ever be scheduled on either legacy or pulsar schedulers
	SubmitChecker *scheduler.SubmitChecker
	// Gang id annotation. Needed because we cannot split a gang across schedulers.
	GangIdAnnotation string
	Authorizer       ActionAuthorizer
	CompressorPool   *pool.ObjectPool
}

func (srv *PulsarSubmitServer) SubmitJobs(grpcCtx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, err
	}

	// Prepare an event sequence to be submitted to the log
	es := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     make([]*armadaevents.EventSequence_Event, 0, len(req.JobRequestItems)),
	}

	// Create legacy API jobs from the requests.
	// We use the legacy code for the conversion to ensure that behaviour doesn't change.
	apiJobs, responseItems, err := srv.createJobs(req, userId, groups)
	if err != nil {
		details := &api.JobSubmitResponse{
			JobResponseItems: responseItems,
		}

		st, e := status.Newf(codes.InvalidArgument, "[SubmitJobs] Failed to parse job request: %s", err.Error()).WithDetails(details)
		if e != nil {
			return nil, status.Newf(codes.Internal, "[SubmitJobs] Failed to parse job request: %s", e.Error()).Err()
		}

		return nil, st.Err()
	}
	if responseItems, err := commonvalidation.ValidateApiJobs(apiJobs, srv.SubmissionConfig); err != nil {
		details := &api.JobSubmitResponse{
			JobResponseItems: responseItems,
		}

		st, e := status.Newf(codes.InvalidArgument, "[SubmitJobs] Failed to parse job request: %s", err.Error()).WithDetails(details)
		if e != nil {
			return nil, status.Newf(codes.Internal, "[SubmitJobs] Failed to parse job request: %s", e.Error()).Err()
		}
		return nil, st.Err()
	}

	jobsSubmitted := make([]*api.Job, 0, len(req.JobRequestItems))
	responses := make([]*api.JobSubmitResponseItem, len(req.JobRequestItems))

	originalIds, err := srv.getOriginalJobIds(ctx, apiJobs)
	if err != nil {
		// Deduplication is best-effort, therefore this is not fatal
		log.WithError(err).Warn("Error fetching original job ids, deduplication will not occur.")
	}

	// Check if all jobs can be scheduled.
	// This check uses the NodeDb of the new scheduler and
	// can check if all jobs in a gang can go onto the same cluster.
	if canSchedule, reason := srv.SubmitChecker.CheckApiJobs(apiJobs); !canSchedule {
		return nil, status.Errorf(codes.InvalidArgument, "at least one job or gang is unschedulable:\n%s", reason)
	}

	pulsarJobDetails := make([]*schedulerobjects.PulsarSchedulerJobDetails, 0)

	for i, apiJob := range apiJobs {
		eventTime := time.Now()
		pulsarJobDetails = append(pulsarJobDetails, &schedulerobjects.PulsarSchedulerJobDetails{
			JobId:  apiJob.Id,
			Queue:  apiJob.Queue,
			JobSet: apiJob.JobSetId,
		})

		// Users submit API-specific service and ingress objects.
		// However, the log only accepts proper k8s objects.
		// Hence, the API-specific objects must be converted to proper k8s objects.
		//
		// We use an empty ingress config here.
		// The executor applies executor-specific information later.
		// We only need this here because we're re-using code that was previously called by the executor.
		err = eventutil.PopulateK8sServicesIngresses(apiJob, &executorConfig.IngressConfiguration{})
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
		if _, err := eventutil.ApiJobFromLogSubmitJob(userId, groups, req.Queue, req.JobSetId, time.Now(), logJob); err != nil {
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
				logJob.IsDuplicate = true
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
		} else {
			jobsSubmitted = append(jobsSubmitted, apiJob)
		}
	}

	if len(pulsarJobDetails) > 0 {
		err = srv.JobRepository.StorePulsarSchedulerJobDetails(ctx, pulsarJobDetails)
		if err != nil {
			log.WithError(err).Error("failed store pulsar job details")
			return nil, status.Error(codes.Internal, "failed store pulsar job details")
		}
	}

	if len(es.Events) > 0 {
		err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{es}, schedulers.Pulsar)
		if err != nil {
			log.WithError(err).Error("failed send pulsar scheduler events to Pulsar")
			return nil, status.Error(codes.Internal, "Failed to send message")
		}
	}

	// Store the deduplication ids. note that this will not be called if pulsar submission has failed, which means that
	// a partial pulsar submission will not cause deduplication ids to be updated and thus we may get duplicate jobs
	// if the user then resubmits.  Likewise, if there is a failure in persisting the ids, we treat this as non-fatal so
	// we could get duplicate events.
	err = srv.storeOriginalJobIds(ctx, jobsSubmitted)
	if err != nil {
		log.WithError(err).Warn("failed to store deduplication ids")
	}
	return &api.JobSubmitResponse{JobResponseItems: responses}, nil
}

func (srv *PulsarSubmitServer) CancelJobs(grpcCtx context.Context, req *api.JobCancelRequest) (*api.CancellationResult, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	if req.JobSetId == "" || req.Queue == "" {
		ctx.
			WithField("apidatamissing", "true").
			Warnf("Cancel jobs called with missing data: jobId=%s, jobset=%s, queue=%s, user=%s", req.JobId, req.JobSetId, req.Queue, srv.GetUser(ctx))
	}

	// separate code path for multiple jobs
	if len(req.JobIds) > 0 {
		return srv.cancelJobsByIdsQueueJobset(ctx, req.JobIds, req.Queue, req.JobSetId, req.Reason)
	}

	// Another separate code path for cancelling an entire job set
	// TODO: We should deprecate this and move people over to CancelJobSet()
	if req.JobId == "" {
		log.Warnf("CancelJobs called for queue=%s and jobset=%s but with empty job id. Redirecting to CancelJobSet()", req.Queue, req.JobSetId)
		_, err := srv.CancelJobSet(ctx, &api.JobSetCancelRequest{
			Queue:    req.Queue,
			JobSetId: req.JobSetId,
			Reason:   req.Reason,
		})
		if err != nil {
			return nil, err
		}
		return &api.CancellationResult{
			CancelledIds: []string{req.JobId}, // we return an empty string here which seems a bit nonsensical- but that's what the old code did!
		}, nil
	}

	// resolve the queue and jobset of the job: we can't trust what the user has given us
	resolvedQueue, resolvedJobset, err := srv.resolveQueueAndJobsetForJob(ctx, req.JobId)
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

	jobId, err := armadaevents.ProtoUuidFromUlidString(req.JobId)
	if err != nil {
		return nil, err
	}

	sequence := &armadaevents.EventSequence{
		Queue:      resolvedQueue,
		JobSetName: resolvedJobset,
		UserId:     userId,
		Groups:     groups,
		Events: []*armadaevents.EventSequence_Event{
			{
				Created: pointer.Now(),
				Event: &armadaevents.EventSequence_Event_CancelJob{
					CancelJob: &armadaevents.CancelJob{
						JobId:  jobId,
						Reason: util.Truncate(req.Reason, 512),
					},
				},
			},
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
func (srv *PulsarSubmitServer) cancelJobsByIdsQueueJobset(grpcCtx context.Context, jobIds []string, q, jobSet string, reason string) (*api.CancellationResult, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
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
	sequence, cancelledIds := eventSequenceForJobIds(jobIds, q, jobSet, userId, groups, reason)
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
func eventSequenceForJobIds(jobIds []string, q, jobSet, userId string, groups []string, reason string) (*armadaevents.EventSequence, []string) {
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
			Created: pointer.Now(),
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{
					JobId:  jobId,
					Reason: util.Truncate(reason, 512),
				},
			},
		})
	}
	return sequence, validIds
}

func (srv *PulsarSubmitServer) CancelJobSet(grpcCtx context.Context, req *api.JobSetCancelRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
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

	states := make([]armadaevents.JobState, len(req.GetFilter().GetStates()))
	for i := 0; i < len(states); i++ {
		switch req.GetFilter().GetStates()[i] {
		case api.JobState_PENDING:
			states[i] = armadaevents.JobState_PENDING
		case api.JobState_QUEUED:
			states[i] = armadaevents.JobState_QUEUED
		case api.JobState_RUNNING:
			states[i] = armadaevents.JobState_RUNNING
		}
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
						States: states,
						Reason: util.Truncate(req.Reason, 512),
					},
				},
			},
		},
	}
	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{pulsarSchedulerSequence}, schedulers.Pulsar)
	if err != nil {
		log.WithError(err).Error("failed to send cancel jobset message to pulsar")
		return nil, status.Error(codes.Internal, "failed to send cancel jobset message to pulsar")
	}

	return &types.Empty{}, err
}

func (srv *PulsarSubmitServer) ReprioritizeJobs(grpcCtx context.Context, req *api.JobReprioritizeRequest) (*api.JobReprioritizeResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	if req.JobSetId == "" || req.Queue == "" {
		ctx.
			WithField("apidatamissing", "true").
			Warnf("Reprioritize jobs called with missing data: jobId=%s, jobset=%s, queue=%s, user=%s", req.JobIds[0], req.JobSetId, req.Queue, srv.GetUser(ctx))
	}

	// If either queue or jobSetId is missing, we get the job set and queue associated
	// with the first job id in the request.
	//
	// This must be done before checking auth, since the auth check expects a queue.
	if len(req.JobIds) > 0 && (req.Queue == "" || req.JobSetId == "") {
		firstJobId := req.JobIds[0]

		resolvedQueue, resolvedJobset, err := srv.resolveQueueAndJobsetForJob(ctx, firstJobId)
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
	priority := eventutil.LogSubmitPriorityFromApiPriority(req.NewPriority)

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

	err = srv.publishToPulsar(ctx, []*armadaevents.EventSequence{sequence}, schedulers.Pulsar)

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
	ctx *armadacontext.Context,
	queueName string,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) (string, []string, error) {
	principal := authorization.GetPrincipal(ctx)
	userId := principal.GetName()
	groups := principal.GetGroupNames()
	q, err := srv.QueueRepository.GetQueue(ctx, queueName)
	if err != nil {
		return userId, groups, err
	}
	err = srv.Authorizer.AuthorizeQueueAction(ctx, q, anyPerm, perm)
	return userId, groups, err
}

func (srv *PulsarSubmitServer) GetUser(ctx *armadacontext.Context) string {
	principal := authorization.GetPrincipal(ctx)
	return principal.GetName()
}

func (srv *PulsarSubmitServer) CreateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := srv.Authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[CreateQueue] error creating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error checking permissions: %s", err)
	}

	if len(req.UserOwners) == 0 {
		principal := authorization.GetPrincipal(ctx)
		req.UserOwners = []string{principal.GetName()}
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[CreateQueue] error validating queue: %s", err)
	}

	err = srv.QueueRepository.CreateQueue(ctx, queue)
	var eq *repository.ErrQueueAlreadyExists
	if errors.As(err, &eq) {
		return nil, status.Errorf(codes.AlreadyExists, "[CreateQueue] error creating queue: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error creating queue: %s", err)
	}

	return &types.Empty{}, nil
}

func (srv *PulsarSubmitServer) CreateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueCreateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueCreateResponse
	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := srv.CreateQueue(ctx, queue)
		if err != nil {
			failedQueues = append(failedQueues, &api.QueueCreateResponse{
				Queue: queue,
				Error: err.Error(),
			})
		}
	}

	return &api.BatchQueueCreateResponse{
		FailedQueues: failedQueues,
	}, nil
}

func (srv *PulsarSubmitServer) UpdateQueue(grpcCtx context.Context, req *api.Queue) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := srv.Authorizer.AuthorizeAction(ctx, permissions.CreateQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[UpdateQueue] error updating queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error checking permissions: %s", err)
	}

	queue, err := queue.NewQueue(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[UpdateQueue] error: %s", err)
	}

	err = srv.QueueRepository.UpdateQueue(ctx, queue)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[UpdateQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[UpdateQueue] error getting queue %q: %s", queue.Name, err)
	}

	return &types.Empty{}, nil
}

func (srv *PulsarSubmitServer) UpdateQueues(grpcCtx context.Context, req *api.QueueList) (*api.BatchQueueUpdateResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	var failedQueues []*api.QueueUpdateResponse

	// Create a queue for each element of the request body and return the failures.
	for _, queue := range req.Queues {
		_, err := srv.UpdateQueue(ctx, queue)
		if err != nil {
			failedQueues = append(failedQueues, &api.QueueUpdateResponse{
				Queue: queue,
				Error: err.Error(),
			})
		}
	}

	return &api.BatchQueueUpdateResponse{
		FailedQueues: failedQueues,
	}, nil
}

func (srv *PulsarSubmitServer) DeleteQueue(grpcCtx context.Context, req *api.QueueDeleteRequest) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := srv.Authorizer.AuthorizeAction(ctx, permissions.DeleteQueue)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return nil, status.Errorf(codes.PermissionDenied, "[DeleteQueue] error deleting queue %s: %s", req.Name, ep)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[DeleteQueue] error checking permissions: %s", err)
	}
	err = srv.QueueRepository.DeleteQueue(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "[DeleteQueue] error deleting queue %s: %s", req.Name, err)
	}
	return &types.Empty{}, nil
}

func (srv *PulsarSubmitServer) GetQueue(grpcCtx context.Context, req *api.QueueGetRequest) (*api.Queue, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	queue, err := srv.QueueRepository.GetQueue(ctx, req.Name)
	var e *repository.ErrQueueNotFound
	if errors.As(err, &e) {
		return nil, status.Errorf(codes.NotFound, "[GetQueue] error: %s", err)
	} else if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[GetQueue] error getting queue %q: %s", req.Name, err)
	}
	return queue.ToAPI(), nil
}

func (srv *PulsarSubmitServer) GetQueues(req *api.StreamingQueueGetRequest, stream api.Submit_GetQueuesServer) error {
	// Receive once to get information about the number of queues to return
	numToReturn := req.GetNum()
	if numToReturn < 1 {
		numToReturn = math.MaxUint32
	}
	ctx := armadacontext.FromGrpcCtx(stream.Context())
	queues, err := srv.QueueRepository.GetAllQueues(ctx)
	if err != nil {
		return err
	}
	for i, queue := range queues {
		if uint32(i) < numToReturn {
			err := stream.Send(&api.StreamingQueueMessage{
				Event: &api.StreamingQueueMessage_Queue{Queue: queue.ToAPI()},
			})
			if err != nil {
				return err
			}
		}
	}
	err = stream.Send(&api.StreamingQueueMessage{
		Event: &api.StreamingQueueMessage_End{
			End: &api.EndMarker{},
		},
	})
	if err != nil {
		return err
	}
	return nil
}

func (srv *PulsarSubmitServer) Health(_ context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
	// For now, lets make the health check really simple.
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

// PublishToPulsar sends pulsar messages async
func (srv *PulsarSubmitServer) publishToPulsar(ctx *armadacontext.Context, sequences []*armadaevents.EventSequence, scheduler schedulers.Scheduler) error {
	// Reduce the number of sequences to send to the minimum possible,
	// and then break up any sequences larger than srv.MaxAllowedMessageSize.
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, srv.MaxAllowedMessageSize, true)
	if err != nil {
		return err
	}
	return pulsarutils.PublishSequences(ctx, srv.Producer, sequences, scheduler)
}

func jobKey(j *api.Job) string {
	combined := fmt.Sprintf("%s:%s", j.Queue, j.ClientId)
	h := sha1.Sum([]byte(combined))
	return fmt.Sprintf("%x", h)
}

// getOriginalJobIds returns the mapping between jobId and originalJobId.  If the job (or more specifically the clientId
// on the job) has not been seen before then jobId -> jobId.  If the job has been seen before then jobId -> originalJobId
// Note that if srv.KVStore is nil then this function simply returns jobId -> jobId
func (srv *PulsarSubmitServer) getOriginalJobIds(ctx *armadacontext.Context, apiJobs []*api.Job) (map[string]string, error) {
	// Default is the current id
	ret := make(map[string]string, len(apiJobs))
	for _, apiJob := range apiJobs {
		ret[apiJob.GetId()] = apiJob.GetId()
	}

	// If we don't have a KV store, then just return original mappings
	if srv.KVStore == nil {
		return ret, nil
	}

	// Armada checks for duplicate job submissions if a ClientId (i.e., a deduplication id) is provided.
	// Deduplication is based on storing the combined hash of the ClientId and queue.
	// For storage efficiency, we store hashes instead of user-provided strings.
	kvs := make(map[string][]byte, len(apiJobs))
	for _, apiJob := range apiJobs {
		if apiJob.ClientId != "" {
			kvs[jobKey(apiJob)] = []byte(apiJob.GetId())
		}
	}

	// If we have any client Ids, retrieve their job ids
	if len(kvs) > 0 {
		keys := maps.Keys(kvs)
		existingKvs, err := srv.KVStore.Load(ctx, keys)
		if err != nil {
			return ret, err
		}
		for _, apiJob := range apiJobs {
			originalJobId, ok := existingKvs[jobKey(apiJob)]
			if apiJob.ClientId != "" && ok {
				ret[apiJob.GetId()] = string(originalJobId)
			}
		}
	}
	return ret, nil
}

func (srv *PulsarSubmitServer) storeOriginalJobIds(ctx *armadacontext.Context, apiJobs []*api.Job) error {
	if srv.KVStore == nil {
		return nil
	}
	kvs := make(map[string][]byte, 0)
	for _, apiJob := range apiJobs {
		if apiJob.ClientId != "" {
			kvs[jobKey(apiJob)] = []byte(apiJob.GetId())
		}
	}
	if len(kvs) == 0 {
		return nil
	}
	return srv.KVStore.Store(ctx, kvs)
}

// resolveQueueAndJobsetForJob returns the queue and jobset for a job.
// If no job can be retrieved then an error is returned.
func (srv *PulsarSubmitServer) resolveQueueAndJobsetForJob(ctx *armadacontext.Context, jobId string) (string, string, error) {
	jobDetails, err := srv.JobRepository.GetPulsarSchedulerJobDetails(ctx, jobId)
	if err != nil {
		return "", "", err
	}
	if jobDetails != nil {
		return jobDetails.Queue, jobDetails.JobSet, nil
	}
	return "", "", &armadaerrors.ErrNotFound{
		Type:  "job",
		Value: jobId,
	}
}

// createJobs returns a list of objects representing the jobs in a JobSubmitRequest.
// This function validates the jobs in the request and the pod specs. in each job.
// If any job or pod in invalid, an error is returned.
func (srv *PulsarSubmitServer) createJobs(request *api.JobSubmitRequest, owner string, ownershipGroups []string) ([]*api.Job, []*api.JobSubmitResponseItem, error) {
	return srv.createJobsObjects(request, owner, ownershipGroups, time.Now, util.NewULID)
}

func (srv *PulsarSubmitServer) createJobsObjects(request *api.JobSubmitRequest, owner string, ownershipGroups []string,
	getTime func() time.Time, getUlid func() string,
) ([]*api.Job, []*api.JobSubmitResponseItem, error) {
	compressor, err := srv.CompressorPool.BorrowObject(armadacontext.Background())
	if err != nil {
		return nil, nil, err
	}
	defer func(compressorPool *pool.ObjectPool, ctx *armadacontext.Context, object interface{}) {
		err := compressorPool.ReturnObject(ctx, object)
		if err != nil {
			log.WithError(err).Errorf("Error returning compressor to pool")
		}
	}(srv.CompressorPool, armadacontext.Background(), compressor)
	compressedOwnershipGroups, err := compress.CompressStringArray(ownershipGroups, compressor.(compress.Compressor))
	if err != nil {
		return nil, nil, err
	}

	jobs := make([]*api.Job, 0, len(request.JobRequestItems))

	if request.JobSetId == "" {
		return nil, nil, errors.Errorf("[createJobs] job set not specified")
	}

	if request.Queue == "" {
		return nil, nil, errors.Errorf("[createJobs] queue not specified")
	}

	responseItems := make([]*api.JobSubmitResponseItem, 0, len(request.JobRequestItems))
	for i, item := range request.JobRequestItems {
		jobId := getUlid()

		if item.PodSpec != nil && len(item.PodSpecs) > 0 {
			response := &api.JobSubmitResponseItem{
				JobId: jobId,
				Error: fmt.Sprintf("[createJobs] job %d in job set %s contains both podSpec and podSpecs, but may only contain either", i, request.JobSetId),
			}
			responseItems = append(responseItems, response)
		}
		podSpec := item.GetMainPodSpec()
		if podSpec == nil {
			response := &api.JobSubmitResponseItem{
				JobId: jobId,
				Error: fmt.Sprintf("[createJobs] job %d in job set %s contains no podSpec", i, request.JobSetId),
			}
			responseItems = append(responseItems, response)
			continue // Safety check, to avoid possible nil pointer dereference below
		}
		if err := validation.ValidateJobSubmitRequestItem(item); err != nil {
			response := &api.JobSubmitResponseItem{
				JobId: jobId,
				Error: fmt.Sprintf("[createJobs] error validating the %d-th job of job set %s: %v", i, request.JobSetId, err),
			}
			responseItems = append(responseItems, response)
		}
		namespace := item.Namespace
		if namespace == "" {
			namespace = "default"
		}
		mutationMsg := fillContainerRequestsAndLimits(podSpec.Containers)
		if mutationMsg != "" {
			log.Infof("Inconsistent resources detected for job %s: %s", jobId, mutationMsg)
		}
		applyDefaultsToAnnotations(item.Annotations, srv.SubmissionConfig)
		applyDefaultsToPodSpec(podSpec, srv.SubmissionConfig)
		if err := validation.ValidatePodSpec(podSpec, srv.SubmissionConfig); err != nil {
			response := &api.JobSubmitResponseItem{
				JobId: jobId,
				Error: fmt.Sprintf("[createJobs] error validating the %d-th job of job set %s: %v", i, request.JobSetId, err),
			}
			responseItems = append(responseItems, response)
		}

		// TODO: remove, RequiredNodeLabels is deprecated and will be removed in future versions
		for k, v := range item.RequiredNodeLabels {
			if podSpec.NodeSelector == nil {
				podSpec.NodeSelector = map[string]string{}
			}
			podSpec.NodeSelector[k] = v
		}

		enrichText(item.Labels, jobId)
		enrichText(item.Annotations, jobId)
		j := &api.Job{
			Id:       jobId,
			ClientId: item.ClientId,
			Queue:    request.Queue,
			JobSetId: request.JobSetId,

			Namespace:   namespace,
			Labels:      item.Labels,
			Annotations: item.Annotations,

			RequiredNodeLabels: item.RequiredNodeLabels,
			Ingress:            item.Ingress,
			Services:           item.Services,

			Priority: item.Priority,

			Scheduler:                          item.Scheduler,
			PodSpec:                            item.PodSpec,
			PodSpecs:                           item.PodSpecs,
			Created:                            getTime(), // Replaced with now for mocking unit test
			Owner:                              owner,
			QueueOwnershipUserGroups:           nil,
			CompressedQueueOwnershipUserGroups: compressedOwnershipGroups,
			QueueTtlSeconds:                    item.QueueTtlSeconds,
		}
		jobs = append(jobs, j)
	}

	if len(responseItems) > 0 {
		return nil, responseItems, errors.New("[createJobs] error creating jobs, check JobSubmitResponse for details")
	}
	return jobs, nil, nil
}

func enrichText(labels map[string]string, jobId string) {
	for key, value := range labels {
		value := strings.ReplaceAll(value, "{{JobId}}", ` \z`) // \z cannot be entered manually, hence its use
		value = strings.ReplaceAll(value, "{JobId}", jobId)
		labels[key] = strings.ReplaceAll(value, ` \z`, "JobId")
	}
}
