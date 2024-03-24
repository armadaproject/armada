package submit

import (
	"context"
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/server"
	"github.com/armadaproject/armada/internal/armada/submit/conversion"
	"github.com/armadaproject/armada/internal/armada/submit/validation"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// Server is a service that accepts API calls according to the original Armada submit API and publishes messages
// to Pulsar based on those calls.
type Server struct {
	publisher        Publisher
	queueRepository  repository.QueueRepository
	jobRepository    repository.JobRepository
	submissionConfig configuration.SubmissionConfig
	deduplicator     Deduplicator
	submitChecker    *scheduler.SubmitChecker
	authorizer       server.ActionAuthorizer
}

func NewServer(
	publisher Publisher,
	queueRepository repository.QueueRepository,
	jobRepository repository.JobRepository,
	submissionConfig configuration.SubmissionConfig,
	deduplicator Deduplicator,
	submitChecker *scheduler.SubmitChecker,
	authorizer server.ActionAuthorizer,
) *Server {
	return &Server{
		publisher:        publisher,
		queueRepository:  queueRepository,
		jobRepository:    jobRepository,
		submissionConfig: submissionConfig,
		deduplicator:     deduplicator,
		submitChecker:    submitChecker,
		authorizer:       authorizer,
	}
}

func (s *Server) SubmitJobs(grpcCtx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	// Check that the user is actually allowed to submit jobs
	userId, groups, err := s.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// Validate the request is well-formed
	if err = validation.ValidateSubmitRequest(req, s.submissionConfig); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// Get a mapping between req.ClientId and existing jobId.  If such a mapping exists, it means that
	// this job has already been submitted.
	originalIds, err := s.deduplicator.GetOriginalJobIds(ctx, req.Queue, req.JobRequestItems)
	if err != nil {
		// Deduplication is best-effort, therefore this is not fatal
		log.WithError(err).Warn("Error fetching original job ids, deduplication will not occur.")
	}

	submitMsgs := make([]*armadaevents.EventSequence_Event, 0, len(req.JobRequestItems))
	jobResponses := make([]*api.JobSubmitResponseItem, 0, len(req.JobRequestItems))
	idMappings := make(map[string]string, len(req.JobRequestItems))

	for _, jobRequest := range req.JobRequestItems {

		// Check if this job has already been submitted. If so we can simply return the previously submitted id
		originalId, isDuplicate := originalIds[jobRequest.ClientId]
		if isDuplicate {
			ctx.Infof("Job with client id %s is a duplicate of %s", jobRequest.ClientId, originalId)
			jobResponses = append(jobResponses, &api.JobSubmitResponseItem{JobId: originalId})
			continue
		}

		// If we get to here then it isn't a duplicate. Create a Job submission and a job response
		submitMsg := conversion.SubmitJobFromApiRequest(req, jobRequest, userId)
		eventTime := time.Now().UTC()
		submitMsgs = append(submitMsgs, &armadaevents.EventSequence_Event{
			Created: &eventTime,
			Event: &armadaevents.EventSequence_Event_SubmitJob{
				SubmitJob: submitMsg,
			},
		})

		jobResponses = append(jobResponses, &api.JobSubmitResponseItem{
			JobId: armadaevents.MustUlidStringFromProtoUuid(submitMsg.JobId),
		})

		if jobRequest.ClientId != "" {
			idMappings[jobRequest.ClientId] = armadaevents.MustUlidStringFromProtoUuid(submitMsg.JobId)
		}
	}

	// Check if all jobs can be scheduled.
	es := &armadaevents.EventSequence{
		Queue:      req.Queue,
		JobSetName: req.JobSetId,
		UserId:     userId,
		Groups:     groups,
		Events:     submitMsgs,
	}
	if canSchedule, reason := s.submitChecker.CheckApiJobs(es); !canSchedule {
		return nil, status.Errorf(codes.InvalidArgument, "at least one job or gang is unschedulable:\n%s", reason)
	}

	if len(jobResponses) > 0 {
		pulsarJobDetails := armadaslices.Map(
			jobResponses,
			func(r *api.JobSubmitResponseItem) *schedulerobjects.PulsarSchedulerJobDetails {
				return &schedulerobjects.PulsarSchedulerJobDetails{
					JobId:  r.JobId,
					Queue:  req.Queue,
					JobSet: req.JobSetId,
				}
			})

		if err = s.jobRepository.StorePulsarSchedulerJobDetails(ctx, pulsarJobDetails); err != nil {
			log.WithError(err).Error("failed store pulsar job details")
			return nil, status.Error(codes.Internal, "failed store pulsar job details")
		}
	}

	if len(es.Events) > 0 {
		err = s.publisher.PublishMessages(ctx, es)
		if err != nil {
			log.WithError(err).Error("failed send events to Pulsar")
			return nil, status.Error(codes.Internal, "Failed to send events to Pulsar")
		}
	}

	// Store the deduplication ids. Note that this will not be called if pulsar submission has failed, hence
	// a partial pulsar submission can result in duplicate jobs.
	if err = s.deduplicator.StoreOriginalJobIds(ctx, req.Queue, idMappings); err != nil {
		log.WithError(err).Warn("failed to store deduplication ids")
	}
	return &api.JobSubmitResponse{JobResponseItems: jobResponses}, nil
}

// Authorize authorizes a user request to submit a state transition message to the log.
// User information used for authorization is extracted from the provided context.
// Checks that the user has either anyPerm (e.g., permissions.SubmitAnyJobs) or perm (e.g., PermissionVerbSubmit) for this queue.
// Returns the userId and groups extracted from the context.
func (s *Server) Authorize(
	ctx *armadacontext.Context,
	queueName string,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) (string, []string, error) {
	principal := authorization.GetPrincipal(ctx)
	userId := principal.GetName()
	groups := principal.GetGroupNames()
	q, err := s.queueRepository.GetQueue(ctx, queueName)
	if err != nil {
		return userId, groups, err
	}
	err = s.authorizer.AuthorizeQueueAction(ctx, q, anyPerm, perm)
	return userId, groups, err
}

func (s *Server) GetUser(ctx *armadacontext.Context) string {
	principal := authorization.GetPrincipal(ctx)
	return principal.GetName()
}

func (s *Server) Health(_ context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
	// For now, lets make the health check really simple.
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

func jobKey(queue, clientId string) string {
	combined := fmt.Sprintf("%s:%s", queue, clientId)
	h := sha1.Sum([]byte(combined))
	return fmt.Sprintf("%x", h)
}
