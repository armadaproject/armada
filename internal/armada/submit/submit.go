package submit

import (
	"context"
	"crypto/sha1"
	"fmt"
	"github.com/armadaproject/armada/internal/armada/submit/validation"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/server"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/schedulers"
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
	Producer         pulsar.Producer
	QueueRepository  repository.QueueRepository
	JobRepository    repository.JobRepository
	SchedulingConfig configuration.SchedulingConfig
	// Maximum size of Pulsar messages
	MaxAllowedMessageSize uint
	// Used for job submission deduplication.
	KVStore *pgkeyvalue.PGKeyValueStore
	// Used to check at job submit time if the job is unschedulable
	SubmitChecker *scheduler.SubmitChecker
	Authorizer    server.ActionAuthorizer
}

func (s *Server) SubmitJobs(grpcCtx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	// Check that the user is actually allowed to submit jobs
	userId, groups, err := s.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// Validate the request is well-formed
	if err = validation.ValidateSubmitRequest(req, s.SchedulingConfig); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// Get a mapping between req.ClientId and existing jobId.  If such a mapping exists, it means that
	// this job has already been submitted.
	originalIds, err := s.getOriginalJobIds(ctx, req.Queue, req.JobRequestItems)
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
		submitMsg := eventutil.LogSubmitJobFromApiJob(jobRequest)
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
	if canSchedule, reason := s.SubmitChecker.CheckApiJobs(es); !canSchedule {
		return nil, status.Errorf(codes.InvalidArgument, "at least one job or gang is unschedulable:\n%s", reason)
	}

	if len(jobResponses) > 0 {
		pulsarJobDetails := armadaslices.Map[](
			jobResponses,
			func(r *api.JobSubmitResponseItem) *schedulerobjects.PulsarSchedulerJobDetails {
				return &schedulerobjects.PulsarSchedulerJobDetails{
					JobId:  r.JobId,
					Queue:  req.Queue,
					JobSet: req.JobSetId,
				}
			})

		if err = s.JobRepository.StorePulsarSchedulerJobDetails(pulsarJobDetails); err != nil {
			log.WithError(err).Error("failed store pulsar job details")
			return nil, status.Error(codes.Internal, "failed store pulsar job details")
		}
	}

	if len(es.Events) > 0 {
		err = pulsarutils.CompactAndPublishSequences(ctx, []*armadaevents.EventSequence{es}, s.Producer, s.MaxAllowedMessageSize, schedulers.Pulsar)
		if err != nil {
			log.WithError(err).Error("failed send events to Pulsar")
			return nil, status.Error(codes.Internal, "Failed to send message")
		}
	}

	// Store the deduplication ids. Note that this will not be called if pulsar submission has failed, hence
	// a partial pulsar submission can result in duplicate jobs.
	if err = s.storeOriginalJobIds(ctx, req.Queue, idMappings); err != nil {
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
	q, err := s.QueueRepository.GetQueue(queueName)
	if err != nil {
		return userId, groups, err
	}
	err = s.Authorizer.AuthorizeQueueAction(ctx, q, anyPerm, perm)
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

func (s *Server) getOriginalJobIds(ctx *armadacontext.Context, queue string, jobRequests []*api.JobSubmitRequestItem) (map[string]string, error) {

	// If we don't have a KV store, then just return empty map
	if s.KVStore == nil {
		return map[string]string{}, nil
	}

	// Armada checks for duplicate job submissions if a ClientId (i.e. a deduplication id) is provided.
	// Deduplication is based on storing the combined hash of the ClientId and queue. For storage efficiency,
	// we store hashes instead of user-provided strings.
	kvs := make(map[string][]byte, len(jobRequests))
	for _, req := range jobRequests {
		if req.ClientId != "" {
			kvs[jobKey(queue, req.ClientId)] = []byte(req.ClientId)
		}
	}

	duplicates := make(map[string]string)
	// If we have any client Ids, retrieve their job ids
	if len(kvs) > 0 {
		keys := maps.Keys(kvs)
		existingKvs, err := s.KVStore.Load(ctx, keys)
		if err != nil {
			return nil, err
		}
		for k, v := range kvs {
			originalJobId, ok := existingKvs[k]
			if ok {
				duplicates[string(v)] = string(originalJobId)
			}
		}
	}
	return duplicates, nil
}

func (s *Server) storeOriginalJobIds(ctx *armadacontext.Context, queue string, mappings map[string]string) error {
	if s.KVStore == nil || len(mappings) == 0 {
		return nil
	}
	kvs := make(map[string][]byte, len(mappings))
	for k, v := range mappings {
		kvs[jobKey(queue, k)] = []byte(v)
	}
	return s.KVStore.Store(ctx, kvs)
}
