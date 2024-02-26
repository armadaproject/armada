package submit

import (
	"context"
	"crypto/sha1"
	"fmt"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/types"
	"github.com/gogo/status"
	pool "github.com/jolestar/go-commons-pool"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/server"
	"github.com/armadaproject/armada/internal/armada/validation"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/schedulers"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
)

// SubmitServer is a service that accepts API calls according to the original Armada submit API
// and publishes messages to Pulsar based on those calls.
type SubmitServer struct {
	Producer         pulsar.Producer
	QueueRepository  repository.QueueRepository
	JobRepository    repository.JobRepository
	SchedulingConfig configuration.SchedulingConfig
	// Maximum size of Pulsar messages
	MaxAllowedMessageSize uint
	// Used for job submission deduplication.
	KVStore *pgkeyvalue.PGKeyValueStore
	// Used to check at job submit time if the job could ever be scheduled
	SubmitChecker    *scheduler.SubmitChecker
	Authorizer       server.ActionAuthorizer
	CompressorPool   *pool.ObjectPool
	RequestValidator validation.Validator[*api.JobSubmitRequest]
}

func (srv *SubmitServer) SubmitJobs(grpcCtx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	// Check that the user is actually allowed to submit jobs
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// Validate the request is well-formed
	err = srv.RequestValidator.Validate(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// Get a mapping between req.ClientId and existing jobId.  If such a mapping exists, it means that
	// this job has already been submitted and we don't need to resubmit it
	originalIds, err := srv.getOriginalJobIds(ctx, req.Queue, req.JobRequestItems)
	if err != nil {
		// Deduplication is best-effort, therefore this is not fatal
		log.WithError(err).Warn("Error fetching original job ids, deduplication will not occur.")
	}

	submitMsgs := make([]*armadaevents.EventSequence_Event, 0, len(req.JobRequestItems))
	jobResponses := make([]*api.JobSubmitResponseItem, 0, len(req.JobRequestItems))

	for _, jobRequest := range req.JobRequestItems {

		// Check if this job has already been submitted. If so we can simply return the previously submitted id
		originalId, isDuplicate := originalIds[jobRequest.ClientId]
		if isDuplicate {
			ctx.Infof("Job with client id %s is a duplicate of %s", jobRequest.ClientId, originalId)
			jobResponses = append(jobResponses, &api.JobSubmitResponseItem{JobId: originalId})
			continue
		}

		// If we get to here then it isn't a duplicate. Create a Job submission
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
	}

	// Check if all jobs can be scheduled.
	if canSchedule, reason := srv.SubmitChecker.CheckApiJobs(req.JobRequestItems); !canSchedule {
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

		err = srv.JobRepository.StorePulsarSchedulerJobDetails(pulsarJobDetails)
		if err != nil {
			log.WithError(err).Error("failed store pulsar job details")
			return nil, status.Error(codes.Internal, "failed store pulsar job details")
		}
	}

	if len(submitMsgs) > 0 {
		es := []*armadaevents.EventSequence{
			{
				Queue:      req.Queue,
				JobSetName: req.JobSetId,
				UserId:     userId,
				Groups:     groups,
				Events:     submitMsgs,
			},
		}
		err = pulsarutils.CompactAndPublishSequences(ctx, es, srv.Producer, srv.MaxAllowedMessageSize, schedulers.Pulsar)
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
	return &api.JobSubmitResponse{JobResponseItems: jobResponses}, nil
}

// Authorize authorises a user request to submit a state transition message to the log.
// User information used for authorization is extracted from the provided context.
// Checks that the user has either anyPerm (e.g., permissions.SubmitAnyJobs) or perm (e.g., PermissionVerbSubmit) for this queue.
// Returns the userId and groups extracted from the context.
func (srv *SubmitServer) Authorize(
	ctx *armadacontext.Context,
	queueName string,
	anyPerm permission.Permission,
	perm queue.PermissionVerb,
) (string, []string, error) {
	principal := authorization.GetPrincipal(ctx)
	userId := principal.GetName()
	groups := principal.GetGroupNames()
	q, err := srv.QueueRepository.GetQueue(queueName)
	if err != nil {
		return userId, groups, err
	}
	err = srv.Authorizer.AuthorizeQueueAction(ctx, q, anyPerm, perm)
	return userId, groups, err
}

func (srv *SubmitServer) GetUser(ctx *armadacontext.Context) string {
	principal := authorization.GetPrincipal(ctx)
	return principal.GetName()
}

func (srv *SubmitServer) Health(ctx context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
	// For now, lets make the health check really simple.
	return &api.HealthCheckResponse{Status: api.HealthCheckResponse_SERVING}, nil
}

func jobKey(queue, clientId string) string {
	combined := fmt.Sprintf("%s:%s", queue., clientId)
	h := sha1.Sum([]byte(combined))
	return fmt.Sprintf("%x", h)
}

func (srv *SubmitServer) getOriginalJobIds(ctx *armadacontext.Context, queue string, jobRequests []*api.JobSubmitRequestItem) (map[string]string, error) {

	// If we don't have a KV store, then just return empty map
	if srv.KVStore == nil {
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
		existingKvs, err := srv.KVStore.Load(ctx, keys)
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

func (srv *SubmitServer) storeOriginalJobIds(ctx *armadacontext.Context, apiJobs []*api.Job) error {
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
