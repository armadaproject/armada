package submit

import (
	"context"
	"crypto/sha1"
	"fmt"
	"github.com/armadaproject/armada/internal/common/logging"
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
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/permission"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pgkeyvalue"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/client/queue"
	"github.com/armadaproject/armada/internal/armada/server"
	"github.com/armadaproject/armada/internal/common/util"
)

// PulsarSubmitServer is a service that accepts API calls according to the original Armada submit API
// and publishes messages to Pulsar based on those calls.
// TODO: Consider returning a list of message ids of the messages generated
// TODO: Include job set as the message key for each message
type PulsarSubmitServer struct {
	Producer         pulsar.Producer
	QueueRepository  repository.QueueRepository
	JobRepository    repository.JobRepository
	SchedulingConfig configuration.SchedulingConfig
	// Maximum size of Pulsar messages
	MaxAllowedMessageSize uint
	// Used for job submission deduplication.
	KVStore *pgkeyvalue.PGKeyValueStore
	// Used to check at job submit time if the job could ever be scheduled on either legacy or pulsar schedulers
	SubmitChecker  *scheduler.SubmitChecker
	Authorizer     server.ActionAuthorizer
	CompressorPool *pool.ObjectPool
	RequestValidator validation.Validator[*api.JobSubmitRequest]
}

func (srv *PulsarSubmitServer) SubmitJobs(grpcCtx context.Context, req *api.JobSubmitRequest) (*api.JobSubmitResponse, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)

	// Check that the user is actually allowed to submit jobs
	userId, groups, err := srv.Authorize(ctx, req.Queue, permissions.SubmitAnyJobs, queue.PermissionVerbSubmit)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}

	// Validate the request is well formed
	err = srv.RequestValidator.Validate(req)
	if err != nil{
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// Get a mapping between req.ClientId and existing jobId.  If such a mapping exists, it means that
	// This job has already been submitted and we don't need to resubmit it
	originalIds, err := srv.getOriginalJobIds(ctx, req.Queue, req.JobRequestItems)
	if err != nil {
		// Deduplication is best-effort, therefore this is not fatal
		log.WithError(err).Warn("Error fetching original job ids, deduplication will not occur.")
	}

	// Check if all jobs can be scheduled. This check uses the NodeDb of the new scheduler and can check
	// if all jobs in a gang can go onto the same cluster.
	if canSchedule, reason := srv.SubmitChecker.CheckApiJobs(apiJobs); !canSchedule {
		return nil, status.Errorf(codes.InvalidArgument, "at least one job or gang is unschedulable:\n%s", reason)
	}

	responses := make([]*api.JobSubmitResponseItem, 0, len(req.JobRequestItems))

	for i, jobRequest := range req.JobRequestItems {

		// Check if this job has already been submitted. If so we can simply return the previously submitted id
		originalId, isDuplicate := originalIds[jobRequest.ClientId]
		if isDuplicate {
			responses[i]= &api.JobSubmitResponseItem{JobId: originalId}
			continue
		}

		eventTime := time.Now()
		var jobId = util.NewULID()



		j, err := eventutil.LogSubmitJobFromApiJob(jobId, jobRequest)
		if err != nil{
			ctx.WithError(err).Errorf("Error in job conversion")
			return nil, status.Error(codes.Internal, "Error in job conversion")
		}
	}

	if len(pulsarJobDetails) > 0 {
		err = srv.JobRepository.StorePulsarSchedulerJobDetails(pulsarJobDetails)
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
	q, err := srv.QueueRepository.GetQueue(queueName)
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

func (srv *PulsarSubmitServer) Health(ctx context.Context, _ *types.Empty) (*api.HealthCheckResponse, error) {
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

func jobKey(queue, clientId string) string {
	combined := fmt.Sprintf("%s:%s", queue., clientId)
	h := sha1.Sum([]byte(combined))
	return fmt.Sprintf("%x", h)
}

func (srv *PulsarSubmitServer) getOriginalJobIds(ctx *armadacontext.Context, queue string, jobRequests []*api.JobSubmitRequestItem) (map[string]string, error) {

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

	duplicates := make(map[string]string, 0)
	// If we have any client Ids, retrieve their job ids
	if len(kvs) > 0 {
		keys := maps.Keys(kvs)
		existingKvs, err := srv.KVStore.Load(ctx, keys)
		if err != nil {
			return nil, err
		}
		for k, v := range kvs {
			originalJobId, ok := existingKvs[k]
			if ok{
				duplicates[string(v)] = string(originalJobId)
			}
		}
	}
	return duplicates, nil
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

func (srv *PulsarSubmitServer) createJobsObjects(request *api.JobSubmitRequest, owner string, ownershipGroups []string) ([]*api.Job, error) {
	compressor, err := srv.CompressorPool.BorrowObject(armadacontext.Background())
	if err != nil {
		return nil, err
	}
	defer func(compressorPool *pool.ObjectPool, ctx *armadacontext.Context, object interface{}) {
		err := compressorPool.ReturnObject(ctx, object)
		if err != nil {
			log.WithError(err).Errorf("Error returning compressor to pool")
		}
	}(srv.CompressorPool, armadacontext.Background(), compressor)
	compressedOwnershipGroups, err := compress.CompressStringArray(ownershipGroups, compressor.(compress.Compressor))
	if err != nil {
		return nil, err
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
		fillContainerRequestsAndLimits(podSpec.Containers)
		applyDefaultsToAnnotations(item.Annotations, srv.SchedulingConfig)
		applyDefaultsToPodSpec(podSpec, srv.SchedulingConfig)
		if err := validation.ValidatePodSpec(podSpec, srv.SchedulingConfig); err != nil {
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
