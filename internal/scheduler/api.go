package scheduler

import (
	"context"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/compress"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	priorityTypes "github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/internal/server/permissions"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

const armadaJobPreemptibleLabel = "armada_preemptible"

// ExecutorApi is the gRPC service executors use to synchronise their state with that of the scheduler.
type ExecutorApi struct {
	// Used to send event sequences received from the executors about job state change to Pulsar
	publisher pulsarutils.Publisher[*armadaevents.EventSequence]
	// Interface to the component storing job information, such as which jobs are leased to a particular executor.
	jobRepository database.JobRepository
	// Interface to the component storing executor information, such as which when we last heard from an executor.
	executorRepository database.ExecutorRepository
	// Allowed priority class priorities.
	allowedPriorities []int32
	// Known priority classes
	priorityClasses map[string]priorityTypes.PriorityClass
	// Allowed resource names - resource requests/limits not on this list are dropped.
	// This is needed to ensure floating resources are not passed to k8s.
	allowedResources map[string]bool
	nodeIdLabel      string
	// See scheduling schedulingConfig.
	priorityClassNameOverride *string
	clock                     clock.Clock
	authorizer                auth.ActionAuthorizer
}

func NewExecutorApi(publisher pulsarutils.Publisher[*armadaevents.EventSequence],
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	allowedPriorities []int32,
	allowedResources []string,
	nodeIdLabel string,
	priorityClassNameOverride *string,
	priorityClasses map[string]priorityTypes.PriorityClass,
	authorizer auth.ActionAuthorizer,
) (*ExecutorApi, error) {
	if len(allowedPriorities) == 0 {
		return nil, errors.New("allowedPriorities cannot be empty")
	}
	return &ExecutorApi{
		publisher:                 publisher,
		jobRepository:             jobRepository,
		executorRepository:        executorRepository,
		allowedPriorities:         allowedPriorities,
		allowedResources:          maps.FromSlice(allowedResources, func(name string) string { return name }, func(name string) bool { return true }),
		nodeIdLabel:               nodeIdLabel,
		priorityClassNameOverride: priorityClassNameOverride,
		priorityClasses:           priorityClasses,
		clock:                     clock.RealClock{},
		authorizer:                authorizer,
	}, nil
}

// LeaseJobRuns reconciles the state of the executor with that of the scheduler. Specifically it:
// 1. Stores job and capacity information received from the executor to make it available to the scheduler.
// 2. Notifies the executor if any of its jobs are no longer active, e.g., due to being preempted by the scheduler.
// 3. Transfers any jobs scheduled on this executor cluster that the executor don't already have.
func (srv *ExecutorApi) LeaseJobRuns(stream executorapi.ExecutorApi_LeaseJobRunsServer) error {
	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	ctx := armadacontext.WithLogField(armadacontext.FromGrpcCtx(stream.Context()), "executor", req.ExecutorId)
	err = srv.authorize(ctx)
	if err != nil {
		return err
	}

	executor := srv.executorFromLeaseRequest(ctx, req)
	if err := srv.executorRepository.StoreExecutor(ctx, executor); err != nil {
		return err
	}

	requestRuns, err := runIdsFromLeaseRequest(req)
	if err != nil {
		return err
	}
	runsToCancel, err := srv.jobRepository.FindInactiveRuns(ctx, requestRuns)
	if err != nil {
		return err
	}
	newRuns, err := srv.jobRepository.FetchJobRunLeases(ctx, req.ExecutorId, uint(req.MaxJobsToLease), requestRuns)
	if err != nil {
		return err
	}
	ctx.Infof(
		"Executor currently has %d job runs; sending %d cancellations and %d new runs",
		len(requestRuns), len(runsToCancel), len(newRuns),
	)

	// Send any runs that should be cancelled.
	if len(runsToCancel) > 0 {
		if err := stream.Send(&executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_CancelRuns{
				CancelRuns: &executorapi.CancelRuns{
					JobRunIdsToCancel: runsToCancel,
				},
			},
		}); err != nil {
			return errors.WithStack(err)
		}
	}

	// Send any scheduled jobs the executor doesn't already have.
	decompressor := compress.NewZlibDecompressor()
	for _, lease := range newRuns {
		submitMsg := &armadaevents.SubmitJob{}
		if err := unmarshalFromCompressedBytes(lease.SubmitMessage, decompressor, submitMsg); err != nil {
			return err
		}

		srv.addNodeIdSelector(submitMsg, lease.Node)
		addAnnotations(submitMsg, map[string]string{configuration.PoolAnnotation: lease.Pool})

		if len(lease.PodRequirementsOverlay) > 0 {
			PodRequirementsOverlay := schedulerobjects.PodRequirements{}
			if err := proto.Unmarshal(lease.PodRequirementsOverlay, &PodRequirementsOverlay); err != nil {
				return err
			}
			addTolerations(submitMsg, PodRequirementsOverlay.Tolerations)
			addAnnotations(submitMsg, PodRequirementsOverlay.Annotations)
		}

		srv.addPreemptibleLabel(submitMsg)

		srv.dropDisallowedResources(submitMsg.MainObject.GetPodSpec().PodSpec)

		// This must happen after anything that relies on the priorityClassName
		if srv.priorityClassNameOverride != nil {
			srv.setPriorityClassName(submitMsg, *srv.priorityClassNameOverride)
		}

		var groups []string
		if len(lease.Groups) > 0 {
			groups, err = compress.DecompressStringArray(lease.Groups, decompressor)
			if err != nil {
				return err
			}
		}

		err := stream.Send(&executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_Lease{
				Lease: &executorapi.JobRunLease{
					JobRunId: lease.RunID,
					Queue:    lease.Queue,
					Jobset:   lease.JobSet,
					User:     lease.UserID,
					Groups:   groups,
					Job:      submitMsg,
				},
			},
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Finally, send an end marker
	err = stream.Send(&executorapi.LeaseStreamMessage{
		Event: &executorapi.LeaseStreamMessage_End{
			End: &executorapi.EndMarker{},
		},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (srv *ExecutorApi) setPriorityClassName(job *armadaevents.SubmitJob, priorityClassName string) {
	if job == nil {
		return
	}
	if job.MainObject != nil {
		switch typed := job.MainObject.Object.(type) {
		case *armadaevents.KubernetesMainObject_PodSpec:
			setPriorityClassName(typed.PodSpec, priorityClassName)
		}
	}
}

func setPriorityClassName(podSpec *armadaevents.PodSpecWithAvoidList, priorityClassName string) {
	if podSpec == nil || podSpec.PodSpec == nil {
		return
	}
	podSpec.PodSpec.PriorityClassName = priorityClassName
}

func (srv *ExecutorApi) addPreemptibleLabel(job *armadaevents.SubmitJob) {
	isPremptible := srv.isPreemptible(job)
	labels := map[string]string{armadaJobPreemptibleLabel: strconv.FormatBool(isPremptible)}
	addLabels(job, labels)
}

// Drop non-supported resources. This is needed to ensure floating resources
// are not passed to k8s.
func (srv *ExecutorApi) dropDisallowedResources(pod *v1.PodSpec) {
	if pod == nil {
		return
	}
	srv.dropDisallowedResourcesFromContainers(pod.InitContainers)
	srv.dropDisallowedResourcesFromContainers(pod.Containers)
}

func (srv *ExecutorApi) dropDisallowedResourcesFromContainers(containers []v1.Container) {
	for _, container := range containers {
		removeDisallowedKeys(container.Resources.Limits, srv.allowedResources)
		removeDisallowedKeys(container.Resources.Requests, srv.allowedResources)
	}
}

func removeDisallowedKeys(rl v1.ResourceList, allowedKeys map[string]bool) {
	maps.RemoveInPlace(rl, func(name v1.ResourceName) bool {
		return !allowedKeys[string(name)]
	})
}

func (srv *ExecutorApi) isPreemptible(job *armadaevents.SubmitJob) bool {
	priorityClassName := ""

	if job.MainObject != nil {
		switch typed := job.MainObject.Object.(type) {
		case *armadaevents.KubernetesMainObject_PodSpec:
			if typed.PodSpec != nil && typed.PodSpec.PodSpec != nil {
				priorityClassName = typed.PodSpec.PodSpec.PriorityClassName
			}
		default:
			return false
		}
	}

	priority, known := srv.priorityClasses[priorityClassName]
	if priorityClassName == "" {
		log.Errorf("priority class name not set on job %s", job.JobId)
		return false
	}

	if !known {
		log.Errorf("unknown priority class found %s on job %s", priorityClassName, job.JobId)
		return false
	}

	return priority.Preemptible
}

func (srv *ExecutorApi) addNodeIdSelector(job *armadaevents.SubmitJob, nodeId string) {
	if job == nil || nodeId == "" {
		return
	}
	if job.MainObject != nil {
		switch typed := job.MainObject.Object.(type) {
		case *armadaevents.KubernetesMainObject_PodSpec:
			addNodeSelector(typed.PodSpec, srv.nodeIdLabel, nodeId)
		}
	}
}

func addNodeSelector(podSpec *armadaevents.PodSpecWithAvoidList, key string, value string) {
	if podSpec == nil || podSpec.PodSpec == nil || key == "" || value == "" {
		return
	}
	if podSpec.PodSpec.NodeSelector == nil {
		podSpec.PodSpec.NodeSelector = map[string]string{key: value}
	} else {
		podSpec.PodSpec.NodeSelector[key] = value
	}
}

func addTolerations(job *armadaevents.SubmitJob, tolerations []*v1.Toleration) {
	if job == nil || len(tolerations) == 0 {
		return
	}
	if job.MainObject != nil {
		switch typed := job.MainObject.Object.(type) {
		case *armadaevents.KubernetesMainObject_PodSpec:
			if typed.PodSpec != nil && typed.PodSpec.PodSpec != nil {
				for _, toleration := range tolerations {
					typed.PodSpec.PodSpec.Tolerations = append(typed.PodSpec.PodSpec.Tolerations, *toleration)
				}
			}
		}
	}
}

func addLabels(job *armadaevents.SubmitJob, labels map[string]string) {
	if job == nil || len(labels) == 0 {
		return
	}
	if job.ObjectMeta == nil {
		job.ObjectMeta = &armadaevents.ObjectMeta{}
	}
	if job.ObjectMeta.Labels == nil {
		job.ObjectMeta.Labels = make(map[string]string, len(labels))
	}
	for k, v := range labels {
		job.ObjectMeta.Labels[k] = v
	}
}

func addAnnotations(job *armadaevents.SubmitJob, annotations map[string]string) {
	if job == nil || len(annotations) == 0 {
		return
	}
	if job.ObjectMeta == nil {
		job.ObjectMeta = &armadaevents.ObjectMeta{}
	}
	if job.ObjectMeta.Annotations == nil {
		job.ObjectMeta.Annotations = make(map[string]string, len(annotations))
	}
	for k, v := range annotations {
		job.ObjectMeta.Annotations[k] = v
	}
}

// ReportEvents publishes all eventSequences to Pulsar. The eventSequences are compacted for more efficient publishing.
func (srv *ExecutorApi) ReportEvents(grpcCtx context.Context, list *executorapi.EventList) (*types.Empty, error) {
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	err := srv.authorize(ctx)
	if err != nil {
		return nil, err
	}

	err = srv.publisher.PublishMessages(ctx, list.GetEvents()...)
	return &types.Empty{}, err
}

func (srv *ExecutorApi) authorize(ctx *armadacontext.Context) error {
	err := srv.authorizer.AuthorizeAction(ctx, permissions.ExecuteJobs)
	var ep *armadaerrors.ErrUnauthorized
	if errors.As(err, &ep) {
		return status.Errorf(codes.PermissionDenied, err.Error())
	} else if err != nil {
		return status.Errorf(codes.Unavailable, "error checking permissions: %s", err)
	}

	return nil
}

// executorFromLeaseRequest extracts a schedulerobjects.Executor from the request.
func (srv *ExecutorApi) executorFromLeaseRequest(ctx *armadacontext.Context, req *executorapi.LeaseRequest) *schedulerobjects.Executor {
	nodes := make([]*schedulerobjects.Node, 0, len(req.Nodes))
	now := srv.clock.Now().UTC()
	for _, nodeInfo := range req.Nodes {
		if node, err := executorapi.NewNodeFromNodeInfo(nodeInfo, req.ExecutorId, srv.allowedPriorities, now); err != nil {
			ctx.Logger().WithStacktrace(err).Warnf(
				"skipping node %s from executor %s", nodeInfo.GetName(), req.GetExecutorId(),
			)
		} else {
			nodes = append(nodes, node)
		}
	}
	return &schedulerobjects.Executor{
		Id:                req.ExecutorId,
		Pool:              req.Pool,
		Nodes:             nodes,
		LastUpdateTime:    now,
		UnassignedJobRuns: req.UnassignedJobRunIds,
	}
}

// runIdsFromLeaseRequest returns the ids of all runs in a lease request, including any not yet assigned to a node.
func runIdsFromLeaseRequest(req *executorapi.LeaseRequest) ([]string, error) {
	runIds := make([]string, 0, 256)
	for _, node := range req.Nodes {
		for runId := range node.RunIdsByState {
			runIds = append(runIds, runId)
		}
	}
	for _, runId := range req.UnassignedJobRunIds {
		runIds = append(runIds, runId)
	}
	return runIds, nil
}

func unmarshalFromCompressedBytes(bytes []byte, decompressor compress.Decompressor, msg proto.Message) error {
	decompressedBytes, err := decompressor.Decompress(bytes)
	if err != nil {
		return err
	}
	return proto.Unmarshal(decompressedBytes, msg)
}
