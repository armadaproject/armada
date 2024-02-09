package scheduler

import (
	"context"
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

// ExecutorApi is the gRPC service executors use to synchronise their state with that of the scheduler.
type ExecutorApi struct {
	// Used to send Pulsar messages when, e.g., executors report a job has finished.
	producer pulsar.Producer
	// Interface to the component storing job information, such as which jobs are leased to a particular executor.
	jobRepository database.JobRepository
	// Interface to the component storing executor information, such as which when we last heard from an executor.
	executorRepository database.ExecutorRepository
	// Like executorRepository
	legacyExecutorRepository database.ExecutorRepository
	// Allowed priority class priorities.
	allowedPriorities []int32
	// Max size of Pulsar messages produced.
	maxPulsarMessageSizeBytes uint
	// See scheduling schedulingConfig.
	nodeIdLabel string
	// See scheduling schedulingConfig.
	priorityClassNameOverride *string
	clock                     clock.Clock
}

func NewExecutorApi(producer pulsar.Producer,
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	legacyExecutorRepository database.ExecutorRepository,
	allowedPriorities []int32,
	nodeIdLabel string,
	priorityClassNameOverride *string,
	maxPulsarMessageSizeBytes uint,
) (*ExecutorApi, error) {
	if len(allowedPriorities) == 0 {
		return nil, errors.New("allowedPriorities cannot be empty")
	}
	return &ExecutorApi{
		producer:                  producer,
		jobRepository:             jobRepository,
		executorRepository:        executorRepository,
		legacyExecutorRepository:  legacyExecutorRepository,
		allowedPriorities:         allowedPriorities,
		maxPulsarMessageSizeBytes: maxPulsarMessageSizeBytes,
		nodeIdLabel:               nodeIdLabel,
		priorityClassNameOverride: priorityClassNameOverride,
		clock:                     clock.RealClock{},
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

	executor := srv.executorFromLeaseRequest(ctx, req)
	if err := srv.executorRepository.StoreExecutor(ctx, executor); err != nil {
		return err
	}
	if err = srv.legacyExecutorRepository.StoreExecutor(ctx, executor); err != nil {
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
		"executor currently has %d job runs; sending %d cancellations and %d new runs",
		len(requestRuns), len(runsToCancel), len(newRuns),
	)

	// Send any runs that should be cancelled.
	if len(runsToCancel) > 0 {
		if err := stream.Send(&executorapi.LeaseStreamMessage{
			Event: &executorapi.LeaseStreamMessage_CancelRuns{
				CancelRuns: &executorapi.CancelRuns{
					JobRunIdsToCancel: util.Map(runsToCancel, func(x uuid.UUID) *armadaevents.Uuid {
						return armadaevents.ProtoUuidFromUuid(x)
					}),
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

		if srv.priorityClassNameOverride != nil {
			srv.setPriorityClassName(submitMsg, *srv.priorityClassNameOverride)
		}

		srv.addNodeIdSelector(submitMsg, lease.Node)

		if len(lease.PodRequirementsOverlay) > 0 {
			PodRequirementsOverlay := schedulerobjects.PodRequirements{}
			if err := proto.Unmarshal(lease.PodRequirementsOverlay, &PodRequirementsOverlay); err != nil {
				return err
			}
			addTolerations(submitMsg, PodRequirementsOverlay.Tolerations)
			addAnnotations(submitMsg, PodRequirementsOverlay.Annotations)
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
					JobRunId: armadaevents.ProtoUuidFromUuid(lease.RunID),
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

func addTolerations(job *armadaevents.SubmitJob, tolerations []v1.Toleration) {
	if job == nil || len(tolerations) == 0 {
		return
	}
	if job.MainObject != nil {
		switch typed := job.MainObject.Object.(type) {
		case *armadaevents.KubernetesMainObject_PodSpec:
			if typed.PodSpec != nil && typed.PodSpec.PodSpec != nil {
				typed.PodSpec.PodSpec.Tolerations = append(typed.PodSpec.PodSpec.Tolerations, tolerations...)
			}
		}
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
	err := pulsarutils.CompactAndPublishSequences(ctx, list.Events, srv.producer, srv.maxPulsarMessageSizeBytes, schedulers.Pulsar)
	return &types.Empty{}, err
}

// executorFromLeaseRequest extracts a schedulerobjects.Executor from the request.
func (srv *ExecutorApi) executorFromLeaseRequest(ctx *armadacontext.Context, req *executorapi.LeaseRequest) *schedulerobjects.Executor {
	nodes := make([]*schedulerobjects.Node, 0, len(req.Nodes))
	now := srv.clock.Now().UTC()
	for _, nodeInfo := range req.Nodes {
		if node, err := executorapi.NewNodeFromNodeInfo(nodeInfo, req.ExecutorId, srv.allowedPriorities, now); err != nil {
			logging.WithStacktrace(ctx, err).Warnf(
				"skipping node %s from executor %s", nodeInfo.GetName(), req.GetExecutorId(),
			)
		} else {
			nodes = append(nodes, node)
		}
	}
	return &schedulerobjects.Executor{
		Id:             req.ExecutorId,
		Pool:           req.Pool,
		Nodes:          nodes,
		MinimumJobSize: schedulerobjects.ResourceList{Resources: req.MinimumJobSize},
		LastUpdateTime: now,
		UnassignedJobRuns: util.Map(req.UnassignedJobRunIds, func(jobId armadaevents.Uuid) string {
			return strings.ToLower(armadaevents.UuidFromProtoUuid(&jobId).String())
		}),
	}
}

// runIdsFromLeaseRequest returns the ids of all runs in a lease request, including any not yet assigned to a node.
func runIdsFromLeaseRequest(req *executorapi.LeaseRequest) ([]uuid.UUID, error) {
	runIds := make([]uuid.UUID, 0, 256)
	for _, node := range req.Nodes {
		for runIdStr := range node.RunIdsByState {
			if runId, err := uuid.Parse(runIdStr); err != nil {
				return nil, errors.WithStack(err)
			} else {
				runIds = append(runIds, runId)
			}
		}
	}
	for _, runId := range req.UnassignedJobRunIds {
		runIds = append(runIds, armadaevents.UuidFromProtoUuid(&runId))
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
