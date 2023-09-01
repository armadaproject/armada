package server

import (
	gocontext "context"
	"github.com/armadaproject/armada/internal/common/context"
	"time"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

type UsageServer struct {
	permissions      authorization.PermissionChecker
	priorityHalfTime time.Duration
	schedulingConfig *configuration.SchedulingConfig
	usageRepository  repository.UsageRepository
	queueRepository  repository.QueueRepository
}

func NewUsageServer(
	permissions authorization.PermissionChecker,
	priorityHalfTime time.Duration,
	schedulingConfig *configuration.SchedulingConfig,
	usageRepository repository.UsageRepository,
	queueRepository repository.QueueRepository,
) *UsageServer {
	return &UsageServer{
		permissions:      permissions,
		priorityHalfTime: priorityHalfTime,
		schedulingConfig: schedulingConfig,
		usageRepository:  usageRepository,
		queueRepository:  queueRepository,
	}
}

func (s *UsageServer) ReportUsage(grpcCtx gocontext.Context, report *api.ClusterUsageReport) (*types.Empty, error) {
	ctx := context.FromGrpcContext(grpcCtx)
	if err := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[ReportUsage] error: %s", err)
	}

	queues, err := s.queueRepository.GetAllQueues()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[ReportUsage] error getting queues: %s", err)
	}

	reports, err := s.usageRepository.GetClusterUsageReports()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[ReportUsage] error getting cluster usage reports: %s", err)
	}

	previousPriority, err := s.usageRepository.GetClusterPriority(report.ClusterId)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[ReportUsage] error getting cluster priority: %s", err)
	}

	previousReport := reports[report.ClusterId]

	resourceScarcity := s.schedulingConfig.GetResourceScarcity(report.Pool)
	if resourceScarcity == nil {
		reports[report.ClusterId] = report
		activeClusterReports := scheduling.FilterActiveClusters(reports)
		activePoolClusterReports := scheduling.FilterPoolClusters(report.Pool, activeClusterReports)
		resourceScarcity = scheduling.ResourceScarcityFromReports(activePoolClusterReports)
	}
	newPriority := scheduling.CalculatePriorityUpdate(resourceScarcity, previousReport, report, previousPriority, s.priorityHalfTime)
	filteredPriority := filterPriority(queue.QueuesToAPI(queues), newPriority)

	err = s.usageRepository.UpdateCluster(report, filteredPriority)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[ReportUsage] error updating cluster: %s", err)
	}
	return &types.Empty{}, nil
}

func filterPriority(queues []*api.Queue, priority map[string]float64) map[string]float64 {
	filteredPriority := map[string]float64{}
	for _, q := range queues {
		priority, ok := priority[q.Name]
		if ok {
			filteredPriority[q.Name] = priority
		}
	}
	return filteredPriority
}
