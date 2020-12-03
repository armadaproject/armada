package server

import (
	"context"
	"time"

	"github.com/gogo/protobuf/types"

	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/pkg/api"
)

type UsageServer struct {
	permissions      authorization.PermissionChecker
	priorityHalfTime time.Duration
	usageRepository  repository.UsageRepository
	queueRepository  repository.QueueRepository
}

func NewUsageServer(
	permissions authorization.PermissionChecker,
	priorityHalfTime time.Duration,
	usageRepository repository.UsageRepository,
	queueRepository repository.QueueRepository) *UsageServer {

	return &UsageServer{
		permissions:      permissions,
		priorityHalfTime: priorityHalfTime,
		usageRepository:  usageRepository,
		queueRepository:  queueRepository}
}

func (s *UsageServer) ReportUsage(ctx context.Context, report *api.ClusterUsageReport) (*types.Empty, error) {
	if e := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}

	queues, err := s.queueRepository.GetAllQueues()
	if err != nil {
		return nil, err
	}

	reports, err := s.usageRepository.GetClusterUsageReports()
	if err != nil {
		return nil, err
	}

	previousPriority, err := s.usageRepository.GetClusterPriority(report.ClusterId)
	if err != nil {
		return nil, err
	}

	newPriority := scheduling.CalculatePriorityUpdateFromReports(reports, report, previousPriority, s.priorityHalfTime)
	filteredPriority := filterPriority(queues, newPriority)

	err = s.usageRepository.UpdateCluster(report, filteredPriority)
	if err != nil {
		return nil, err
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
