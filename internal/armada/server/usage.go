package server

import (
	"context"
	"time"

	"github.com/gogo/protobuf/types"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
)

type UsageServer struct {
	permissions      authorization.PermissionChecker
	priorityHalfTime time.Duration
	usageRepository  repository.UsageRepository
}

func NewUsageServer(
	permissions authorization.PermissionChecker,
	priorityHalfTime time.Duration,
	usageRepository repository.UsageRepository) *UsageServer {

	return &UsageServer{
		permissions:      permissions,
		priorityHalfTime: priorityHalfTime,
		usageRepository:  usageRepository}
}

func (s *UsageServer) ReportUsage(ctx context.Context, report *api.ClusterUsageReport) (*types.Empty, error) {
	if e := checkPermission(s.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
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

	err = s.usageRepository.UpdateCluster(report, newPriority)
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}
