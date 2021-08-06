package util

import (
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func GetClusterCapacity(report *api.ClusterUsageReport) common.ComputeResources {
	result := common.ComputeResources{}
	if len(report.NodeTypeUsageReports) > 0 {
		for _, nodeTypeReport := range report.NodeTypeUsageReports {
			result.Add(nodeTypeReport.Capacity)
		}
	} else {
		result = report.ClusterCapacity
	}
	return result
}

func GetClusterAvailableCapacity(report *api.ClusterUsageReport) common.ComputeResources {
	result := common.ComputeResources{}
	if len(report.NodeTypeUsageReports) > 0 {
		for _, nodeTypeReport := range report.NodeTypeUsageReports {
			result.Add(nodeTypeReport.AvailableCapacity)
		}
	} else {
		result = report.ClusterAvailableCapacity
	}
	return result
}

func SumReportClusterCapacity(reports map[string]*api.ClusterUsageReport) common.ComputeResources {
	result := common.ComputeResources{}
	for _, report := range reports {
		result.Add(GetClusterCapacity(report))
	}
	return result
}

func GetQueueReports(report *api.ClusterUsageReport) []*api.QueueReport {
	result := make([]*api.QueueReport, 0, 10)
	if len(report.NodeTypeUsageReports) > 0 {
		for _, nodeTypeReport := range report.NodeTypeUsageReports {
			result = append(result, nodeTypeReport.Queues...)
		}
	} else if len(report.Queues) > 0 {
		result = append(result, report.Queues...)
	}
	return result
}
