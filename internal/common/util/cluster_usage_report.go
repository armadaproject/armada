package util

import (
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
)

// GetClusterCapacity returns the total capacity on all nodes on a cluster,
// even if they are unschedulable.
func GetClusterCapacity(report *api.ClusterUsageReport) armadaresource.ComputeResources {
	result := armadaresource.ComputeResources{}
	if len(report.NodeTypeUsageReports) > 0 {
		for _, nodeTypeReport := range report.NodeTypeUsageReports {
			result.Add(nodeTypeReport.Capacity)
		}
	} else {
		result = report.ClusterCapacity
	}
	return result
}

// GetClusterAvailableCapacity returns the total resource to be shared amongst queues.
// This is the total capacity available to armada on schedulable nodes + the capacity currently
// in use on unschedulable nodes.
func GetClusterAvailableCapacity(report *api.ClusterUsageReport) armadaresource.ComputeResources {
	result := armadaresource.ComputeResources{}
	if len(report.NodeTypeUsageReports) > 0 {
		for _, nodeTypeReport := range report.NodeTypeUsageReports {
			result.Add(nodeTypeReport.AvailableCapacity)
			result.Add(nodeTypeReport.CordonedUsage)
		}
	} else {
		result = report.ClusterAvailableCapacity
	}
	return result
}

func SumReportClusterCapacity(reports map[string]*api.ClusterUsageReport) armadaresource.ComputeResources {
	result := armadaresource.ComputeResources{}
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
