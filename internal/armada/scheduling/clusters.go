package scheduling

import (
	"time"

	"github.com/armadaproject/armada/pkg/api"
)

// Each executor periodically reports cluster resource usage to the server.
// A cluster is considered inactive if the most recent such report is older than this amount of time.
const activeClusterExpiry = 10 * time.Minute

// Each executor periodically sends a list of all nodes in its cluster to the server.
// These lists are used by the scheduler and are considered valid for this amount of time.
const recentlyActiveClusterExpiry = 60 * time.Minute

// FilterActiveClusters returns the subset of reports corresponding to active clusters.
// A cluster is considered active if the most recent ClusterUsageReport was received less than activeClusterExpiry ago.
func FilterActiveClusters(reports map[string]*api.ClusterUsageReport) map[string]*api.ClusterUsageReport {
	result := map[string]*api.ClusterUsageReport{}
	now := time.Now()
	for id, report := range reports {
		if report.ReportTime.Add(activeClusterExpiry).After(now) {
			result[id] = report
		}
	}
	return result
}

// FilterPoolClusters returns the subset of reports for which the pool has a specific value.
func FilterPoolClusters(pool string, reports map[string]*api.ClusterUsageReport) map[string]*api.ClusterUsageReport {
	result := map[string]*api.ClusterUsageReport{}
	for id, report := range reports {
		if report.Pool == pool {
			result[id] = report
		}
	}
	return result
}

// GroupByPool returns a map from pool name to another map, which in turn maps report ids to reports.
func GroupByPool(reports map[string]*api.ClusterUsageReport) map[string]map[string]*api.ClusterUsageReport {
	result := map[string]map[string]*api.ClusterUsageReport{}
	for id, report := range reports {
		poolReports, ok := result[report.Pool]
		if !ok {
			poolReports = map[string]*api.ClusterUsageReport{}
			result[report.Pool] = poolReports
		}
		poolReports[id] = report
	}
	return result
}

// FilterClusterLeasedReports returns the subset of reports with id in the provided slice of ids.
// ids for which there is no corresponding report are ignored.
func FilterClusterLeasedReports(ids []string, reports map[string]*api.ClusterLeasedReport) map[string]*api.ClusterLeasedReport {
	result := map[string]*api.ClusterLeasedReport{}
	for _, id := range ids {
		if report, ok := reports[id]; ok {
			result[id] = report
		}
	}
	return result
}

// FilterActiveClusterSchedulingInfoReports returns the subset of reports within the expiry time.
func FilterActiveClusterSchedulingInfoReports(reports map[string]*api.ClusterSchedulingInfoReport) map[string]*api.ClusterSchedulingInfoReport {
	result := map[string]*api.ClusterSchedulingInfoReport{}
	now := time.Now()
	for id, report := range reports {
		if report.ReportTime.Add(recentlyActiveClusterExpiry).After(now) {
			result[id] = report
		}
	}
	return result
}

// GroupSchedulingInfoByPool returns a map from pool name to another map, which in turn maps report ids to reports.
func GroupSchedulingInfoByPool(reports map[string]*api.ClusterSchedulingInfoReport) map[string]map[string]*api.ClusterSchedulingInfoReport {
	result := map[string]map[string]*api.ClusterSchedulingInfoReport{}
	for id, report := range reports {
		poolReports, ok := result[report.Pool]
		if !ok {
			poolReports = map[string]*api.ClusterSchedulingInfoReport{}
			result[report.Pool] = poolReports
		}
		poolReports[id] = report
	}
	return result
}

// GetClusterReportIds returns a slice composed of all unique report ids in the provided map.
func GetClusterReportIds(reports map[string]*api.ClusterUsageReport) []string {
	var result []string
	for id := range reports {
		result = append(result, id)
	}
	return result
}
