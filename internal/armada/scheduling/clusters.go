package scheduling

import (
	"time"

	"github.com/G-Research/armada/pkg/api"
)

const activeClusterExpiry = 10 * time.Minute
const recentlyActiveClusterExpiry = 60 * time.Minute

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

func FilterPoolClusters(pool string, reports map[string]*api.ClusterUsageReport) map[string]*api.ClusterUsageReport {
	result := map[string]*api.ClusterUsageReport{}
	for id, report := range reports {
		if report.Pool == pool {
			result[id] = report
		}
	}
	return result
}

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

func FilterActiveClusterLeasedReports(reports map[string]*api.ClusterLeasedReport) map[string]*api.ClusterLeasedReport {
	result := map[string]*api.ClusterLeasedReport{}
	now := time.Now()
	for id, report := range reports {
		if report.ReportTime.Add(activeClusterExpiry).After(now) {
			result[id] = report
		}
	}
	return result
}

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

func GetClusterReportIds(reports map[string]*api.ClusterUsageReport) []string {
	var result []string
	for id := range reports {
		result = append(result, id)
	}
	return result
}
