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

func FilterActiveClusterNodeInfoReports(reports map[string]*api.ClusterNodeInfoReport) map[string]*api.ClusterNodeInfoReport {
	result := map[string]*api.ClusterNodeInfoReport{}
	now := time.Now()
	for id, report := range reports {
		if report.ReportTime.Add(recentlyActiveClusterExpiry).After(now) {
			result[id] = report
		}
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
