package scheduling

import (
	"time"

	"github.com/G-Research/armada/internal/armada/api"
)

const activeClusterExpiry = 10 * time.Minute

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

func GetClusterReportIds(reports map[string]*api.ClusterUsageReport) []string {
	var result []string
	for id := range reports {
		result = append(result, id)
	}
	return result
}
