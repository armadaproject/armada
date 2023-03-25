package scheduling

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api"
)

func TestFilterActiveClusters(t *testing.T) {
	clusterUsageReportInput := getClusterUsageReportInput()
	clusterUsageReportInput["cluster-1"].ReportTime = time.Now()
	clusterUsageReportInput["cluster-2"].ReportTime = time.Now().Add(time.Duration(-activeClusterExpiry))
	result := FilterActiveClusters(clusterUsageReportInput)
	assert.NotNil(t, result["cluster-1"])
	assert.Nil(t, result["cluster-2"])
}

func TestFilterPoolClusters(t *testing.T) {
	result := FilterPoolClusters("pool-1", getClusterUsageReportInput())
	assert.NotNil(t, result["cluster-1"])
	assert.Nil(t, result["cluster-3"])
}

func TestGroupByPool(t *testing.T) {
	result := GroupByPool(getClusterUsageReportInput())
	expected := map[string]map[string]*api.ClusterUsageReport{
		"pool-1": {
			"cluster-1": {
				ClusterId: "cluster-1",
				Pool:      "pool-1",
			},
			"cluster-2": {
				ClusterId: "cluster-2",
				Pool:      "pool-1",
			},
		},
		"pool-2": {
			"cluster-3": {
				ClusterId: "cluster-3",
				Pool:      "pool-2",
			},
			"cluster-4": {
				ClusterId: "cluster-4",
				Pool:      "pool-2",
			},
		},
	}
	assert.Equal(t, expected, result)
}

func TestFilterClusterLeasedReport(t *testing.T) {
	ids := []string{"cluster-2"}
	clusterLeasedReportInput := map[string]*api.ClusterLeasedReport{
		"cluster-1": {
			ClusterId: "cluster-1",
		},
		"cluster-2": {
			ClusterId: "cluster-2",
		},
	}
	result := FilterClusterLeasedReports(ids, clusterLeasedReportInput)
	assert.NotNil(t, result["cluster-2"])
	assert.Nil(t, result["cluster-1"])
}

func TestFilterActiveClusterSchedulingInfoReports(t *testing.T) {
	clusterSchedulingInfoReportInput := getClusterSchedulingInfoReportInput()
	clusterSchedulingInfoReportInput["cluster-1"].ReportTime = time.Now()
	clusterSchedulingInfoReportInput["cluster-2"].ReportTime = time.Now().Add(time.Duration(-recentlyActiveClusterExpiry))
	result := FilterActiveClusterSchedulingInfoReports(clusterSchedulingInfoReportInput)
	assert.NotNil(t, result["cluster-1"])
	assert.Nil(t, result["cluster-2"])
}

func TestGroupSchedulingInfoByPool(t *testing.T) {
	result := GroupSchedulingInfoByPool(getClusterSchedulingInfoReportInput())
	expected := map[string]map[string]*api.ClusterSchedulingInfoReport{
		"pool-1": {
			"cluster-1": {
				ClusterId: "cluster-1",
				Pool:      "pool-1",
			},
			"cluster-2": {
				ClusterId: "cluster-2",
				Pool:      "pool-1",
			},
		},
		"pool-2": {
			"cluster-3": {
				ClusterId: "cluster-3",
				Pool:      "pool-2",
			},
			"cluster-4": {
				ClusterId: "cluster-4",
				Pool:      "pool-2",
			},
		},
	}
	assert.Equal(t, expected, result)
}

func TestGetClusterReportsIds(t *testing.T) {
	expected := []string{"cluster-1", "cluster-2", "cluster-3", "cluster-4"}
	result := GetClusterReportIds(getClusterUsageReportInput())
	sort.Strings(result)
	assert.Equal(t, expected, result)
}

func getClusterUsageReportInput() map[string]*api.ClusterUsageReport {
	return map[string]*api.ClusterUsageReport{
		"cluster-1": {
			ClusterId: "cluster-1",
			Pool:      "pool-1",
		},
		"cluster-2": {
			ClusterId: "cluster-2",
			Pool:      "pool-1",
		},
		"cluster-3": {
			ClusterId: "cluster-3",
			Pool:      "pool-2",
		},
		"cluster-4": {
			ClusterId: "cluster-4",
			Pool:      "pool-2",
		},
	}
}

func getClusterSchedulingInfoReportInput() map[string]*api.ClusterSchedulingInfoReport {
	return map[string]*api.ClusterSchedulingInfoReport{
		"cluster-1": {
			ClusterId: "cluster-1",
			Pool:      "pool-1",
		},
		"cluster-2": {
			ClusterId: "cluster-2",
			Pool:      "pool-1",
		},
		"cluster-3": {
			ClusterId: "cluster-3",
			Pool:      "pool-2",
		},
		"cluster-4": {
			ClusterId: "cluster-4",
			Pool:      "pool-2",
		},
	}
}
