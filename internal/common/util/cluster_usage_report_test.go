package util

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
)

func TestGetClusterAvailableCapacity(t *testing.T) {
	capacity := common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	availability := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	clusterReport := createClusterUsageReport(availability, capacity, 2)

	expected := common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	result := GetClusterAvailableCapacity(clusterReport)
	assert.True(t, result.Equal(expected))
}

func TestGetClusterAvailableCapacity_WhenUsingDeprecatedFields(t *testing.T) {
	availability := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	clusterReport := &api.ClusterUsageReport{
		ClusterAvailableCapacity: availability,
	}

	result := GetClusterAvailableCapacity(clusterReport)
	assert.True(t, result.Equal(availability))
}

func TestGetClusterCapacity(t *testing.T) {
	availability := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	capacity := common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	clusterReport := createClusterUsageReport(availability, capacity, 2)

	expected := common.ComputeResources{"cpu": resource.MustParse("4"), "memory": resource.MustParse("4Gi")}
	result := GetClusterCapacity(clusterReport)
	assert.True(t, result.Equal(expected))
}

func TestGetClusterCapacity_WhenNodesCordoned(t *testing.T) {
	runningJob := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	totalCap := common.ComputeResources{"cpu": resource.MustParse("10"), "memory": resource.MustParse("10Gi")}
	nodeReport := createNodeInfoUsageReport("test", common.ComputeResources{}, totalCap)
	nodeReport.CordonedUsage = runningJob.DeepCopy()

	report := api.ClusterUsageReport{
		NodeTypeUsageReports: []api.NodeTypeUsageReport{nodeReport},
	}

	assert.Equal(t, runningJob, GetClusterAvailableCapacity(&report))
}

func TestGetClusterCapacity_WhenNodesMixed(t *testing.T) {
	runningJob := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	totalCap := common.ComputeResources{"cpu": resource.MustParse("20"), "memory": resource.MustParse("20Gi")}
	uncordoned := common.ComputeResources{"cpu": resource.MustParse("10"), "memory": resource.MustParse("10Gi")}
	nodeReport := createNodeInfoUsageReport("test", uncordoned, totalCap)
	nodeReport.CordonedUsage = runningJob.DeepCopy()

	report := api.ClusterUsageReport{
		NodeTypeUsageReports: []api.NodeTypeUsageReport{nodeReport},
	}

	expected := common.ComputeResources{}
	expected.Add(runningJob)
	expected.Add(uncordoned)

	assert.Equal(t, expected, GetClusterAvailableCapacity(&report))
}

func TestGetClusterCapacity_WhenUsingDeprecatedFields(t *testing.T) {
	capacity := common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	clusterReport := &api.ClusterUsageReport{
		ClusterCapacity: capacity,
	}

	result := GetClusterCapacity(clusterReport)
	assert.True(t, result.Equal(capacity))
}

func TestSumReportClusterCapacity(t *testing.T) {
	capacity := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	clusterReports := map[string]*api.ClusterUsageReport{
		"cluster1": createClusterUsageReport(capacity, capacity, 1),
		"cluster2": createClusterUsageReport(capacity, capacity, 2),
		"cluster3": createClusterUsageReport(capacity, capacity, 3),
	}

	expected := common.ComputeResources{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")}
	result := SumReportClusterCapacity(clusterReports)
	assert.True(t, result.Equal(expected))
}

func TestSumReportClusterCapacity_WhenEmpty(t *testing.T) {
	clusterReports := map[string]*api.ClusterUsageReport{}

	expected := common.ComputeResources{}
	result := SumReportClusterCapacity(clusterReports)
	assert.True(t, result.Equal(expected))
}

func TestGetQueueReports(t *testing.T) {
	allocated1 := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	allocated2 := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	queueReports1 := createQueueReports(allocated1, allocated1, "queue1", "queue2")
	queueReports2 := createQueueReports(allocated2, allocated2, "queue1", "queue3")
	nodeInfos := []api.NodeTypeUsageReport{
		{
			Queues: queueReports1,
		},
		{
			Queues: queueReports2,
		},
	}
	clusterReport := &api.ClusterUsageReport{
		NodeTypeUsageReports: nodeInfos,
	}

	result := GetQueueReports(clusterReport)
	assert.Equal(t, len(result), 4)
	for _, report := range append(queueReports1, queueReports2...) {
		exists := false
		for _, returnedReport := range result {
			if report == returnedReport {
				exists = true
				break
			}
		}
		assert.True(t, exists)
	}
}

func TestGetQueueReports_WhenUsingDeprecatedFields(t *testing.T) {
	allocated1 := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	queueReports1 := createQueueReports(allocated1, allocated1, "queue1", "queue2")
	clusterReport := &api.ClusterUsageReport{
		Queues: queueReports1,
	}

	result := GetQueueReports(clusterReport)
	assert.Equal(t, result, queueReports1)
}

func createClusterUsageReport(available common.ComputeResources, capacity common.ComputeResources, numberOfNodeTypes int) *api.ClusterUsageReport {
	nodeTypeUsageReports := make([]api.NodeTypeUsageReport, 0, numberOfNodeTypes)
	for i := 0; i < numberOfNodeTypes; i++ {
		nodeTypeUsageReports = append(nodeTypeUsageReports, createNodeInfoUsageReport("nodeType"+strconv.Itoa(numberOfNodeTypes), available, capacity))
	}
	return &api.ClusterUsageReport{
		NodeTypeUsageReports: nodeTypeUsageReports,
	}
}

func createQueueReports(resourceAllocated common.ComputeResources, resourceUsed common.ComputeResources, queueNames ...string) []*api.QueueReport {
	result := []*api.QueueReport{}
	for _, name := range queueNames {
		result = append(result,
			&api.QueueReport{
				Name:          name,
				Resources:     resourceAllocated.DeepCopy(),
				ResourcesUsed: resourceUsed.DeepCopy(),
			})
	}
	return result
}

func createNodeInfoUsageReport(nodeType string, available common.ComputeResources, capacity common.ComputeResources) api.NodeTypeUsageReport {
	return api.NodeTypeUsageReport{
		NodeType: &api.NodeTypeIdentifier{
			Id:     nodeType,
			Taints: []v1.Taint{},
		},
		Capacity:          capacity.DeepCopy(),
		AvailableCapacity: available.DeepCopy(),
		Queues:            []*api.QueueReport{},
	}
}
