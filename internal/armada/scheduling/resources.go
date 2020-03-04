package scheduling

import (
	"math"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
)

func SliceResource(resourceScarcity map[string]float64, queuePriorities map[*api.Queue]QueuePriorityInfo, quantityToSlice common.ComputeResourcesFloat) map[*api.Queue]common.ComputeResourcesFloat {

	inversePriorities := make(map[*api.Queue]float64)
	inverseSum := 0.0

	usages := make(map[*api.Queue]float64)
	allCurrentUsage := 0.0

	for queue, info := range queuePriorities {
		inverse := 1 / info.Priority
		inversePriorities[queue] = inverse
		inverseSum += inverse

		queueUsage := ResourcesAsUsage(resourceScarcity, info.CurrentUsage)
		usages[queue] = queueUsage
		allCurrentUsage += queueUsage
	}

	usageToSlice := ResourcesFloatAsUsage(resourceScarcity, quantityToSlice)
	allUsage := usageToSlice + allCurrentUsage

	shares := make(map[*api.Queue]float64)
	shareSum := 0.0
	for queue, inverse := range inversePriorities {
		share := math.Max(0, allUsage*(inverse/inverseSum)-usages[queue])
		shareSum += share
		shares[queue] = share
	}

	shareResources := make(map[*api.Queue]common.ComputeResourcesFloat)
	for queue, share := range shares {
		shareResources[queue] = quantityToSlice.Mul(share / shareSum)
	}
	return shareResources
}

func ResourcesAsUsage(resourceScarcity map[string]float64, resources common.ComputeResources) float64 {
	usage := 0.0
	for resourceName, quantity := range resources {
		scarcity := util.GetOrDefault(resourceScarcity, resourceName, 1)
		usage += common.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

func ResourcesFloatAsUsage(resourceScarcity map[string]float64, resources common.ComputeResourcesFloat) float64 {
	usage := 0.0
	for resourceName, quantity := range resources {
		scarcity := util.GetOrDefault(resourceScarcity, resourceName, 1)
		usage += quantity * scarcity
	}
	return usage
}

func QueueSlicesToShares(resourceScarcity map[string]float64, slices map[*api.Queue]common.ComputeResourcesFloat) map[*api.Queue]float64 {
	shares := map[*api.Queue]float64{}
	for queue, slice := range slices {
		shares[queue] = ResourcesFloatAsUsage(resourceScarcity, slice)
	}
	return shares
}

func SumQueueSlices(slices map[*api.Queue]common.ComputeResourcesFloat) common.ComputeResourcesFloat {
	sum := common.ComputeResourcesFloat{}
	for _, slice := range slices {
		sum.Add(slice)
	}
	return sum
}

func ResourceScarcityFromReports(reports map[string]*api.ClusterUsageReport) map[string]float64 {
	availableResources := sumReportResources(reports)
	return calculateResourceScarcity(availableResources.AsFloat())
}

// Calculates inverse of resources per cpu unit
// { cpu: 4, memory: 20GB, gpu: 2 } -> { cpu: 1.0, memory: 0.2, gpu: 2 }
func calculateResourceScarcity(res common.ComputeResourcesFloat) map[string]float64 {
	importance := map[string]float64{
		"cpu": 1,
	}
	cpu := res["cpu"]

	for k, q := range res {
		if k == "cpu" {
			continue
		}
		if q >= 0.00001 {
			importance[k] = cpu / q
		}
	}
	return importance
}

func usageFromQueueReports(resourceScarcity map[string]float64, queues []*api.QueueReport) map[string]float64 {
	usages := map[string]float64{}
	for _, queue := range queues {
		usages[queue.Name] = ResourcesAsUsage(resourceScarcity, queue.Resources)
	}
	return usages
}

func sumReportResources(reports map[string]*api.ClusterUsageReport) common.ComputeResources {
	result := common.ComputeResources{}
	for _, report := range reports {
		result.Add(report.ClusterCapacity)
	}
	return result
}
