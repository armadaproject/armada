package scheduling

import (
	"math"
	"time"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

type QueueSchedulingInfo struct {
	remainingSchedulingLimit common.ComputeResourcesFloat
	schedulingShare          common.ComputeResourcesFloat
	adjustedShare            common.ComputeResourcesFloat
}

func NewQueueSchedulingInfo(
	remainingSchedulingLimit common.ComputeResourcesFloat,
	schedulingShare common.ComputeResourcesFloat,
	adjustedShare common.ComputeResourcesFloat,
) *QueueSchedulingInfo {
	return &QueueSchedulingInfo{
		remainingSchedulingLimit: remainingSchedulingLimit.DeepCopy(),
		schedulingShare:          schedulingShare.DeepCopy(),
		adjustedShare:            adjustedShare.DeepCopy(),
	}
}

func (info *QueueSchedulingInfo) UpdateLimits(resourceUsed common.ComputeResourcesFloat) {
	schedulingShareScaled := info.schedulingShare.DeepCopy()
	for key, schedulingShareOfResource := range info.schedulingShare {
		allocated, used := resourceUsed[key]
		if used {
			adjustedShareOfResource := info.adjustedShare[key]
			scalingFactor := 0.0
			if adjustedShareOfResource > 0 {
				scalingFactor = schedulingShareOfResource / adjustedShareOfResource
			}
			scaledValue := allocated * scalingFactor

			schedulingShareScaled[key] = math.Max(schedulingShareScaled[key]-scaledValue, 0)
		}
	}
	info.remainingSchedulingLimit.Sub(resourceUsed)
	info.remainingSchedulingLimit.LimitToZero()
	info.schedulingShare = schedulingShareScaled
	info.schedulingShare.LimitToZero()
	info.adjustedShare.Sub(resourceUsed)
	info.adjustedShare.LimitToZero()
}

func SliceResourceWithLimits(resourceScarcity map[string]float64, queueSchedulingInfo map[*api.Queue]*QueueSchedulingInfo, queuePriorities map[*api.Queue]QueuePriorityInfo, quantityToSlice common.ComputeResourcesFloat) map[*api.Queue]*QueueSchedulingInfo {
	queuesWithCapacity := filterQueuesWithNoCapacity(queueSchedulingInfo, queuePriorities)
	naiveSlicedResource := sliceResource(resourceScarcity, queuesWithCapacity, quantityToSlice)

	result := map[*api.Queue]*QueueSchedulingInfo{}
	for queue, slice := range naiveSlicedResource {
		schedulingInfo := queueSchedulingInfo[queue]
		adjustedSlice := slice.DeepCopy()
		adjustedSlice = adjustedSlice.LimitWith(schedulingInfo.remainingSchedulingLimit)
		result[queue] = NewQueueSchedulingInfo(schedulingInfo.remainingSchedulingLimit, slice, adjustedSlice)
	}

	return result
}

func filterQueuesWithNoCapacity(queueSchedulingInfo map[*api.Queue]*QueueSchedulingInfo, queuePriorities map[*api.Queue]QueuePriorityInfo) map[*api.Queue]QueuePriorityInfo {
	queuesWithCapacity := map[*api.Queue]QueuePriorityInfo{}
	for queue, info := range queueSchedulingInfo {
		for _, resource := range info.remainingSchedulingLimit {
			if resource > 0 {
				queuesWithCapacity[queue] = queuePriorities[queue]
				break
			}
		}
	}
	return queuesWithCapacity
}

func sliceResource(resourceScarcity map[string]float64, queuePriorities map[*api.Queue]QueuePriorityInfo, quantityToSlice common.ComputeResourcesFloat) map[*api.Queue]common.ComputeResourcesFloat {
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
		scarcity := util.GetOrDefault(resourceScarcity, resourceName, 0)
		usage += common.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

func ResourcesFloatAsUsage(resourceScarcity map[string]float64, resources common.ComputeResourcesFloat) float64 {
	usage := 0.0
	for resourceName, quantity := range resources {
		scarcity := util.GetOrDefault(resourceScarcity, resourceName, 0)
		usage += quantity * scarcity
	}
	return usage
}

func QueueSlicesToShares(resourceScarcity map[string]float64, schedulingInfo map[*api.Queue]*QueueSchedulingInfo) map[*api.Queue]float64 {
	shares := map[*api.Queue]float64{}
	for queue, info := range schedulingInfo {
		shares[queue] = ResourcesFloatAsUsage(resourceScarcity, info.schedulingShare)
	}
	return shares
}

func SumRemainingResource(schedulingInfo map[*api.Queue]*QueueSchedulingInfo) common.ComputeResourcesFloat {
	sum := common.ComputeResourcesFloat{}
	for _, info := range schedulingInfo {
		sum.Add(info.adjustedShare)
	}
	return sum
}

func ResourceScarcityFromReports(reports map[string]*api.ClusterUsageReport) map[string]float64 {
	availableResources := util.SumReportClusterCapacity(reports)
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
	resourceUsageByQueue := map[string]common.ComputeResources{}
	for _, queueReport := range queues {
		if _, present := resourceUsageByQueue[queueReport.Name]; !present {
			resourceUsageByQueue[queueReport.Name] = common.ComputeResources{}
		}
		resourceUsageByQueue[queueReport.Name].Add(queueReport.Resources)
	}

	usages := map[string]float64{}
	for queueName, resourceRequest := range resourceUsageByQueue {
		usages[queueName] = ResourcesAsUsage(resourceScarcity, resourceRequest)
	}
	return usages
}

func CombineLeasedReportResourceByQueue(reports map[string]*api.ClusterLeasedReport) map[string]common.ComputeResources {
	resourceLeasedByQueue := map[string]common.ComputeResources{}
	for _, clusterReport := range reports {
		for _, queueReport := range clusterReport.Queues {
			if _, ok := resourceLeasedByQueue[queueReport.Name]; !ok {
				resourceLeasedByQueue[queueReport.Name] = queueReport.ResourcesLeased
			} else {
				resourceLeasedByQueue[queueReport.Name].Add(queueReport.ResourcesLeased)
			}
		}
	}
	return resourceLeasedByQueue
}

func CreateClusterLeasedReport(clusterId string, currentReport *api.ClusterLeasedReport, additionallyLeasedJobs []*api.Job) *api.ClusterLeasedReport {
	leasedResourceByQueue := CombineLeasedReportResourceByQueue(map[string]*api.ClusterLeasedReport{
		clusterId: currentReport,
	})
	for _, job := range additionallyLeasedJobs {
		if _, ok := leasedResourceByQueue[job.Queue]; !ok {
			leasedResourceByQueue[job.Queue] = common.TotalJobResourceRequest(job)
		} else {
			leasedResourceByQueue[job.Queue].Add(common.TotalJobResourceRequest(job))
		}
	}
	leasedQueueReports := make([]*api.QueueLeasedReport, 0, len(leasedResourceByQueue))
	for queueName, leasedResource := range leasedResourceByQueue {
		leasedQueueReport := &api.QueueLeasedReport{
			Name:            queueName,
			ResourcesLeased: leasedResource,
		}
		leasedQueueReports = append(leasedQueueReports, leasedQueueReport)
	}

	clusterLeasedReport := api.ClusterLeasedReport{
		ClusterId:  clusterId,
		ReportTime: time.Now(),
		Queues:     leasedQueueReports,
	}
	return &clusterLeasedReport
}
