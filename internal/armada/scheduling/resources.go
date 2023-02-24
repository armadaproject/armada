package scheduling

import (
	"time"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

func ResourcesAsUsage(resourceScarcity map[string]float64, resources armadaresource.ComputeResources) float64 {
	usage := 0.0
	for resourceName, quantity := range resources {
		scarcity := util.GetOrDefault(resourceScarcity, resourceName, 0)
		usage += armadaresource.QuantityAsFloat64(quantity) * scarcity
	}
	return usage
}

func ResourceScarcityFromReports(reports map[string]*api.ClusterUsageReport) map[string]float64 {
	availableResources := util.SumReportClusterCapacity(reports)
	return calculateResourceScarcity(availableResources.AsFloat())
}

// Calculates inverse of resources per cpu unit
// { cpu: 4, memory: 20GB, gpu: 2 } -> { cpu: 1.0, memory: 0.2, gpu: 2 }
func calculateResourceScarcity(res armadaresource.ComputeResourcesFloat) map[string]float64 {
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
	resourceUsageByQueue := map[string]armadaresource.ComputeResources{}
	for _, queueReport := range queues {
		if _, present := resourceUsageByQueue[queueReport.Name]; !present {
			resourceUsageByQueue[queueReport.Name] = armadaresource.ComputeResources{}
		}
		resourceUsageByQueue[queueReport.Name].Add(queueReport.Resources)
	}

	usages := map[string]float64{}
	for queueName, resourceRequest := range resourceUsageByQueue {
		usages[queueName] = ResourcesAsUsage(resourceScarcity, resourceRequest)
	}
	return usages
}

func CombineLeasedReportResourceByQueue(reports map[string]*api.ClusterLeasedReport) map[string]armadaresource.ComputeResources {
	resourceLeasedByQueue := map[string]armadaresource.ComputeResources{}
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
			leasedResourceByQueue[job.Queue] = job.TotalResourceRequest()
		} else {
			leasedResourceByQueue[job.Queue].Add(job.TotalResourceRequest())
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
