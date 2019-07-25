
package server

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/gogo/protobuf/types"
	"math"
	"time"
)

type UsageServer struct {
	PriorityHalfTime time.Duration
	UsageRepository  repository.UsageRepository
}

func (s UsageServer) ReportUsage(ctx context.Context, report *api.ClusterUsageReport) (*types.Empty, error) {

	reports, err := s.UsageRepository.GetClusterUsageReports()
	if err != nil {
		return nil, err
	}

	previousPriority, err := s.UsageRepository.GetClusterPriority(report.ClusterId)
	if err != nil {
		return nil, err
	}

	previousReport := reports[report.ClusterId]
	timeChange := time.Minute
	if previousReport != nil {
		timeChange = report.ReportTime.Sub(previousReport.ReportTime)
	}

	reports[report.ClusterId] = report
	availableResources := sumResources(reports)
	resourceScarcity := calculateResourceScarcity(availableResources.AsFloat())
	usage := calculateUsage(resourceScarcity, report.Queues)
	newPriority := calculatePriority(usage, previousPriority, timeChange, s.PriorityHalfTime)

	err = s.UsageRepository.UpdateCluster(report, newPriority)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func calculatePriority(usage map[string]float64, previousPriority map[string]float64, timeChange time.Duration, halfTime time.Duration) map[string]float64 {

	newPriority := map[string]float64{}
	timeChangeFactor := math.Pow(0.5, timeChange.Seconds() / halfTime.Seconds())

	for queue, oldPriority := range previousPriority {
		newPriority[queue] = timeChangeFactor * util.GetOrDefault(usage, queue,0) +
							(1 - timeChangeFactor) * oldPriority
	}
	for queue, usage := range usage {
		_, exists := newPriority[queue]
		if !exists {
			newPriority[queue] = timeChangeFactor * usage
		}
	}
	return newPriority
}

func calculateUsage(resourceScarcity map[string]float64, queues []*api.QueueReport) map[string]float64 {
	usages := map[string]float64{}
	for _, queue := range queues {
		usage := 0.0
		for resourceName, quantity := range queue.Resources {
			scarcity := util.GetOrDefault(resourceScarcity, resourceName, 1)
			usage += common.QuantityAsFloat64(quantity) * scarcity
		}
		usages[queue.Name] = usage
	}
	return usages
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

func sumResources(reports map[string]*api.ClusterUsageReport) common.ComputeResources {
	result := common.ComputeResources{}
	for _, report := range reports {
		result.Add(report.ClusterCapacity)
	}
	return result
}
