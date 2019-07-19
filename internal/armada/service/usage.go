
package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/gogo/protobuf/types"
	"k8s.io/apimachinery/pkg/api/resource"
	"math"
	"math/big"
	"time"
)

type UsageServer struct {
	priorityHalfTime time.Duration
	usageRepository  repository.UsageRepository
}

func (s UsageServer) ReportUsage(ctx context.Context, report *api.ClusterUsageReport) (*types.Empty, error) {

	err := s.usageRepository.UpdateClusterResource(report.ClusterId, report.ClusterCapacity)
	if err != nil {
		return nil, err
	}
	availableResources, err := s.usageRepository.GetAvailableResources()
	if err != nil {
		return nil, err
	}
	previousPriority, previousTime, err := s.usageRepository.GetClusterPriority(report.ClusterId)
	if err != nil {
		return nil, err
	}

	timeChange := difference(previousTime, report.ReportTime)
	resourceScarcity := calculateResourceScarcity(availableResources)
	usage := calculateUsage(resourceScarcity, report.Queues)
	newPriority := calculatePriority(usage, previousPriority, timeChange, s.priorityHalfTime)

	err = s.usageRepository.UpdateClusterPriority(report.ClusterId, newPriority, report.ReportTime)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func calculatePriority(usage map[string]float64, previousPriority map[string]float64, timeChange time.Duration, halfTime time.Duration) map[string]float64 {

	newPriority := map[string]float64{}
	timeChangeFactor := math.Pow(0.5, timeChange.Seconds() / halfTime.Seconds())

	for queue, oldPriority := range previousPriority {
		newPriority[queue] = timeChangeFactor * getOrDefault(usage, queue,0) +
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
			scarcity := getOrDefault(resourceScarcity, resourceName, 1)
			usage += asFloat64(quantity) * scarcity
		}
		usages[queue.Name] = usage
	}
	return usages
}

// Calculates inverse of resources per cpu unit
// { cpu: 4, memory: 20GB, gpu: 2 } -> { cpu: 1.0, memory: 0.2, gpu: 2 }
func calculateResourceScarcity(res common.ComputeResources) map[string]float64 {
	importance := map[string]float64{
		"cpu": 1,
	}
	cpu := asFloat64(res["cpu"])

	for k, v := range res {
		if k == "cpu"{
			continue
		}
		q := asFloat64(v)
		if q >= 0.00001 {
			importance[k] = cpu / q
		}
	}
	return importance
}

func difference(from *types.Timestamp, to*types.Timestamp) time.Duration{
	fromTime, _ := types.TimestampFromProto(from)
	toTime, _ := types.TimestampFromProto(to)

	return toTime.Sub(fromTime)
}

func getOrDefault(m map[string]float64, key string, def float64) float64 {
	v, ok := m[key]
	if ok {
		return v
	}
	return def
}

func asFloat64(q resource.Quantity) float64 {
	dec:= q.AsDec()
	unscaled := dec.UnscaledBig()
	scale := dec.Scale()
	unscaledFloat, _ := new(big.Float).SetInt(unscaled).Float64()
	return unscaledFloat * math.Pow10(-int(scale))
}
