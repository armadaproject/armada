package service

import (
	"math"
	"time"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/armada/metrics"
	"github.com/G-Research/k8s-batch/internal/armada/repository"
	"github.com/G-Research/k8s-batch/internal/common/util"
)

const minPriority = 0.5

type PriorityService interface {
	GetQueuePriorities() (map[*api.Queue]float64, error)
}

type MultiClusterPriorityService struct {
	usageRepository repository.UsageRepository
	queueRepository repository.QueueRepository
	metricRecorder  metrics.MetricRecorder
}

func NewMultiClusterPriorityService(
	usageRepository repository.UsageRepository,
	queueRepository repository.QueueRepository,
	metricRecorder metrics.MetricRecorder,
) *MultiClusterPriorityService {
	return &MultiClusterPriorityService{
		usageRepository: usageRepository,
		queueRepository: queueRepository,
		metricRecorder:  metricRecorder,
	}
}

func (p *MultiClusterPriorityService) GetQueuePriorities() (map[*api.Queue]float64, error) {
	queues, e := p.queueRepository.GetQueues()
	if e != nil {
		return nil, e
	}

	queuePriority, e := p.calculateQueuePriorities(queues)
	if e != nil {
		return nil, e
	}
	p.metricRecorder.RecordQueuePriorities(queuePriority)

	return queuePriority, nil
}

func (q *MultiClusterPriorityService) calculateQueuePriorities(queues []*api.Queue) (map[*api.Queue]float64, error) {

	usageReports, e := q.usageRepository.GetClusterUsageReports()
	if e != nil {
		return nil, e
	}

	activeClusterIds := filterActiveClusters(usageReports, 10*time.Minute)
	clusterPriorities, e := q.usageRepository.GetClusterPriorities(activeClusterIds)
	if e != nil {
		return nil, e
	}
	queuePriority := aggregatePriority(clusterPriorities)

	resultPriorityMap := map[*api.Queue]float64{}
	for _, queue := range queues {
		currentPriority, ok := queuePriority[queue.Name]
		if ok {
			resultPriorityMap[queue] = math.Max(currentPriority, minPriority) * queue.PriorityFactor
		} else {
			resultPriorityMap[queue] = minPriority * queue.PriorityFactor
		}
	}
	return resultPriorityMap, nil
}

func aggregatePriority(clusterPriorities map[string]map[string]float64) map[string]float64 {
	result := make(map[string]float64)
	for _, clusterPriority := range clusterPriorities {
		for queue, priority := range clusterPriority {
			result[queue] = priority + util.GetOrDefault(result, queue, 0)
		}
	}
	return result
}

func filterActiveClusters(reports map[string]*api.ClusterUsageReport, expiry time.Duration) []string {
	var result []string
	now := time.Now()
	for id, report := range reports {
		if report.ReportTime.Add(expiry).After(now) {
			result = append(result, id)
		}
	}
	return result
}
