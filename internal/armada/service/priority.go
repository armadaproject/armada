package service

import (
	"math"
	"time"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/metrics"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/types"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
)

const minPriority = 0.5

type PriorityService interface {
	GetQueuePriorities() (map[*api.Queue]types.QueuePriorityInfo, error)
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

func (p *MultiClusterPriorityService) GetQueuePriorities() (map[*api.Queue]types.QueuePriorityInfo, error) {
	queues, e := p.queueRepository.GetAllQueues()
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

func (q *MultiClusterPriorityService) calculateQueuePriorities(queues []*api.Queue) (map[*api.Queue]types.QueuePriorityInfo, error) {

	usageReports, e := q.usageRepository.GetClusterUsageReports()
	if e != nil {
		return nil, e
	}

	activeClusterReports := filterActiveClusters(usageReports, 10*time.Minute)
	clusterPriorities, e := q.usageRepository.GetClusterPriorities(getClusterReportIds(activeClusterReports))
	if e != nil {
		return nil, e
	}
	queuePriority := aggregatePriority(clusterPriorities)
	queueUsage := aggregateQueueUsage(activeClusterReports)

	resultPriorityMap := map[*api.Queue]types.QueuePriorityInfo{}
	for _, queue := range queues {
		priority := minPriority * queue.PriorityFactor
		currentPriority, ok := queuePriority[queue.Name]
		if ok {
			priority = math.Max(currentPriority, minPriority) * queue.PriorityFactor
		}
		resultPriorityMap[queue] = types.QueuePriorityInfo{
			Priority:     priority,
			CurrentUsage: queueUsage[queue.Name],
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

func filterActiveClusters(reports map[string]*api.ClusterUsageReport, expiry time.Duration) map[string]*api.ClusterUsageReport {
	result := map[string]*api.ClusterUsageReport{}
	now := time.Now()
	for id, report := range reports {
		if report.ReportTime.Add(expiry).After(now) {
			result[id] = report
		}
	}
	return result
}

func getClusterReportIds(reports map[string]*api.ClusterUsageReport) []string {
	var result []string
	for id := range reports {
		result = append(result, id)
	}
	return result
}

func aggregateQueueUsage(reports map[string]*api.ClusterUsageReport) map[string]common.ComputeResources {
	result := map[string]common.ComputeResources{}
	for _, report := range reports {
		for _, queueReport := range report.Queues {
			current, ok := result[queueReport.Name]
			if !ok {
				result[queueReport.Name] = queueReport.Resources
			} else {
				current.Add(queueReport.Resources)
			}
		}
	}
	return result
}
