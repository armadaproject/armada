package scheduling

import (
	"math"
	"time"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

const minPriority = 0.5

type QueuePriorityInfo struct {
	Priority     float64
	CurrentUsage armadaresource.ComputeResources
}

func CalculateQueuesPriorityInfo(
	clusterPriorities map[string]map[string]float64,
	activeClusterReports map[string]*api.ClusterUsageReport,
	queues []*api.Queue,
) map[*api.Queue]QueuePriorityInfo {
	queuePriority := aggregatePriority(clusterPriorities)
	queueUsage := aggregateQueueUsage(activeClusterReports)
	resultPriorityMap := map[*api.Queue]QueuePriorityInfo{}
	for _, queue := range queues {
		priority := minPriority
		currentPriority, ok := queuePriority[queue.Name]
		if ok {
			priority = math.Max(currentPriority, minPriority) * queue.PriorityFactor
		}
		resultPriorityMap[queue] = QueuePriorityInfo{
			Priority:     priority,
			CurrentUsage: queueUsage[queue.Name],
		}
	}
	return resultPriorityMap
}

func CalculatePriorityUpdate(
	resourceScarcity map[string]float64,
	previousReport *api.ClusterUsageReport,
	report *api.ClusterUsageReport,
	previousPriority map[string]float64,
	halfTime time.Duration,
) map[string]float64 {
	timeChange := time.Minute
	if previousReport != nil {
		timeChange = report.ReportTime.Sub(previousReport.ReportTime)
	}
	usage := usageFromQueueReports(resourceScarcity, util.GetQueueReports(report))
	newPriority := calculatePriorityUpdate(usage, previousPriority, timeChange, halfTime)
	return newPriority
}

func calculatePriorityUpdate(
	usage map[string]float64,
	previousPriority map[string]float64,
	timeChange time.Duration,
	halfTime time.Duration,
) map[string]float64 {
	newPriority := map[string]float64{}
	timeChangeFactor := math.Pow(0.5, timeChange.Seconds()/halfTime.Seconds())

	for queue, oldPriority := range previousPriority {
		newPriority[queue] = timeChangeFactor*oldPriority +
			(1-timeChangeFactor)*util.GetOrDefault(usage, queue, 0)
	}
	for queue, usage := range usage {
		_, exists := newPriority[queue]
		if !exists {
			newPriority[queue] = (1 - timeChangeFactor) * usage
		}
	}
	return newPriority
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

func aggregateQueueUsage(reports map[string]*api.ClusterUsageReport) map[string]armadaresource.ComputeResources {
	result := map[string]armadaresource.ComputeResources{}
	for _, report := range reports {
		for _, queueReport := range util.GetQueueReports(report) {
			current, ok := result[queueReport.Name]
			if !ok {
				result[queueReport.Name] = armadaresource.ComputeResources(queueReport.Resources).DeepCopy()
			} else {
				current.Add(queueReport.Resources)
			}
		}
	}
	return result
}
