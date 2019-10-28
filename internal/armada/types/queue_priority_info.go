package types

import (
	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
)

type QueuePriorityInfo struct {
	Priority     float64
	CurrentUsage common.ComputeResources
}

func GetPriorityMapQueues(priorities map[*api.Queue]QueuePriorityInfo) []*api.Queue {
	queues := []*api.Queue{}
	for queue := range priorities {
		queues = append(queues, queue)
	}
	return queues
}
