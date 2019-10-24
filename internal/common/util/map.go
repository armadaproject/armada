package util

import "github.com/G-Research/armada/internal/armada/api"

func GetOrDefault(m map[string]float64, key string, def float64) float64 {
	v, ok := m[key]
	if ok {
		return v
	}
	return def
}

func GetPriorityMapQueues(priorities map[*api.Queue]float64) []*api.Queue {
	queues := []*api.Queue{}
	for queue := range priorities {
		queues = append(queues, queue)
	}
	return queues
}
