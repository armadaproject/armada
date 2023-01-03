package database

import (
	"time"

	"time"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
)

type Cluster struct {
	Name           string
	Pool           string
	Usage          *schedulerobjects.ClusterResourceUsageReport
	Nodes          []*schedulerobjects.Node
	MinimumJobSize map[string]resource.Quantity
	lastUpdateTime time.Time
}

type ClusterRepository interface {
	GetClusters() ([]*Cluster, error)
	GetLastUpdateTimes() (map[string]time.Time, error)
}
