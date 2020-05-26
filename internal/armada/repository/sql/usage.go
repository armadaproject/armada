package sql

import (
	"database/sql"

	"github.com/G-Research/armada/pkg/api"
)

type UsageRepository struct {
	db sql.DB
}

func (u UsageRepository) GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error) {
	panic("implement me")
}

func (u UsageRepository) GetClusterPriority(clusterId string) (map[string]float64, error) {
	panic("implement me")
}

func (u UsageRepository) GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error) {
	panic("implement me")
}

func (u UsageRepository) GetClusterLeasedReports() (map[string]*api.ClusterLeasedReport, error) {
	panic("implement me")
}

func (u UsageRepository) UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error {
	panic("implement me")
}

func (u UsageRepository) UpdateClusterLeased(report *api.ClusterLeasedReport) error {
	panic("implement me")
}
