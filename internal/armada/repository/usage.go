package repository

import (
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/types"
)

type Usage struct {
	PriorityPerQueue map[string]float64
	CurrentUsagePerQueue map[string]float64
}

type UsageRepository interface {
	GetAvailableResources() (common.ComputeResources, error)
	UpdateClusterResource(clusterId string, resources common.ComputeResources) error

	GetClusterPriority(clusterId string) (map[string]float64, *types.Timestamp, error)
	UpdateClusterPriority(clusterId string, priorities map[string]float64, timestamp *types.Timestamp) error

	GetActiveClusterIds(timestamp types.Timestamp) ([]string, error)
}

type RedisUsageRepository struct {
	Db *redis.Client
}

func (r RedisUsageRepository) GetClusterPriority(clusterId string) (map[string]float64, *types.Timestamp, error) {
	panic("implement me")
}

func (r RedisUsageRepository) UpdateClusterPriority(clusterId string, priorities map[string]float64, timestamp *types.Timestamp) error {
	panic("implement me")
	return nil
}

func (r RedisUsageRepository) GetAvailableResources() (common.ComputeResources, error) {
	panic("implement me")
}

func (r RedisUsageRepository) UpdateClusterResource(clusterId string, resources common.ComputeResources) error {
	panic("implement me")
	return nil
}

func (r RedisUsageRepository) GetActiveClusterIds(timestamp types.Timestamp) ([]string, error) {
	panic("implement me")
}
