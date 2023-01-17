package repository

import (
	"fmt"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/armadaproject/armada/pkg/api"
)

const clusterSchedulingInfoReportKey = "Cluster:SchedulingInfo"

type SchedulingInfoRepository interface {
	GetClusterSchedulingInfo() (map[string]*api.ClusterSchedulingInfoReport, error)
	UpdateClusterSchedulingInfo(report *api.ClusterSchedulingInfoReport) error
}

type RedisSchedulingInfoRepository struct {
	db redis.UniversalClient
}

func NewRedisSchedulingInfoRepository(db redis.UniversalClient) *RedisSchedulingInfoRepository {
	return &RedisSchedulingInfoRepository{db: db}
}

func (r *RedisSchedulingInfoRepository) GetClusterSchedulingInfo() (map[string]*api.ClusterSchedulingInfoReport, error) {
	result, err := r.db.HGetAll(clusterSchedulingInfoReportKey).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisSchedulingInfoRepository.GetClusterSchedulingInfo] error reading from database: %s", err)
	}

	reports := make(map[string]*api.ClusterSchedulingInfoReport)
	for k, v := range result {
		report := &api.ClusterSchedulingInfoReport{}
		err = proto.Unmarshal([]byte(v), report)
		if err != nil {
			return nil, fmt.Errorf("[RedisSchedulingInfoRepository.GetClusterSchedulingInfo] error unmarshalling: %s", err)
		}
		reports[k] = report
	}
	return reports, nil
}

func (r *RedisSchedulingInfoRepository) UpdateClusterSchedulingInfo(report *api.ClusterSchedulingInfoReport) error {
	data, err := proto.Marshal(report)
	if err != nil {
		return fmt.Errorf("[RedisSchedulingInfoRepository.UpdateClusterSchedulingInfo] error marshalling: %s", err)
	}

	_, err = r.db.HSet(clusterSchedulingInfoReportKey, report.ClusterId, data).Result()
	if err != nil {
		return fmt.Errorf("[RedisSchedulingInfoRepository.UpdateClusterSchedulingInfo] error writing to database: %s", err)
	}

	return nil
}
