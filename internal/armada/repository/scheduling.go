package repository

import (
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/pkg/api"
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
		return nil, err
	}
	reports := make(map[string]*api.ClusterSchedulingInfoReport)

	for k, v := range result {
		report := &api.ClusterSchedulingInfoReport{}
		e := proto.Unmarshal([]byte(v), report)
		if e != nil {
			return nil, e
		}
		reports[k] = report
	}
	return reports, nil
}

func (r *RedisSchedulingInfoRepository) UpdateClusterSchedulingInfo(report *api.ClusterSchedulingInfoReport) error {
	data, e := proto.Marshal(report)
	if e != nil {
		return e
	}
	_, e = r.db.HSet(clusterSchedulingInfoReportKey, report.ClusterId, data).Result()
	return e
}
