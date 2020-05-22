package repository

import (
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"

	"github.com/G-Research/armada/pkg/api"
)

const clusterNodeInfoReportKey = "Cluster:NodeInfo"

type NodeInfoRepository interface {
	GetClusterNodeInfo() (map[string]*api.ClusterNodeInfoReport, error)
	UpdateClusterNodeInfo(report *api.ClusterNodeInfoReport) error
}

type RedisNodeInfoRepository struct {
	db redis.UniversalClient
}

func NewRedisNodeInfoRepository(db redis.UniversalClient) *RedisNodeInfoRepository {
	return &RedisNodeInfoRepository{db: db}
}

func (r *RedisNodeInfoRepository) GetClusterNodeInfo() (map[string]*api.ClusterNodeInfoReport, error) {
	result, err := r.db.HGetAll(clusterNodeInfoReportKey).Result()
	if err != nil {
		return nil, err
	}
	reports := make(map[string]*api.ClusterNodeInfoReport)

	for k, v := range result {
		report := &api.ClusterNodeInfoReport{}
		e := proto.Unmarshal([]byte(v), report)
		if e != nil {
			return nil, e
		}
		reports[k] = report
	}
	return reports, nil
}

func (r *RedisNodeInfoRepository) UpdateClusterNodeInfo(report *api.ClusterNodeInfoReport) error {
	data, e := proto.Marshal(report)
	if e != nil {
		return e
	}
	_, e = r.db.HSet(clusterNodeInfoReportKey, report.ClusterId, data).Result()
	return e
}
