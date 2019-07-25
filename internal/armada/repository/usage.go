package repository

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"strconv"
)

type Usage struct {
	PriorityPerQueue map[string]float64
	CurrentUsagePerQueue map[string]float64
}

const clusterReportKey = "Cluster:Report"
const clusterPrioritiesPrefix = "Cluster:Priority:"

type UsageRepository interface {
	GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error)
	GetClusterPriority(clusterId string) (map[string]float64, error)
	GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error)

	UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error
}

type RedisUsageRepository struct {
	Db *redis.Client
}

func (r RedisUsageRepository) GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error) {
	result, err := r.Db.HGetAll(clusterReportKey).Result()
	if err != nil {
		return nil, err
	}
	reports := make(map[string]*api.ClusterUsageReport)

	for k, v := range result {
		report := &api.ClusterUsageReport{}
		e := proto.Unmarshal([]byte(v), report)
		if e!= nil {
			return nil, e
		}
		reports[k] = report
	}
	return reports, nil
}

func (r RedisUsageRepository) GetClusterPriority(clusterId string) (map[string]float64, error) {
	result, err := r.Db.HGetAll(clusterPrioritiesPrefix+clusterId).Result()
	if err != nil {
		return nil, err
	}
	return toFloat64Map(result)
}

func (r RedisUsageRepository) GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error) {
	pipe := r.Db.Pipeline()
	cmds := make(map[string]*redis.StringStringMapCmd)
	for _, id := range clusterIds {
		cmds[id] = pipe.HGetAll(clusterPrioritiesPrefix+id)
	}
	_, e := pipe.Exec()
	if e != nil {
		return nil, e
	}

	clusterPriorities := make(map[string]map[string]float64)
	for id, cmd := range cmds {
		priorities, e := toFloat64Map(cmd.Val())
		if e != nil {
			return nil, e
		}
		clusterPriorities[id] = priorities
	}
	return clusterPriorities, nil
}

func (r RedisUsageRepository) UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error {

	pipe := r.Db.TxPipeline()

	data, e := proto.Marshal(report)
	if e != nil {
		return e
	}
	pipe.HSet(clusterReportKey, report.ClusterId, data)

	if len(priorities) > 0 {
		untyped := make(map[string]interface{})
		for k, v := range priorities {
			untyped[k] = v
		}
		pipe.HMSet(clusterPrioritiesPrefix+report.ClusterId, untyped)
	}

	_, err := pipe.Exec()
	return err
}

func toFloat64Map(result map[string]string) (map[string]float64, error) {
	reports := make(map[string]float64)
	for k, v := range result {
		priority, e := strconv.ParseFloat(v, 64)
		if e!= nil {
			return nil, e
		}
		reports[k] = priority
	}
	return reports, nil
}
