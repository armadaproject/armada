package repository

import (
	"fmt"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

type Usage struct {
	PriorityPerQueue     map[string]float64
	CurrentUsagePerQueue map[string]float64
}

const (
	clusterReportKey             = "Cluster:Report"
	clusterQueueResourceUsageKey = "Cluster:QueueResourceUsage"
	clusterLeasedReportKey       = "Cluster:Leased"
	clusterPrioritiesPrefix      = "Cluster:Priority:"
)

type UsageRepository interface {
	GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error)
	GetClusterPriority(clusterId string) (map[string]float64, error)
	GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error)
	GetClusterLeasedReports() (map[string]*api.ClusterLeasedReport, error)

	GetClusterQueueResourceUsage() (map[string]*schedulerobjects.ClusterResourceUsageReport, error)

	UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error
	UpdateClusterLeased(report *api.ClusterLeasedReport) error
	UpdateClusterQueueResourceUsage(cluster string, resourceUsage *schedulerobjects.ClusterResourceUsageReport) error
}

type RedisUsageRepository struct {
	db redis.UniversalClient
}

func NewRedisUsageRepository(db redis.UniversalClient) *RedisUsageRepository {
	return &RedisUsageRepository{db: db}
}

func (r *RedisUsageRepository) GetClusterUsageReports() (map[string]*api.ClusterUsageReport, error) {
	result, err := r.db.HGetAll(clusterReportKey).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisUsageRepository.GetClusterUsageReports] error reading from database: %s", err)
	}
	reports := make(map[string]*api.ClusterUsageReport)

	for k, v := range result {
		report := &api.ClusterUsageReport{}
		err = proto.Unmarshal([]byte(v), report)
		if err != nil {
			return nil, fmt.Errorf("[RedisUsageRepository.GetClusterUsageReports] error unmarshalling: %s", err)
		}
		reports[k] = report
	}
	return reports, nil
}

func (r *RedisUsageRepository) GetClusterLeasedReports() (map[string]*api.ClusterLeasedReport, error) {
	result, err := r.db.HGetAll(clusterLeasedReportKey).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisUsageRepository.GetClusterLeasedReports] error reading from database: %s", err)
	}
	reports := make(map[string]*api.ClusterLeasedReport)

	for k, v := range result {
		report := &api.ClusterLeasedReport{}
		err = proto.Unmarshal([]byte(v), report)
		if err != nil {
			return nil, fmt.Errorf("[RedisUsageRepository.GetClusterLeasedReports] error unmarshalling: %s", err)
		}
		reports[k] = report
	}
	return reports, nil
}

func (r *RedisUsageRepository) GetClusterPriority(clusterId string) (map[string]float64, error) {
	result, err := r.db.HGetAll(clusterPrioritiesPrefix + clusterId).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisUsageRepository.GetClusterPriority] error reading from database: %s", err)
	}

	rv, err := toFloat64Map(result)
	if err != nil {
		return nil, fmt.Errorf("[RedisUsageRepository.GetClusterPriority] error converting to Float64: %s", err)
	}

	return rv, nil
}

// GetClusterPriorities returns a map from clusterId to clusterPriority.
// This method makes a single aggregated call to the database, making this method more efficient
// than calling GetClusterPriority repeatredly.
func (r *RedisUsageRepository) GetClusterPriorities(clusterIds []string) (map[string]map[string]float64, error) {
	pipe := r.db.Pipeline()
	cmds := make(map[string]*redis.StringStringMapCmd)
	for _, id := range clusterIds {
		cmds[id] = pipe.HGetAll(clusterPrioritiesPrefix + id)
		err := cmds[id].Err()
		if err != nil {
			return nil, fmt.Errorf("[RedisUsageRepository.GetClusterPriorities] error reading from database: %s", err)
		}
	}

	// For the default pipeline executor, only the first error is returned,
	// i.e., commands executed after the erroring command may have also returned an error.
	_, err := pipe.Exec()
	if err != nil {
		return nil, fmt.Errorf("[RedisUsageRepository.GetClusterPriorities] error performing pipelined read from database: %s", err)
	}

	clusterPriorities := make(map[string]map[string]float64)
	for id, cmd := range cmds {
		priorities, err := toFloat64Map(cmd.Val())
		if err != nil {
			return nil, fmt.Errorf("[RedisUsageRepository.GetClusterPriorities] error converting to Float64: %s", err)
		}
		clusterPriorities[id] = priorities
	}

	return clusterPriorities, nil
}

func (r *RedisUsageRepository) UpdateCluster(report *api.ClusterUsageReport, priorities map[string]float64) error {
	data, err := proto.Marshal(report)
	if err != nil {
		return fmt.Errorf("[RedisUsageRepository.UpdateCluster] error marshalling report: %s", err)
	}

	pipe := r.db.TxPipeline()
	pipe.HSet(clusterReportKey, report.ClusterId, data)
	if len(priorities) > 0 {
		untyped := make(map[string]interface{})
		for k, v := range priorities {
			untyped[k] = v
		}
		pipe.HMSet(clusterPrioritiesPrefix+report.ClusterId, untyped)
	}

	_, err = pipe.Exec()
	if err != nil {
		return fmt.Errorf("[RedisUsageRepository.UpdateCluster] error performing pipelined writes to database: %s", err)
	}

	return nil
}

// UpdateClusterLeased updates the count of resources leased to a particular cluster.
func (r *RedisUsageRepository) UpdateClusterLeased(report *api.ClusterLeasedReport) error {
	data, err := proto.Marshal(report)
	if err != nil {
		return fmt.Errorf("[RedisUsageRepository.UpdateClusterLeased] error marshalling report: %s", err)
	}

	_, err = r.db.HSet(clusterLeasedReportKey, report.ClusterId, data).Result()
	if err != nil {
		return fmt.Errorf("[RedisUsageRepository.UpdateClusterLeased] error writing to database: %s", err)
	}

	return err
}

func (r *RedisUsageRepository) GetClusterQueueResourceUsage() (map[string]*schedulerobjects.ClusterResourceUsageReport, error) {
	result, err := r.db.HGetAll(clusterQueueResourceUsageKey).Result()
	if err != nil {
		return nil, fmt.Errorf("[RedisUsageRepository.GetClusterQueueResourceUsage] error reading from database: %s", err)
	}
	reports := make(map[string]*schedulerobjects.ClusterResourceUsageReport)

	for k, v := range result {
		report := &schedulerobjects.ClusterResourceUsageReport{}
		err = proto.Unmarshal([]byte(v), report)
		if err != nil {
			return nil, fmt.Errorf("[RedisUsageRepository.GetClusterQueueResourceUsage] error unmarshalling: %s", err)
		}
		reports[k] = report
	}
	return reports, nil
}

func (r *RedisUsageRepository) UpdateClusterQueueResourceUsage(cluster string, resourceUsage *schedulerobjects.ClusterResourceUsageReport) error {
	data, err := proto.Marshal(resourceUsage)
	if err != nil {
		return errors.WithStack(err)
	}
	cmd := r.db.HSet(clusterQueueResourceUsageKey, cluster, data)
	if cmd.Err() != nil {
		return errors.WithStack(cmd.Err())
	}
	return nil
}

func toFloat64Map(result map[string]string) (map[string]float64, error) {
	reports := make(map[string]float64)
	for k, v := range result {
		priority, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return nil, fmt.Errorf("[toFloat64Map] error converting %q to Float64: %s", v, err)
		}
		reports[k] = priority
	}
	return reports, nil
}
