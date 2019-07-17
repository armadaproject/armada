package repository

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/kjk/betterguid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"time"

	//"github.com/golang/protobuf/ptypes/timestamp"
)

const jobObjectPrefix = "job:"
const queuePrefix = "Job:Queue:"

type JobRepository interface {
	AddJob(request *api.JobRequest) (string, error)
	PeekQueue(queue string, limit int64) ([]*api.Job, error)
}

type RedisJobRepository struct {
	Db *redis.Client
}

func (repo RedisJobRepository) AddJob(request *api.JobRequest) (string, error) {

	pipe := repo.Db.TxPipeline()

	job := createJob(request)
	jobData, e := proto.Marshal(job)
	if e != nil {
		return "", e
	}

	pipe.ZAdd(queuePrefix+job.Queue, redis.Z{
		Member: job.Id,
		Score:  job.Priority})

	pipe.SAdd(jobObjectPrefix+job.Id, jobData)

	fmt.Println(job)

	_, e = pipe.Exec()
	return job.Id, e
}

func (repo RedisJobRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	ids, e := repo.Db.ZRange(queuePrefix+queue, 0, limit -1).Result()
	if e != nil {
		return nil, e
	}
	return repo.GetJobsByIds(ids)
}

func (repo RedisJobRepository) GetJobsByIds(ids []string) ([]*api.Job, error) {
	pipe := repo.Db.Pipeline()
	var cmds []*redis.StringCmd
	for _, id := range ids {
		cmds = append(cmds, pipe.Get(id))
	}
	_, e := pipe.Exec()
	if e != nil {
		return nil, e
	}

	var jobs []*api.Job
	for _, cmd := range cmds {
		d, _ := cmd.Bytes()
		job := &api.Job{}
		e = proto.Unmarshal(d,job)
		if e != nil {
			return nil, e
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func createJob(jobRequest *api.JobRequest) *api.Job {
	now := time.Now()
	res := calculateResource(jobRequest.PodSpec)

	j := api.Job{
		Id:       betterguid.New(),
		Queue:    jobRequest.Queue,
		JobSetId: jobRequest.JobSetId,

		Priority: jobRequest.Priority,

		Resources: res,
		PodSpec:  jobRequest.PodSpec,
		Created: &types.Timestamp{ Seconds:now.Unix(), Nanos: int32(now.Nanosecond())},
	}
	return &j
}

func calculateResource(podSpec *v1.PodSpec) map[string]resource.Quantity {
	resources := make(map[string]resource.Quantity)
	for _, c := range podSpec.Containers {
		for k, v := range  c.Resources.Limits {
			existing, ok := resources[string(k)];
			if ok {
				existing.Add(v)
			} else {
				resources[string(k)] = v.DeepCopy()
			}
		}
	}
	return resources
}


type ComputeResources map[string]resource.Quantity;

func (ComputeResources) Add(ComputeResources){

}




