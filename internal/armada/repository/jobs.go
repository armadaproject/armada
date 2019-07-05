package repository

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
	"github.com/kjk/betterguid"

	"github.com/G-Research/k8s-batch/internal/model"
)

const jobObjectPrefix = "job:"
const queuePrefix = "Job:Queue:"

func AddJobs(db *redis.Client, requets []model.JobRequest) error {

	pipe := db.TxPipeline()
	for _, request := range requets {

		job := createJob(&request)

		pipe.ZAdd(queuePrefix+job.Queue, redis.Z{
			Member: job.Id,
			Score:  job.Priority})

		saveJobObject(pipe, job)
		fmt.Println(job)
	}
	_, e := pipe.Exec()
	return e
}

func createJob(jobRequest *model.JobRequest) *model.Job {
	j := model.Job{
		Id:       betterguid.New(),
		Queue:    jobRequest.Queue,
		JobSetId: jobRequest.JobSetId,

		Status:   model.Queued,
		Priority: jobRequest.Priority,

		Resource: model.ComputeResource{}, // todo
		PodSpec:  jobRequest.PodSpec,

		Created: time.Now(),
	}
	return &j
}

func saveJobObject(db redis.Cmdable, job *model.Job) {
	db.HMSet(jobObjectPrefix+job.Id, map[string]interface{}{
		"queue": job.Queue,
		// ... TODO
	})
}
