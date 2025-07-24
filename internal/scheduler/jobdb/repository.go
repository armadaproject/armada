package jobdb

import "github.com/armadaproject/armada/internal/scheduler/internaltypes"

type JobRepository interface {
	QueuedJobs(queueName string, pool string, sortOrder JobSortOrder) JobIterator
	GetById(id string) *Job
	NewJob(
		jobId string,
		jobSet string,
		queue string,
		priority uint32,
		schedulingInfo *internaltypes.JobSchedulingInfo,
		queued bool,
		queuedVersion int32,
		cancelRequested bool,
		cancelByJobSetRequested bool,
		cancelled bool,
		created int64,
		validated bool,
		pools []string,
		priceBand int32,
	) (*Job, error)
}
