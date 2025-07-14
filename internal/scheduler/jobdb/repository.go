package jobdb

type JobRepository interface {
	GetGangJobsByGangId(queue string, gangId string) ([]*Job, error)
	QueuedJobs(queueName string, pool string, sortOrder JobSortOrder) JobIterator
	GetById(id string) *Job
}
