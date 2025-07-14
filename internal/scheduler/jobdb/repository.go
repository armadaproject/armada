package jobdb

type JobRepository interface {
	QueuedJobs(queueName string, pool string, sortOrder JobSortOrder) JobIterator
	GetById(id string) *Job
}
