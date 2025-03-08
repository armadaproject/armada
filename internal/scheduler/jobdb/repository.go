package jobdb

type JobRepository interface {
	QueuedJobs(queueName string, order JobSortOrder) JobIterator
	GetById(id string) *Job
}
