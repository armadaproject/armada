package jobdb

type JobRepository interface {
	QueuedJobs(queueName string) JobIterator
	GetById(id string) *Job
}
