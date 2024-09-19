package jobiteration

import (
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/benbjohnson/immutable"
)

type JobRepository interface {
	GetById(id string) *jobdb.Job
	QueuedJobs(queue string) jobdb.JobIterator
}

type InMemoryJobRepository struct {
	jobsByQueue map[string]immutable.SortedSet[*jobdb.Job]
	jobsById    map[string]*jobdb.Job
}

func NewInMemoryJobRepository(jobs []*jobdb.Job) *InMemoryJobRepository {

	jobsByQueue := make(map[string]immutable.SortedSet[*jobdb.Job])
	jobsById := make(map[string]*jobdb.Job, len(jobs))

	for _, job := range jobs {
		queueName := job.Queue()
		queue, ok := jobsByQueue[queueName]
		if !ok {
			queue = immutable.NewSortedSet[*jobdb.Job](jobdb.JobPriorityComparer{})
		}
		jobsByQueue[queueName] = queue.Add(job)
		jobsById[job.Id()] = job
	}
	return &InMemoryJobRepository{
		jobsByQueue: jobsByQueue,
		jobsById:    jobsById,
	}
}

func (repo *InMemoryJobRepository) GetById(jobId string) *jobdb.Job {
	if job, ok := repo.jobsById[jobId]; ok {
		return job
	}
	return nil
}

func (repo *InMemoryJobRepository) QueuedJobs(queueName string) jobdb.JobIterator {
	queue, ok := repo.jobsByQueue[queueName]
	if ok {
		return queue.Iterator()
	}
	return nil
}
