package scheduler

import (
	"github.com/armadaproject/armada/internal/common/armadacontext"
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

// QueuedJobsIterator is an iterator over all jobs in a queue.
type QueuedJobsIterator struct {
	repo   JobRepository
	jobIds []string
	idx    int
	ctx    *armadacontext.Context
}

// MultiJobsIterator chains several JobIterators together in the order provided.
type MultiJobsIterator struct {
	i   int
	its []jobdb.JobIterator
}

func NewMultiJobsIterator(its ...jobdb.JobIterator) *MultiJobsIterator {
	return &MultiJobsIterator{
		its: its,
	}
}

func (it *MultiJobsIterator) Next() (*jobdb.Job, bool) {
	if it.i >= len(it.its) {
		return nil, false
	}
	v, ok := it.its[it.i].Next()
	if ok {
		return v, true
	}
	it.i++
	return it.Next()
}
