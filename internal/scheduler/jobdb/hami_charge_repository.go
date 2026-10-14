package jobdb

// NewHamiChargingRepository returns a view of repository whose jobs are
// charged for HAMi GPUs in a pool whose mean GPU memory is memoryEstimateMiB
// (see Job.WithHamiCharge). Only the jobs' Device resource requirements differ
// from the underlying repository.
func NewHamiChargingRepository(repository JobRepository, memoryEstimateMiB int64) JobRepository {
	return &hamiChargingRepository{JobRepository: repository, memoryEstimateMiB: memoryEstimateMiB}
}

type hamiChargingRepository struct {
	JobRepository
	memoryEstimateMiB int64
}

func (r *hamiChargingRepository) GetById(id string) *Job {
	job := r.JobRepository.GetById(id)
	if job == nil {
		return nil
	}
	return job.WithHamiCharge(r.memoryEstimateMiB)
}

func (r *hamiChargingRepository) GetGangJobsByGangId(queue string, gangId string) ([]*Job, error) {
	jobs, err := r.JobRepository.GetGangJobsByGangId(queue, gangId)
	if err != nil {
		return nil, err
	}
	result := make([]*Job, len(jobs))
	for i, job := range jobs {
		result[i] = job.WithHamiCharge(r.memoryEstimateMiB)
	}
	return result, nil
}

func (r *hamiChargingRepository) QueuedJobs(queueName string, pool string, sortOrder JobSortOrder) JobIterator {
	return &hamiChargingIterator{JobIterator: r.JobRepository.QueuedJobs(queueName, pool, sortOrder), memoryEstimateMiB: r.memoryEstimateMiB}
}

type hamiChargingIterator struct {
	JobIterator
	memoryEstimateMiB int64
}

func (it *hamiChargingIterator) Next() (*Job, bool) {
	job, ok := it.JobIterator.Next()
	if job == nil {
		return job, ok
	}
	return job.WithHamiCharge(it.memoryEstimateMiB), ok
}
