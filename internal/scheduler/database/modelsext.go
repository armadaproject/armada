package database

func (job Job) Terminal() bool {
	return job.Failed || job.Succeeded || job.Cancelled
}

func (job Job) GetSerial() int64 {
	return job.Serial
}

func (job Job) JobIdString() string {
	return job.JobID
}

func (run Run) RunIdString() string {
	return run.JobID
}

func (run Run) GetSerial() int64 {
	return run.Serial
}
