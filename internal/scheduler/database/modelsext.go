package database

// InTerminalState returns true if Job is in a terminal state
func (job Job) InTerminalState() bool {
	return job.Failed || job.Succeeded || job.Cancelled
}

// GetSerial is needed for the HasSerial interface
func (job Job) GetSerial() int64 {
	return job.Serial
}

// GetSerial is needed for the HasSerial interface
func (run Run) GetSerial() int64 {
	return run.Serial
}

// GetSerial is needed for the HasSerial interface
func (row SelectUpdatedJobsRow) GetSerial() int64 {
	return row.Serial
}
