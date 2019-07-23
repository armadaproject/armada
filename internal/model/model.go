package model

import (
	"time"
)

type JobStatus int

const (
	Queued     = iota // queued outside cluster
	Submitting = iota // send to the cluster, no confirmation available yet
	Pending    = iota // aknowledged by the cluster, not started yet
	Running    = iota
	Succeeded  = iota // job finished with exit code 0
	Failed     = iota // failed to start or finished with non zero exit code
	Cancelling = iota // Cancellation of job was requested
	Cancelled  = iota
)

type JobEvent struct {
	SequenceId string
	JobId      string
	JobSetId   string
	Status     JobStatus
	Created    time.Time
}
