package model

import (
	"time"

	v1 "k8s.io/api/core/v1"
)

type JobRequest struct {
	Queue    string      `json:"queue" binding:"required"`
	JobSetId string      `json:"jobSetId" binding:"required"`
	Priority float64     `json:"priority"`
	PodSpec  *v1.PodSpec `json:"podSpec" binding:"required"`
}

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

type Job struct {
	Id       string
	JobSetId string
	Queue    string

	Status    JobStatus
	ClusterId string
	Priority  float64

	Resource ComputeResource
	PodSpec  *v1.PodSpec

	Created time.Time
}

type JobEvent struct {
	SequenceId string
	JobId      string
	JobSetId   string
	Status     JobStatus
	Created    time.Time
}

type ComputeResource struct {
}
