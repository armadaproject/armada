package model

import (
	"time"
)

const (
	MatchExact                = "exact"
	MatchAnyOf                = "anyOf"
	MatchStartsWith           = "startsWith"
	MatchContains             = "contains"
	MatchGreaterThan          = "greaterThan"
	MatchLessThan             = "lessThan"
	MatchGreaterThanOrEqualTo = "greaterThanOrEqualTo"
	MatchLessThanOrEqualTo    = "lessThanOrEqualTo"

	DirectionAsc  = "ASC"
	DirectionDesc = "DESC"
)

type Job struct {
	Annotations        map[string]string
	Cancelled          *time.Time
	Cpu                int64
	Duplicate          bool
	EphemeralStorage   int64
	Gpu                int64
	JobId              string
	JobSet             string
	LastActiveRunId    *string
	LastTransitionTime time.Time
	Memory             int64
	Owner              string
	Priority           int64
	PriorityClass      *string
	Queue              string
	Runs               []*Run
	State              string
	Submitted          time.Time
	CancelReason       *string
}

type Run struct {
	Cluster     string
	ExitCode    *int32
	Finished    *time.Time
	JobRunState string
	Node        *string
	Pending     time.Time
	RunId       string
	Started     *time.Time
}

type JobGroup struct {
	Aggregates map[string]string
	Count      int64
	Name       string
}

type Filter struct {
	Field        string
	Match        string
	Value        interface{}
	IsAnnotation bool
}

type Order struct {
	Direction string
	Field     string
}
