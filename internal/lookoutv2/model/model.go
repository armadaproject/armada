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
	MatchExists               = "exists"

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
	Namespace          *string
	Priority           int64
	PriorityClass      *string
	Queue              string
	Runs               []*Run
	State              string
	Submitted          time.Time
	CancelReason       *string
	Node               *string
	Cluster            string
	ExitCode           *int32
}

// PostgreSQLTime is a wrapper around time.Time that converts to UTC when
// deserializing from JSON.
//
// It exists to work around the following issue:
//
//  1. PostgreSQL serializes UTC timestamps within a JSON object in the format
//     "2023-11-03T09:10:42.201577+00:00"; in particular, this format uses a
//     timezone offset instead of "Z" to indicate UTC.
//  2. When deserializing this UTC timestamp, Go sets the location of the
//     resulting time.Time value to "local".
//  3. Tests compare timestamps with == instead of Equal, which means that two
//     time.Time values with different locations are not considered equal
//     (even if they represent the same instants in time).
type PostgreSQLTime struct {
	Time time.Time
}

func NewPostgreSQLTime(t *time.Time) *PostgreSQLTime {
	if t == nil {
		return nil
	}
	return &PostgreSQLTime{Time: *t}
}

func (t PostgreSQLTime) MarshalJSON() ([]byte, error) {
	return t.Time.MarshalJSON()
}

func (t *PostgreSQLTime) UnmarshalJSON(b []byte) error {
	if err := t.Time.UnmarshalJSON(b); err != nil {
		return err
	}
	t.Time = t.Time.UTC()
	return nil
}

type Run struct {
	Cluster     string
	ExitCode    *int32
	Finished    *PostgreSQLTime
	JobRunState int
	Node        *string
	Leased      *PostgreSQLTime
	Pending     *PostgreSQLTime
	RunId       string
	Started     *PostgreSQLTime
}

type JobGroup struct {
	Aggregates map[string]interface{}
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

type GroupedField struct {
	Field        string
	IsAnnotation bool
}
