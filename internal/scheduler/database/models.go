// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.16.0

package database

import (
	"database/sql"
	"time"

	"github.com/google/uuid"
)

type Executor struct {
	ExecutorID  string    `db:"executor_id"`
	LastRequest []byte    `db:"last_request"`
	LastUpdated time.Time `db:"last_updated"`
}

type Job struct {
	JobID           string    `db:"job_id"`
	JobSet          string    `db:"job_set"`
	Queue           string    `db:"queue"`
	UserID          string    `db:"user_id"`
	Submitted       int64     `db:"submitted"`
	Groups          []byte    `db:"groups"`
	Priority        int64     `db:"priority"`
	CancelRequested bool      `db:"cancel_requested"`
	Cancelled       bool      `db:"cancelled"`
	Succeeded       bool      `db:"succeeded"`
	Failed          bool      `db:"failed"`
	SubmitMessage   []byte    `db:"submit_message"`
	SchedulingInfo  []byte    `db:"scheduling_info"`
	Serial          int64     `db:"serial"`
	LastModified    time.Time `db:"last_modified"`
}

type JobRunError struct {
	RunID uuid.UUID `db:"run_id"`
	JobID string    `db:"job_id"`
	Error []byte    `db:"error"`
}

type Marker struct {
	GroupID     uuid.UUID    `db:"group_id"`
	PartitionID int32        `db:"partition_id"`
	Created     sql.NullTime `db:"created"`
}

type Queue struct {
	Name   string  `db:"name"`
	Weight float64 `db:"weight"`
}

type Run struct {
	RunID        uuid.UUID `db:"run_id"`
	JobID        string    `db:"job_id"`
	JobSet       string    `db:"job_set"`
	Executor     string    `db:"executor"`
	Cancelled    bool      `db:"cancelled"`
	Running      bool      `db:"running"`
	Succeeded    bool      `db:"succeeded"`
	Failed       bool      `db:"failed"`
	Returned     bool      `db:"returned"`
	Serial       int64     `db:"serial"`
	LastModified time.Time `db:"last_modified"`
}
