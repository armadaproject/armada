// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0

package database

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type Job struct {
	JobID                     string           `db:"job_id"`
	Queue                     string           `db:"queue"`
	Owner                     string           `db:"owner"`
	Jobset                    string           `db:"jobset"`
	Cpu                       int64            `db:"cpu"`
	Memory                    int64            `db:"memory"`
	EphemeralStorage          int64            `db:"ephemeral_storage"`
	Gpu                       int64            `db:"gpu"`
	Priority                  int64            `db:"priority"`
	Submitted                 pgtype.Timestamp `db:"submitted"`
	Cancelled                 pgtype.Timestamp `db:"cancelled"`
	State                     int16            `db:"state"`
	LastTransitionTime        pgtype.Timestamp `db:"last_transition_time"`
	LastTransitionTimeSeconds int64            `db:"last_transition_time_seconds"`
	JobSpec                   []byte           `db:"job_spec"`
	Duplicate                 bool             `db:"duplicate"`
	PriorityClass             *string          `db:"priority_class"`
	LatestRunID               *string          `db:"latest_run_id"`
	CancelReason              *string          `db:"cancel_reason"`
	Namespace                 *string          `db:"namespace"`
	Annotations               []byte           `db:"annotations"`
	ExternalJobUri            *string          `db:"external_job_uri"`
	CancelUser                *string          `db:"cancel_user"`
}

type JobDeduplication struct {
	DeduplicationID string           `db:"deduplication_id"`
	JobID           string           `db:"job_id"`
	Inserted        pgtype.Timestamp `db:"inserted"`
}

type JobError struct {
	JobID string `db:"job_id"`
	Error []byte `db:"error"`
}

type JobRun struct {
	RunID       string           `db:"run_id"`
	JobID       string           `db:"job_id"`
	Cluster     string           `db:"cluster"`
	Node        *string          `db:"node"`
	Pending     pgtype.Timestamp `db:"pending"`
	Started     pgtype.Timestamp `db:"started"`
	Finished    pgtype.Timestamp `db:"finished"`
	JobRunState int16            `db:"job_run_state"`
	Error       []byte           `db:"error"`
	ExitCode    *int32           `db:"exit_code"`
	Leased      pgtype.Timestamp `db:"leased"`
	Debug       []byte           `db:"debug"`
	Pool        *string          `db:"pool"`
}

type JobSpec struct {
	JobID   string `db:"job_id"`
	JobSpec []byte `db:"job_spec"`
}

type Queue struct {
	Name       string `db:"name"`
	Definition []byte `db:"definition"`
}
