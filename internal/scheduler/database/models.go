// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.22.0

package database

import (
	"time"

	"github.com/google/uuid"
)

type Executor struct {
	ExecutorID  string    `db:"executor_id"`
	LastRequest []byte    `db:"last_request"`
	LastUpdated time.Time `db:"last_updated"`
}

type ExecutorSetting struct {
	ExecutorID   string     `db:"executor_id"`
	Cordoned     bool       `db:"cordoned"`
	CordonReason *string    `db:"cordon_reason"`
	SetByUser    *string    `db:"set_by_user"`
	SetAtTime    *time.Time `db:"set_at_time"`
}

type Job struct {
	JobID                   string    `db:"job_id"`
	JobSet                  string    `db:"job_set"`
	Queue                   string    `db:"queue"`
	UserID                  string    `db:"user_id"`
	Submitted               int64     `db:"submitted"`
	Groups                  []byte    `db:"groups"`
	Priority                int64     `db:"priority"`
	Queued                  bool      `db:"queued"`
	QueuedVersion           int32     `db:"queued_version"`
	CancelRequested         bool      `db:"cancel_requested"`
	Cancelled               bool      `db:"cancelled"`
	CancelByJobsetRequested bool      `db:"cancel_by_jobset_requested"`
	Succeeded               bool      `db:"succeeded"`
	Failed                  bool      `db:"failed"`
	SubmitMessage           []byte    `db:"submit_message"`
	SchedulingInfo          []byte    `db:"scheduling_info"`
	SchedulingInfoVersion   int32     `db:"scheduling_info_version"`
	Serial                  int64     `db:"serial"`
	LastModified            time.Time `db:"last_modified"`
	Validated               bool      `db:"validated"`
	Pools                   []string  `db:"pools"`
}

type JobRunError struct {
	RunID string `db:"run_id"`
	JobID string `db:"job_id"`
	Error []byte `db:"error"`
}

type Marker struct {
	GroupID     uuid.UUID `db:"group_id"`
	PartitionID int32     `db:"partition_id"`
	Created     time.Time `db:"created"`
}

type Run struct {
	RunID                  string     `db:"run_id"`
	JobID                  string     `db:"job_id"`
	Created                int64      `db:"created"`
	JobSet                 string     `db:"job_set"`
	Executor               string     `db:"executor"`
	Node                   string     `db:"node"`
	Cancelled              bool       `db:"cancelled"`
	Running                bool       `db:"running"`
	Succeeded              bool       `db:"succeeded"`
	Failed                 bool       `db:"failed"`
	Returned               bool       `db:"returned"`
	RunAttempted           bool       `db:"run_attempted"`
	Serial                 int64      `db:"serial"`
	LastModified           time.Time  `db:"last_modified"`
	LeasedTimestamp        *time.Time `db:"leased_timestamp"`
	PendingTimestamp       *time.Time `db:"pending_timestamp"`
	RunningTimestamp       *time.Time `db:"running_timestamp"`
	TerminatedTimestamp    *time.Time `db:"terminated_timestamp"`
	ScheduledAtPriority    *int32     `db:"scheduled_at_priority"`
	Preempted              bool       `db:"preempted"`
	Pending                bool       `db:"pending"`
	PreemptedTimestamp     *time.Time `db:"preempted_timestamp"`
	PodRequirementsOverlay []byte     `db:"pod_requirements_overlay"`
	PreemptRequested       bool       `db:"preempt_requested"`
	Queue                  string     `db:"queue"`
	Pool                   string     `db:"pool"`
}
