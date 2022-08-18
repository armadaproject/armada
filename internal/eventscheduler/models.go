// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.13.0

package eventscheduler

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
)

type Executor struct {
	ID             string      `db:"id"`
	TotalResources pgtype.JSON `db:"total_resources"`
	MaxResources   pgtype.JSON `db:"max_resources"`
}

type Job struct {
	JobID        uuid.UUID `db:"job_id"`
	JobSet       string    `db:"job_set"`
	Queue        string    `db:"queue"`
	Priority     int64     `db:"priority"`
	Message      []byte    `db:"message"`
	MessageIndex int64     `db:"message_index"`
}

type Pulsar struct {
	Topic        string `db:"topic"`
	LedgerID     int64  `db:"ledger_id"`
	EntryID      int64  `db:"entry_id"`
	BatchIdx     int32  `db:"batch_idx"`
	PartitionIdx int32  `db:"partition_idx"`
}

type Queue struct {
	Name   string  `db:"name"`
	Weight float64 `db:"weight"`
}

type Run struct {
	RunID        uuid.UUID   `db:"run_id"`
	JobID        uuid.UUID   `db:"job_id"`
	Executor     string      `db:"executor"`
	Assignment   pgtype.JSON `db:"assignment"`
	Deleted      bool        `db:"deleted"`
	LastModified time.Time   `db:"last_modified"`
}
