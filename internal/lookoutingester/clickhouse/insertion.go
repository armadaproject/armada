package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
	"k8s.io/utils/pointer"
	"time"
)

type JobRow struct {
	JobId              string     `ch:"job_id"`
	EventTime          time.Time  `ch:"event_time"`
	Queue              *string    `ch:"queue"`
	Namespace          *string    `ch:"namespace"`
	JobSet             *string    `ch:"job_set"`
	Cpu                *int64     `ch:"cpu"`
	Memory             *int64     `ch:"memory"`
	EphemeralStorage   *int64     `ch:"ephemeral_storage"`
	Gpu                *int64     `ch:"gpu"`
	Priority           *int64     `ch:"priority"`
	Submitted          *time.Time `ch:"submitted"`
	PriorityClass      *string    `ch:"priority_class"`
	Annotations        *string    `ch:"annotations"`
	JobState           *int32     `ch:"job_state"`
	Cancelled          *time.Time `ch:"cancelled"`
	CancelReason       *string    `ch:"cancel_reason"`
	CancelUser         *string    `ch:"cancel_user"`
	LastTransitionTime *time.Time `ch:"last_transition_time"`
	LatestRunId        *string    `ch:"latest_run_id"`
	RunCluster         *string    `ch:"run_cluster"`
	RunExitCode        *int32     `ch:"run_exit_code"`
	RunFinished        *time.Time `ch:"run_finished"`
	RunState           *int32     `ch:"run_state"`
	RunNode            *string    `ch:"run_node"`
	RunLeased          *time.Time `ch:"run_leased"`
	RunPending         *time.Time `ch:"run_pending"`
	RunStarted         *time.Time `ch:"run_started"`
}

func FromCreateJob(inst model.CreateJobInstruction) JobRow {
	eventTime := inst.LastTransitionTime
	return JobRow{
		JobId:              inst.JobId,
		EventTime:          eventTime,
		Queue:              pointer.String(inst.Queue),
		Namespace:          nullableString(inst.Namespace),
		JobSet:             pointer.String(inst.JobSet),
		Cpu:                pointer.Int64(inst.Cpu),
		Memory:             pointer.Int64(inst.Memory),
		EphemeralStorage:   pointer.Int64(inst.EphemeralStorage),
		Gpu:                pointer.Int64(inst.Gpu),
		Priority:           pointer.Int64(inst.Priority),
		Submitted:          &inst.Submitted,
		PriorityClass:      inst.PriorityClass,
		Annotations:        nil,
		JobState:           pointer.Int32(inst.State),
		LastTransitionTime: &eventTime,
	}
}

func FromUpdateJob(inst model.UpdateJobInstruction) JobRow {
	eventTime := *inst.LastTransitionTime
	log.Infof("eventTime: %s", eventTime)
	return JobRow{
		JobId:              inst.JobId,
		EventTime:          eventTime,
		Priority:           inst.Priority,
		JobState:           inst.State,
		Cancelled:          inst.Cancelled,
		CancelReason:       inst.CancelReason,
		CancelUser:         inst.CancelUser,
		LastTransitionTime: inst.LastTransitionTime,
	}
}

func InsertJobsBatch(ctx context.Context, db clickhouse.Conn, rows []JobRow) error {
	batch, err := db.PrepareBatch(ctx, "INSERT INTO job_events")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, row := range rows {
		if err := batch.AppendStruct(&row); err != nil {
			return fmt.Errorf("append row: %w", err)
		}
	}

	return batch.Send()
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
