package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	log "github.com/armadaproject/armada/internal/common/logging"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/lookoutingester/model"
	"k8s.io/utils/pointer"
	"time"
)

type LookoutClickhouseDb struct {
	Db clickhouse.Conn
}

func NewLookoutClickhouseDb(db clickhouse.Conn) *LookoutClickhouseDb {
	return &LookoutClickhouseDb{
		Db: db,
	}
}

func (l *LookoutClickhouseDb) Store(ctx *armadacontext.Context, instructions *model.InstructionSet) error {
	start := time.Now()
	rows := make([]JobRow, 0, len(instructions.JobsToUpdate)+len(instructions.JobsToCreate)+len(instructions.JobRunsToCreate)+len(instructions.JobRunsToUpdate))
	rows = append(rows, armadaslices.Map(instructions.JobsToCreate, FromCreateJob)...)
	rows = append(rows, armadaslices.Map(instructions.JobsToUpdate, FromUpdateJob)...)
	rows = append(rows, armadaslices.Map(instructions.JobRunsToCreate, FromCreateJobRun)...)
	rows = append(rows, armadaslices.Map(instructions.JobRunsToUpdate, FromUpdateJobRun)...)
	err := InsertJobsBatch(ctx, l.Db, rows)
	if err != nil {
		return err
	}
	log.Infof("Stored %d jobs updates in %s", len(rows), time.Since(start))
	return nil
}

type JobRow struct {
	JobId              string            `ch:"job_id"`
	LastTransitionTime *time.Time        `ch:"last_transition_time"`
	Queue              *string           `ch:"queue"`
	Namespace          *string           `ch:"namespace"`
	JobSet             *string           `ch:"job_set"`
	Cpu                *int64            `ch:"cpu"`
	Memory             *int64            `ch:"memory"`
	EphemeralStorage   *int64            `ch:"ephemeral_storage"`
	Gpu                *int64            `ch:"gpu"`
	Priority           *int64            `ch:"priority"`
	Submitted          *time.Time        `ch:"submitted"`
	PriorityClass      *string           `ch:"priority_class"`
	Annotations        map[string]string `ch:"annotations"`
	JobState           *int32            `ch:"job_state"`
	Cancelled          *time.Time        `ch:"cancelled"`
	CancelReason       *string           `ch:"cancel_reason"`
	CancelUser         *string           `ch:"cancel_user"`

	LatestRunId *string    `ch:"latest_run_id"`
	RunCluster  *string    `ch:"run_cluster"`
	RunExitCode *int32     `ch:"run_exit_code"`
	RunFinished *time.Time `ch:"run_finished"`
	RunState    *int32     `ch:"run_state"`
	RunNode     *string    `ch:"run_node"`
	RunLeased   *time.Time `ch:"run_leased"`
	RunPending  *time.Time `ch:"run_pending"`
	RunStarted  *time.Time `ch:"run_started"`
}

func FromCreateJob(inst *model.CreateJobInstruction) JobRow {
	return JobRow{
		JobId:              inst.JobId,
		Queue:              &inst.Queue,
		Namespace:          &inst.Namespace,
		JobSet:             &inst.JobSet,
		Cpu:                &inst.Cpu,
		Memory:             &inst.Memory,
		EphemeralStorage:   &inst.EphemeralStorage,
		Gpu:                &inst.Gpu,
		Priority:           &inst.Priority,
		Submitted:          &inst.Submitted,
		PriorityClass:      inst.PriorityClass,
		Annotations:        inst.Annotations,
		JobState:           &inst.State,
		LastTransitionTime: &inst.LastTransitionTime,
	}
}

func FromUpdateJob(inst *model.UpdateJobInstruction) JobRow {
	return JobRow{
		JobId:              inst.JobId,
		LastTransitionTime: inst.LastTransitionTime,
		Priority:           inst.Priority,
		JobState:           inst.State,
		Cancelled:          inst.Cancelled,
		CancelReason:       inst.CancelReason,
		CancelUser:         inst.CancelUser,
	}
}

func FromCreateJobRun(inst *model.CreateJobRunInstruction) JobRow {
	return JobRow{
		JobId:              inst.JobId,
		LastTransitionTime: inst.Leased,
		JobState:           pointer.Int32(lookout.JobLeasedOrdinal),
		LatestRunId:        &inst.RunId,
		RunCluster:         &inst.Cluster,
		RunState:           pointer.Int32(lookout.JobRunLeasedOrdinal),
		RunNode:            inst.Node,
		RunLeased:          inst.Leased,
		RunPending:         inst.Pending,
	}
}

func FromUpdateJobRun(inst *model.UpdateJobRunInstruction) JobRow {
	return JobRow{
		JobId:              inst.JobId,
		LastTransitionTime: inst.LastTransitionTime,
		RunExitCode:        inst.ExitCode,
		RunFinished:        inst.Finished,
		RunState:           inst.JobRunState,
		RunPending:         inst.Pending,
		RunStarted:         inst.Started,
	}
}

func InsertJobsBatch(ctx context.Context, db clickhouse.Conn, rows []JobRow) error {
	batch, err := db.PrepareBatch(ctx, "INSERT INTO jobs")
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
