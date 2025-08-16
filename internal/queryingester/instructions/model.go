package instructions

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type JobRow struct {
	JobId            string     `db:"job_id"`
	Queue            string     `db:"queue"`
	Namespace        *string    `db:"namespace"`
	JobSet           *string    `db:"job_set"`
	Cpu              *int64     `db:"cpu"`
	Memory           *int64     `db:"memory"`
	EphemeralStorage *int64     `db:"ephemeral_storage"`
	Gpu              *int64     `db:"gpu"`
	Priority         *int64     `db:"priority"`
	SubmitTs         *time.Time `db:"submit_ts"`
	PriorityClass    *string    `db:"priority_class"`
	Annotations      *string    `db:"annotations"`

	JobState           *string    `db:"job_state"`
	CancelTs           *time.Time `db:"cancel_ts"`
	CancelReason       *string    `db:"cancel_reason"`
	CancelUser         *string    `db:"cancel_user"`
	LatestRunId        *string    `db:"latest_run_id"`
	RunCluster         *string    `db:"run_cluster"`
	RunExitCode        *int32     `db:"run_exit_code"`
	RunFinishedTs      *time.Time `db:"run_finished_ts"`
	RunState           *string    `db:"run_state"`
	RunNode            *string    `db:"run_node"`
	RunLeasedTs        *time.Time `db:"run_leased_ts"`
	RunPendingTs       *time.Time `db:"run_pending_ts"`
	RunStartedTs       *time.Time `db:"run_started_ts"`
	LastTransitionTime *time.Time `db:"last_transition_time"`
	LastUpdateTs       time.Time  `db:"last_update_ts"`
	Error              *string    `db:"error"`
}

type JobRunRow struct {
	JobId      string     `ch:"job_id"`
	RunId      string     `ch:"run_id"`
	Cluster    *string    `ch:"cluster"`
	ExitCode   *int32     `ch:"exit_code"`
	State      *string    `ch:"state"`
	Node       *string    `ch:"node"`
	LeasedTs   *time.Time `ch:"leased_ts"`
	PendingTs  *time.Time `ch:"pending_ts"`
	StartedTS  *time.Time `ch:"started_ts"`
	FinishedTS *time.Time `ch:"finished_ts"`
	Merged     *bool      `ch:"merged"`
}

type JobErrorRow struct {
	RunId        string `ch:"run_id"`
	ErrorMessage string `ch:"error_message"`
}

type JobDebugRow struct {
	RunId        string `ch:"run_id"`
	DebugMessage string `ch:"error_message"`
}

type JobSpecRow struct {
	JobId   string `ch:"run_id"`
	JobSpec string `ch:"jobSpec"`
}

type Instructions struct {
	Jobs       []JobRow
	JobRuns    []JobRunRow
	JobErrors  []JobErrorRow
	JobDebugs  []JobDebugRow
	JobSpecs   []JobSpecRow
	MessageIds []pulsar.MessageID
}

func (i *Instructions) GetMessageIDs() []pulsar.MessageID {
	return i.MessageIds
}

func (i *Instructions) Add(update Update) {
	if update.Job != nil {
		i.Jobs = append(i.Jobs, *update.Job)
	}

	if update.JobRun != nil {
		i.JobRuns = append(i.JobRuns, *update.JobRun)
	}

	if update.JobSpec != nil {
		i.JobSpecs = append(i.JobSpecs, *update.JobSpec)
	}

	if update.JobDebug != nil {
		i.JobDebugs = append(i.JobDebugs, *update.JobDebug)
	}

	if update.JobError != nil {
		i.JobErrors = append(i.JobErrors, *update.JobError)
	}
}

type Update struct {
	Job      *JobRow
	JobRun   *JobRunRow
	JobError *JobErrorRow
	JobDebug *JobDebugRow
	JobSpec  *JobSpecRow
}

type JobResources struct {
	Cpu              int64
	Memory           int64
	EphemeralStorage int64
	Gpu              int64
}
