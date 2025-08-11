package clickhouseingester

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type JobRow struct {
	JobId              string            `ch:"job_id"`
	Queue              *string           `ch:"queue"`
	Namespace          *string           `ch:"namespace"`
	JobSet             *string           `ch:"job_set"`
	CPU                *int64            `ch:"cpu"`
	Memory             *int64            `ch:"memory"`
	EphemeralStorage   *int64            `ch:"ephemeral_storage"`
	GPU                *int64            `ch:"gpu"`
	Priority           *int64            `ch:"priority"`
	SubmitTs           *time.Time        `ch:"submit_ts"`
	PriorityClass      *string           `ch:"priority_class"`
	Annotations        map[string]string `ch:"annotations"`
	JobState           *string           `ch:"job_state"`
	CancelTs           *time.Time        `ch:"cancel_ts"`
	CancelReason       *string           `ch:"cancel_reason"`
	CancelUser         *string           `ch:"cancel_user"`
	LatestRunId        *string           `ch:"latest_run_id"`
	RunCluster         *string           `ch:"run_cluster"`
	RunExitCode        *int32            `ch:"run_exit_code"`
	RunFinishedTs      *time.Time        `ch:"run_finished_ts"`
	RunState           *string           `ch:"run_state"`
	RunNode            *string           `ch:"run_node"`
	RunLeasedTs        *time.Time        `ch:"run_leased"`
	RunPendingTs       *time.Time        `ch:"run_pending_ts"`
	RunStartedTs       *time.Time        `ch:"run_started_ts"`
	LastTransitionTime *time.Time        `ch:"last_transition_time"`
	LastUpdateTs       time.Time         `ch:"last_update_ts"`
	Error              *string           `ch:"error"`
	Merged             *bool             `ch:"merged"`
}

type JobRunRow struct {
	JobId      string     `ch:"job_id"`
	RunId      string     `ch:"run_id"`
	Cluster    *string    `ch:"cluster"`
	ExitCode   *int32     `ch:"exit_code"`
	State      *string    `ch:"state"`
	Node       *string    `ch:"run_node"`
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

type jobResources struct {
	Cpu              int64
	Memory           int64
	EphemeralStorage int64
	Gpu              int64
}
