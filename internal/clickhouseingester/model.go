package clickhouseingester

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type JobRow struct {
	JobID              string            `ch:"job_id"`
	Queue              *string           `ch:"queue"`
	Namespace          *string           `ch:"namespace"`
	JobSet             *string           `ch:"job_set"`
	CPU                *int64            `ch:"cpu"`
	Memory             *int64            `ch:"memory"`
	EphemeralStorage   *int64            `ch:"ephemeral_storage"`
	GPU                *int64            `ch:"gpu"`
	Priority           *int64            `ch:"priority"`
	SubmitTS           *time.Time        `ch:"submit_ts"`
	PriorityClass      *string           `ch:"priority_class"`
	Annotations        map[string]string `ch:"annotations"`
	JobState           *string           `ch:"job_state"`
	CancelTS           *time.Time        `ch:"cancel_ts"`
	CancelReason       *string           `ch:"cancel_reason"`
	CancelUser         *string           `ch:"cancel_user"`
	LatestRunID        *string           `ch:"latest_run_id"`
	RunCluster         *string           `ch:"run_cluster"`
	RunExitCode        *int32            `ch:"run_exit_code"`
	RunFinishedTS      *time.Time        `ch:"run_finished_ts"`
	RunState           *string           `ch:"run_state"`
	RunNode            *string           `ch:"run_node"`
	RunLeased          *time.Time        `ch:"run_leased"`
	RunPendingTS       *time.Time        `ch:"run_pending_ts"`
	RunStartedTS       *time.Time        `ch:"run_started_ts"`
	LastTransitionTime *time.Time        `ch:"last_transition_time"`
	LastUpdateTS       time.Time         `ch:"last_update_ts"`
	Merged             *bool             `ch:"merged"`
}

type Instructions struct {
	Rows       []JobRow
	MessageIds []pulsar.MessageID
}

func (i *Instructions) GetMessageIDs() []pulsar.MessageID {
	return i.MessageIds
}

type jobResources struct {
	Cpu              int64
	Memory           int64
	EphemeralStorage int64
	Gpu              int64
}
