package repository

import (
	"encoding/json"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/lib/pq"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

type JobRecorder interface {
	RecordJob(job *api.Job) error
	MarkCancelled(*api.JobCancelledEvent) error
	RecordJobPriorityChange(event *api.JobReprioritizedEvent) error

	RecordJobPending(event *api.JobPendingEvent) error
	RecordJobRunning(event *api.JobRunningEvent) error
	RecordJobSucceeded(event *api.JobSucceededEvent) error
	RecordJobFailed(event *api.JobFailedEvent) error
	RecordJobUnableToSchedule(event *api.JobUnableToScheduleEvent) error
}

type SQLJobStore struct {
	db *goqu.Database
}

func NewSQLJobStore(db *goqu.Database) *SQLJobStore {
	return &SQLJobStore{db: db}
}

func (r *SQLJobStore) RecordJob(job *api.Job) error {
	jobJson, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = upsert(r.db, "job",
		"job_id", []string{"queue", "owner", "jobset", "priority", "submitted", "job"},
		[]interface{}{job.Id, job.Queue, job.Owner, job.JobSetId, job.Priority, job.Created, jobJson})
	return err
}

func (r *SQLJobStore) MarkCancelled(event *api.JobCancelledEvent) error {
	_, err := upsert(r.db, "job",
		"job_id", []string{"queue", "jobset", "cancelled"},
		[]interface{}{event.JobId, event.Queue, event.JobSetId, event.Created})
	return err
}

func (r *SQLJobStore) RecordJobPriorityChange(event *api.JobReprioritizedEvent) error {
	panic("implement me")
}

func (r *SQLJobStore) RecordJobPending(event *api.JobPendingEvent) error {
	fields := []string{"created"}
	values := []interface{}{event.Created}
	return r.updateJobRun(event, event.KubernetesId, fields, values)
}

func (r *SQLJobStore) RecordJobRunning(event *api.JobRunningEvent) error {
	fields := []string{"started"}
	values := []interface{}{event.Created}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}
	return r.updateJobRun(event, event.KubernetesId, fields, values)
}

func (r *SQLJobStore) RecordJobSucceeded(event *api.JobSucceededEvent) error {
	fields := []string{"finished", "succeeded"}
	values := []interface{}{event.Created, true}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}
	return r.updateJobRun(event, event.KubernetesId, fields, values)
}

func (r *SQLJobStore) RecordJobFailed(event *api.JobFailedEvent) error {
	fields := []string{"finished", "succeeded", "error"}
	values := []interface{}{event.Created, false, fmt.Sprintf("%.2048s", event.Reason)}

	// If job fails before a pod is created, we generate a new ULID
	k8sId := event.KubernetesId
	if k8sId == "" {
		k8sId = util.NewULID() + "-nopod"
	}

	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}

	// TODO update in one call?
	for name, code := range event.ExitCodes {
		_, err := upsertCombinedKey(r.db, "job_run_container",
			[]string{"run_id", "container_name"}, []string{"exit_code"},
			[]interface{}{k8sId, name, code})
		if err != nil {
			return err
		}
	}

	return r.updateJobRun(event, k8sId, fields, values)
}

func (r *SQLJobStore) RecordJobUnableToSchedule(event *api.JobUnableToScheduleEvent) error {
	fields := []string{"finished", "unable_to_schedule"}
	values := []interface{}{event.Created, true}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}
	return r.updateJobRun(event, event.KubernetesId, fields, values)
}

func (r *SQLJobStore) updateJobRun(event api.KubernetesEvent, k8sId string, fields []string, values []interface{}) error {
	_, err := upsert(r.db, "job_run",
		"run_id", append([]string{"job_id", "cluster"}, fields...),
		append([]interface{}{k8sId, event.GetJobId(), event.GetClusterId()}, values...))
	return err
}
