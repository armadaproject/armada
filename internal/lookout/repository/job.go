package repository

import (
	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq"

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
}

type SQLJobRepository struct {
	db *sql.DB
}

func NewSQLJobRepository(db *sql.DB) *SQLJobRepository {
	return &SQLJobRepository{db: db}
}

func (r *SQLJobRepository) RecordJob(job *api.Job) error {
	jobJson, err := json.Marshal(job)
	if err != nil {
		return err
	}
	_, err = upsert(r.db, "job",
		"job_id", []string{"queue", "jobset", "priority", "submitted", "job"},
		[]interface{}{job.Id, job.Queue, job.JobSetId, job.Priority, job.Created, jobJson})
	return err
}

func (r *SQLJobRepository) MarkCancelled(event *api.JobCancelledEvent) error {
	_, err := upsert(r.db, "job",
		"job_id", []string{"queue", "jobset", "cancelled"},
		[]interface{}{event.JobId, event.Queue, event.JobSetId, event.Created})
	return err
}

func (r *SQLJobRepository) RecordJobPriorityChange(event *api.JobReprioritizedEvent) error {
	panic("implement me")
}

func (r *SQLJobRepository) RecordJobPending(event *api.JobPendingEvent) error {
	fields := []string{"created"}
	values := []interface{}{event.Created}
	return r.updateJobRun(event, fields, values)
}

func (r *SQLJobRepository) RecordJobRunning(event *api.JobRunningEvent) error {
	fields := []string{"started"}
	values := []interface{}{event.Created}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}
	return r.updateJobRun(event, fields, values)
}

func (r *SQLJobRepository) RecordJobSucceeded(event *api.JobSucceededEvent) error {
	fields := []string{"finished", "succeeded"}
	values := []interface{}{event.Created, true}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}
	return r.updateJobRun(event, fields, values)
}

func (r *SQLJobRepository) RecordJobFailed(event *api.JobFailedEvent) error {
	fields := []string{"finished", "succeeded", "error"}
	values := []interface{}{event.Created, false, event.Reason}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}

	// TODO update in one call?
	for name, code := range event.ExitCodes {
		_, err := upsertCombinedKey(r.db, "job_run_container",
			[]string{"run_id", "container_name"}, []string{"exit_code"},
			[]interface{}{event.KubernetesId, name, code})
		if err != nil {
			return err
		}
	}

	return r.updateJobRun(event, fields, values)
}

func (r *SQLJobRepository) updateJobRun(event api.KubernetesEvent, fields []string, values []interface{}) error {
	_, err := upsert(r.db, "job_run",
		"run_id", append([]string{"job_id", "cluster"}, fields...),
		append([]interface{}{event.GetKubernetesId(), event.GetJobId(), event.GetClusterId()}, values...))
	return err
}
