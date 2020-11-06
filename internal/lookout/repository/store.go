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

type SQLJobStore struct {
	db *sql.DB
}

func NewSQLJobStore(db *sql.DB) *SQLJobStore {
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
	_, err := r.db.Exec(`
		UPDATE job
		SET cancelled = $1
		WHERE job.job_id = $2`, event.Created, event.JobId)
	return err
}

func (r *SQLJobStore) RecordJobPriorityChange(event *api.JobReprioritizedEvent) error {
	panic("implement me")
}

func (r *SQLJobStore) RecordJobPending(event *api.JobPendingEvent) error {
	fields := []string{"created"}
	values := []interface{}{event.Created}
	return r.updateJobRun(event, fields, values)
}

func (r *SQLJobStore) RecordJobRunning(event *api.JobRunningEvent) error {
	fields := []string{"started"}
	values := []interface{}{event.Created}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}
	return r.updateJobRun(event, fields, values)
}

func (r *SQLJobStore) RecordJobSucceeded(event *api.JobSucceededEvent) error {
	fields := []string{"finished", "succeeded"}
	values := []interface{}{event.Created, true}
	if event.NodeName != "" {
		fields = append(fields, "node")
		values = append(values, event.NodeName)
	}
	return r.updateJobRun(event, fields, values)
}

func (r *SQLJobStore) RecordJobFailed(event *api.JobFailedEvent) error {
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

func (r *SQLJobStore) updateJobRun(event api.KubernetesEvent, fields []string, values []interface{}) error {
	_, err := upsert(r.db, "job_run",
		"run_id", append([]string{"job_id", "cluster"}, fields...),
		append([]interface{}{event.GetKubernetesId(), event.GetJobId(), event.GetClusterId()}, values...))
	return err
}
