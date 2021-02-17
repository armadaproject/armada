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

	ds := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":     job.Id,
			"queue":      job.Queue,
			"owner":      job.Owner,
			"jobset":     job.JobSetId,
			"priority":   job.Priority,
			"submitted":  job.Created,
			"job":        jobJson,
			"state":      JobStateToIntMap[JobQueued],
			"last_event": job.Created,
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"queue":     job.Queue,
			"owner":     job.Owner,
			"jobset":    job.JobSetId,
			"priority":  job.Priority,
			"submitted": job.Created,
			"job":       jobJson,
			"state": goqu.Case().
				When(job_lastEvent.Lte(job.Created), JobStateToIntMap[JobQueued]).
				Else(job_state),
			"last_event": goqu.Case().
				When(job_lastEvent.Lte(job.Created), job.Created).
				Else(job_lastEvent),
		}))

	_, err = ds.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) MarkCancelled(event *api.JobCancelledEvent) error {
	ds := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":     event.JobId,
			"queue":      event.Queue,
			"jobset":     event.JobSetId,
			"cancelled":  event.Created,
			"state":      JobStateToIntMap[JobCancelled],
			"last_event": event.Created,
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"queue":     event.Queue,
			"jobset":    event.JobSetId,
			"cancelled": event.Created,
			"state":     JobStateToIntMap[JobCancelled],
			"last_event": goqu.Case().
				When(job_lastEvent.Lte(event.Created), event.Created).
				Else(job_lastEvent),
		}))

	_, err := ds.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) RecordJobPriorityChange(event *api.JobReprioritizedEvent) error {
	panic("implement me")
}

func (r *SQLJobStore) RecordJobPending(event *api.JobPendingEvent) error {
	jobDs := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":     event.JobId,
			"queue":      event.Queue,
			"jobset":     event.JobSetId,
			"state":      JobStateToIntMap[JobPending],
			"last_event": event.Created,
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"queue":  event.Queue,
			"jobset": event.JobSetId,
			"state": goqu.Case().
				When(job_lastEvent.Lte(event.Created), JobStateToIntMap[JobPending]).
				Else(job_state),
			"last_event": goqu.Case().
				When(job_lastEvent.Lte(event.Created), event.Created).
				Else(job_lastEvent),
		}))

	if _, err := jobDs.Prepared(true).Executor().Exec(); err != nil {
		return err
	}

	return r.upsertJobRun(goqu.Record{
		"run_id":     event.GetKubernetesId(),
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"created":    event.GetCreated(),
	})
}

func (r *SQLJobStore) RecordJobRunning(event *api.JobRunningEvent) error {
	jobDs := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":     event.JobId,
			"queue":      event.Queue,
			"jobset":     event.JobSetId,
			"state":      JobStateToIntMap[JobRunning],
			"last_event": event.Created,
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"state": goqu.Case().
				When(job_lastEvent.Lte(event.Created), JobStateToIntMap[JobRunning]).
				Else(job_state),
			"last_event": goqu.Case().
				When(job_lastEvent.Lte(event.Created), event.Created).
				Else(job_lastEvent),
		}))

	if _, err := jobDs.Prepared(true).Executor().Exec(); err != nil {
		return err
	}

	jobRunRecord := goqu.Record{
		"run_id":     event.GetKubernetesId(),
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"started":    event.GetCreated(),
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}
	return r.upsertJobRun(jobRunRecord)
}

func (r *SQLJobStore) RecordJobSucceeded(event *api.JobSucceededEvent) error {
	ds := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":     event.JobId,
			"queue":      event.Queue,
			"jobset":     event.JobSetId,
			"state":      JobStateToIntMap[JobSucceeded],
			"last_event": event.Created,
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"state": goqu.Case().
				When(job_state.Neq(JobStateToIntMap[JobFailed]), JobStateToIntMap[JobSucceeded]).
				Else(job_state),
			"last_event": goqu.Case().
				When(job_lastEvent.Lte(event.Created), event.Created).
				Else(job_lastEvent),
		}))

	if _, err := ds.Prepared(true).Executor().Exec(); err != nil {
		return err
	}

	jobRunRecord := goqu.Record{
		"run_id":     event.GetKubernetesId(),
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"finished":   event.GetCreated(),
		"succeeded":  true,
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}
	return r.upsertJobRun(jobRunRecord)
}

func (r *SQLJobStore) RecordJobFailed(event *api.JobFailedEvent) error {
	jobDs := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":     event.JobId,
			"queue":      event.Queue,
			"jobset":     event.JobSetId,
			"state":      JobStateToIntMap[JobFailed],
			"last_event": event.Created,
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"state": JobStateToIntMap[JobFailed],
			"last_event": goqu.Case().
				When(job_lastEvent.Lte(event.Created), event.Created).
				Else(job_lastEvent),
		}))

	if _, err := jobDs.Prepared(true).Executor().Exec(); err != nil {
		return err
	}

	// If job fails before a pod is created, we generate a new ULID
	k8sId := event.KubernetesId
	if k8sId == "" {
		k8sId = util.NewULID() + "-nopod"
	}

	if err := r.upsertContainers(k8sId, event.ExitCodes); err != nil {
		return err
	}

	jobRunRecord := goqu.Record{
		"run_id":     k8sId,
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"finished":   event.GetCreated(),
		"succeeded":  false,
		"error":      fmt.Sprintf("%.2048s", event.GetReason()),
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}
	return r.upsertJobRun(jobRunRecord)
}

func (r *SQLJobStore) RecordJobUnableToSchedule(event *api.JobUnableToScheduleEvent) error {
	jobRunRecord := goqu.Record{
		"run_id":             event.GetKubernetesId(),
		"job_id":             event.GetJobId(),
		"cluster":            event.GetClusterId(),
		"pod_number":         event.GetPodNumber(),
		"finished":           event.GetCreated(),
		"unable_to_schedule": true,
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}
	return r.upsertJobRun(jobRunRecord)
}

func (r *SQLJobStore) upsertJobRun(record goqu.Record) error {
	return upsert(r.db, jobRunTable, []string{"run_id"}, []goqu.Record{record})
}

func (r *SQLJobStore) upsertContainers(k8sId string, exitCodes map[string]int32) error {
	containerRecords := make([]goqu.Record, len(exitCodes))
	i := 0
	for name, code := range exitCodes {
		containerRecords[i] = goqu.Record{
			"run_id":         k8sId,
			"container_name": name,
			"exit_code":      code,
		}
		i++
	}

	return upsert(r.db, jobRunContainerTable, []string{"run_id", "container_name"}, containerRecords)
}
