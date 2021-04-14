package repository

import (
	"encoding/json"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
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
		With("run_states", r.getRunStateCounts(job.Id)).
		Rows(goqu.Record{
			"job_id":    job.Id,
			"queue":     job.Queue,
			"owner":     job.Owner,
			"jobset":    job.JobSetId,
			"priority":  job.Priority,
			"submitted": ToUTC(job.Created),
			"job":       jobJson,
			"state":     JobStateToIntMap[JobQueued],
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"queue":     job.Queue,
			"owner":     job.Owner,
			"jobset":    job.JobSetId,
			"priority":  job.Priority,
			"submitted": ToUTC(job.Created),
			"job":       jobJson,
			"state":     r.determineJobState(),
		}))

	_, err = ds.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) MarkCancelled(event *api.JobCancelledEvent) error {
	ds := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":    event.JobId,
			"queue":     event.Queue,
			"jobset":    event.JobSetId,
			"cancelled": ToUTC(event.Created),
			"state":     JobStateToIntMap[JobCancelled],
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"queue":     event.Queue,
			"jobset":    event.JobSetId,
			"cancelled": ToUTC(event.Created),
			"state":     JobStateToIntMap[JobCancelled],
		}))

	_, err := ds.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) RecordJobPriorityChange(event *api.JobReprioritizedEvent) error {
	panic("implement me")
}

func (r *SQLJobStore) RecordJobPending(event *api.JobPendingEvent) error {
	if err := r.upsertJobRun(goqu.Record{
		"run_id":     event.GetKubernetesId(),
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"created":    ToUTC(event.GetCreated()),
	}); err != nil {
		return err
	}

	jobDs := r.db.Insert(jobTable).
		With("run_states", r.getRunStateCounts(event.GetJobId())).
		Rows(goqu.Record{
			"job_id": event.JobId,
			"queue":  event.Queue,
			"jobset": event.JobSetId,
			"state":  JobStateToIntMap[JobPending],
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"queue":  event.Queue,
			"jobset": event.JobSetId,
			"state":  r.determineJobState(),
		}))

	_, err := jobDs.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) RecordJobRunning(event *api.JobRunningEvent) error {
	jobRunRecord := goqu.Record{
		"run_id":     event.GetKubernetesId(),
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"started":    ToUTC(event.GetCreated()),
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}
	if err := r.upsertJobRun(jobRunRecord); err != nil {
		return err
	}

	jobDs := r.db.Insert(jobTable).
		With("run_states", r.getRunStateCounts(event.GetJobId())).
		Rows(goqu.Record{
			"job_id": event.JobId,
			"queue":  event.Queue,
			"jobset": event.JobSetId,
			"state":  JobStateToIntMap[JobRunning],
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"state": r.determineJobState(),
		}))

	_, err := jobDs.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) RecordJobSucceeded(event *api.JobSucceededEvent) error {
	jobRunRecord := goqu.Record{
		"run_id":     event.GetKubernetesId(),
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"finished":   ToUTC(event.GetCreated()),
		"succeeded":  true,
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}
	if err := r.upsertJobRun(jobRunRecord); err != nil {
		return err
	}

	ds := r.db.Insert(jobTable).
		With("run_states", r.getRunStateCounts(event.GetJobId())).
		Rows(goqu.Record{
			"job_id": event.JobId,
			"queue":  event.Queue,
			"jobset": event.JobSetId,
			"state":  JobStateToIntMap[JobSucceeded],
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"state": r.determineJobState(),
		}))

	_, err := ds.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) RecordJobFailed(event *api.JobFailedEvent) error {
	// If job fails before a pod is created, we generate a new ULID
	k8sId := event.KubernetesId
	if k8sId == "" {
		k8sId = util.NewULID() + "-nopod"
	}

	jobRunRecord := goqu.Record{
		"run_id":     k8sId,
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"finished":   ToUTC(event.GetCreated()),
		"succeeded":  false,
		"error":      fmt.Sprintf("%.2048s", event.GetReason()),
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}
	if err := r.upsertJobRun(jobRunRecord); err != nil {
		return err
	}

	jobDs := r.db.Insert(jobTable).
		With("run_states", r.getRunStateCounts(event.GetJobId())).
		Rows(goqu.Record{
			"job_id": event.JobId,
			"queue":  event.Queue,
			"jobset": event.JobSetId,
			"state":  JobStateToIntMap[JobFailed],
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"state": r.determineJobState(),
		}))

	if _, err := jobDs.Prepared(true).Executor().Exec(); err != nil {
		return err
	}

	return r.upsertContainers(k8sId, event.ExitCodes)
}

func (r *SQLJobStore) RecordJobUnableToSchedule(event *api.JobUnableToScheduleEvent) error {
	jobRunRecord := goqu.Record{
		"run_id":             event.GetKubernetesId(),
		"job_id":             event.GetJobId(),
		"cluster":            event.GetClusterId(),
		"pod_number":         event.GetPodNumber(),
		"finished":           ToUTC(event.GetCreated()),
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

func (r *SQLJobStore) determineJobState() exp.CaseExpression {
	return goqu.Case().
		When(job_state.Eq(stateAsLiteral(JobCancelled)), stateAsLiteral(JobCancelled)).
		When(r.db.Select(goqu.I("run_states.failed").Gt(0)).
			From("run_states"), stateAsLiteral(JobFailed)).
		When(r.db.Select(goqu.I("run_states.pending").Gt(0)).
			From("run_states"), stateAsLiteral(JobPending)).
		When(r.db.Select(goqu.I("run_states.running").Gt(0)).
			From("run_states"), stateAsLiteral(JobRunning)).
		When(r.db.Select(goqu.I("run_states.succeeded").Eq(goqu.I("run_states.total"))).
			From("run_states"), stateAsLiteral(JobSucceeded)).
		Else(stateAsLiteral(JobQueued))
}

func (r *SQLJobStore) getRunStateCounts(jobId string) *goqu.SelectDataset {
	ds := r.db.Select(
		goqu.COUNT("*").As("total"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 1)").As("queued"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 2)").As("pending"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 3)").As("running"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 4)").As("succeeded"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 5)").As("failed")).
		From(
			// State based on latest run for each pod
			r.db.Select(
				goqu.Case().
					When(goqu.And(
						jobRun_finished.IsNotNull(),
						jobRun_succeeded.IsTrue()), stateAsLiteral(JobSucceeded)).
					When(goqu.And(
						jobRun_finished.IsNotNull(),
						jobRun_succeeded.IsFalse()), stateAsLiteral(JobFailed)).
					When(jobRun_started.IsNotNull(), stateAsLiteral(JobRunning)).
					When(jobRun_created.IsNotNull(), stateAsLiteral(JobPending)).
					Else(stateAsLiteral(JobQueued)).As("run_state")).
				From(jobRunTable).
				Distinct(jobRun_podNumber).
				Where(jobRun_jobId.Eq(jobId)).
				Order(
					jobRun_podNumber.Asc(),
					goqu.L("GREATEST(job_run.created, job_run.started, job_run.finished)").Desc()))

	return ds
}

// Avoid interpolating states
func stateAsLiteral(state JobState) exp.LiteralExpression {
	return goqu.L(fmt.Sprintf("%d", JobStateToIntMap[state]))
}
