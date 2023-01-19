package repository

import (
	"fmt"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

type JobRecorder interface {
	RecordJob(job *api.Job, timestamp time.Time) error
	MarkCancelled(*api.JobCancelledEvent) error

	RecordJobPending(event *api.JobPendingEvent) error
	RecordJobRunning(event *api.JobRunningEvent) error
	RecordJobSucceeded(event *api.JobSucceededEvent) error
	RecordJobFailed(event *api.JobFailedEvent) error
	RecordJobUnableToSchedule(event *api.JobUnableToScheduleEvent) error
	RecordJobDuplicate(event *api.JobDuplicateFoundEvent) error
	RecordJobTerminated(event *api.JobTerminatedEvent) error
	RecordJobReprioritized(event *api.JobReprioritizedEvent) error
}

type SQLJobStore struct {
	db                   *goqu.Database
	userAnnotationPrefix string
}

func NewSQLJobStore(db *goqu.Database, annotationPrefix string) *SQLJobStore {
	return &SQLJobStore{db: db, userAnnotationPrefix: annotationPrefix}
}

func (r *SQLJobStore) RecordJob(job *api.Job, timestamp time.Time) error {
	jobMarshalled, err := job.Marshal()
	if err != nil {
		return err
	}

	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		ds := tx.Insert(jobTable).
			With("run_states", getRunStateCounts(tx, job.Id)).
			Rows(goqu.Record{
				"job_id":        job.Id,
				"queue":         job.Queue,
				"owner":         job.Owner,
				"jobset":        job.JobSetId,
				"priority":      job.Priority,
				"submitted":     ToUTC(job.Created),
				"orig_job_spec": jobMarshalled,
				"state":         JobStateToIntMap[JobQueued],
				"job_updated":   timestamp,
			}).
			OnConflict(goqu.DoUpdate("job_id", goqu.Record{
				"queue":         job.Queue,
				"owner":         job.Owner,
				"jobset":        job.JobSetId,
				"priority":      job.Priority,
				"submitted":     ToUTC(job.Created),
				"orig_job_spec": jobMarshalled,
				"state":         determineJobState(tx),
				"job_updated":   timestamp,
			}).Where(job_jobUpdated.Lt(timestamp)))

		res, err := ds.Prepared(true).Executor().Exec()
		if err != nil {
			return err
		}

		rowsChanged, err := res.RowsAffected()
		if err != nil {
			return err
		}

		if rowsChanged == 0 {
			return nil
		}

		return upsertUserAnnotations(tx, r.userAnnotationPrefix, job.Id, job.Annotations)
	})
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

func (r *SQLJobStore) RecordJobReprioritized(event *api.JobReprioritizedEvent) error {
	updatedJobProto, err := r.getReprioritizedJobProto(event)
	if err != nil {
		return err
	}

	ds := r.db.Insert(jobTable).
		Rows(goqu.Record{
			"job_id":        event.JobId,
			"queue":         event.Queue,
			"jobset":        event.JobSetId,
			"priority":      event.NewPriority,
			"orig_job_spec": updatedJobProto,
		}).
		OnConflict(goqu.DoUpdate("job_id", goqu.Record{
			"queue":         event.Queue,
			"jobset":        event.JobSetId,
			"priority":      event.NewPriority,
			"orig_job_spec": updatedJobProto,
		}))

	_, err = ds.Prepared(true).Executor().Exec()
	return err
}

func (r *SQLJobStore) RecordJobDuplicate(event *api.JobDuplicateFoundEvent) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		ds := tx.Insert(jobTable).
			With("run_states", getRunStateCounts(tx, event.GetJobId())).
			Rows(goqu.Record{
				"job_id":    event.JobId,
				"queue":     event.Queue,
				"jobset":    event.JobSetId,
				"duplicate": true,
				"state":     JobStateToIntMap[JobDuplicate],
			}).
			OnConflict(goqu.DoUpdate("job_id", goqu.Record{
				"state":     JobStateToIntMap[JobDuplicate],
				"duplicate": true,
			}))

		_, err := ds.Prepared(true).Executor().Exec()
		return err
	})
}

func (r *SQLJobStore) RecordJobPending(event *api.JobPendingEvent) error {
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		if err := upsertJobRun(tx, goqu.Record{
			"run_id":     event.GetKubernetesId(),
			"job_id":     event.GetJobId(),
			"cluster":    event.GetClusterId(),
			"pod_number": event.GetPodNumber(),
			"created":    ToUTC(event.GetCreated()),
		}); err != nil {
			return err
		}

		jobDs := tx.Insert(jobTable).
			With("run_states", getRunStateCounts(tx, event.GetJobId())).
			Rows(goqu.Record{
				"job_id": event.JobId,
				"queue":  event.Queue,
				"jobset": event.JobSetId,
				"state":  JobStateToIntMap[JobPending],
			}).
			OnConflict(goqu.DoUpdate("job_id", goqu.Record{
				"queue":  event.Queue,
				"jobset": event.JobSetId,
				"state":  determineJobState(tx),
			}))

		_, err := jobDs.Prepared(true).Executor().Exec()
		return err
	})
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

	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		if err := upsertJobRun(tx, jobRunRecord); err != nil {
			return err
		}

		jobDs := tx.Insert(jobTable).
			With("run_states", getRunStateCounts(tx, event.GetJobId())).
			Rows(goqu.Record{
				"job_id": event.JobId,
				"queue":  event.Queue,
				"jobset": event.JobSetId,
				"state":  JobStateToIntMap[JobRunning],
			}).
			OnConflict(goqu.DoUpdate("job_id", goqu.Record{
				"state": determineJobState(tx),
			}))

		_, err := jobDs.Prepared(true).Executor().Exec()
		return err
	})
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

	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		if err := upsertJobRun(tx, jobRunRecord); err != nil {
			return err
		}

		ds := tx.Insert(jobTable).
			With("run_states", getRunStateCounts(tx, event.GetJobId())).
			Rows(goqu.Record{
				"job_id": event.JobId,
				"queue":  event.Queue,
				"jobset": event.JobSetId,
				"state":  JobStateToIntMap[JobSucceeded],
			}).
			OnConflict(goqu.DoUpdate("job_id", goqu.Record{
				"state": determineJobState(tx),
			}))

		_, err := ds.Prepared(true).Executor().Exec()
		return err
	})
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
		"error":      util.Truncate(util.RemoveNullsFromString(event.GetReason()), util.MaxMessageLength),
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}

	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		if err := upsertJobRun(tx, jobRunRecord); err != nil {
			return err
		}

		jobDs := tx.Insert(jobTable).
			With("run_states", getRunStateCounts(tx, event.GetJobId())).
			Rows(goqu.Record{
				"job_id": event.JobId,
				"queue":  event.Queue,
				"jobset": event.JobSetId,
				"state":  JobStateToIntMap[JobFailed],
			}).
			OnConflict(goqu.DoUpdate("job_id", goqu.Record{
				"state": determineJobState(tx),
			}))

		if _, err := jobDs.Prepared(true).Executor().Exec(); err != nil {
			return err
		}

		return upsertContainers(tx, k8sId, event.ExitCodes)
	})
}

func (r *SQLJobStore) RecordJobUnableToSchedule(event *api.JobUnableToScheduleEvent) error {
	if event.GetKubernetesId() == "" {
		event.KubernetesId = util.NewULID() + "-nopod"
	}

	jobRunRecord := goqu.Record{
		"run_id":             event.GetKubernetesId(),
		"job_id":             event.GetJobId(),
		"cluster":            event.GetClusterId(),
		"pod_number":         event.GetPodNumber(),
		"finished":           ToUTC(event.GetCreated()),
		"unable_to_schedule": true,
		"error":              util.Truncate(util.RemoveNullsFromString(event.GetReason()), util.MaxMessageLength),
	}
	if event.GetNodeName() != "" {
		jobRunRecord["node"] = event.GetNodeName()
	}

	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		return upsertJobRun(tx, jobRunRecord)
	})
}

func (r *SQLJobStore) RecordJobTerminated(event *api.JobTerminatedEvent) error {
	jobRunRecord := goqu.Record{
		"run_id":     event.GetKubernetesId(),
		"job_id":     event.GetJobId(),
		"cluster":    event.GetClusterId(),
		"pod_number": event.GetPodNumber(),
		"finished":   ToUTC(event.GetCreated()),
		"succeeded":  false,
		"error":      util.Truncate(util.RemoveNullsFromString(event.GetReason()), util.MaxMessageLength),
	}

	tx, err := r.db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		return upsertJobRun(tx, jobRunRecord)
	})
}

func (r *SQLJobStore) getReprioritizedJobProto(event *api.JobReprioritizedEvent) ([]byte, error) {
	selectDs := r.db.From(jobTable).
		Select(job_orig_job_spec).
		Where(job_jobId.Eq(event.JobId))

	jobsInQueueRows := make([]*JobRow, 0)
	err := selectDs.Prepared(true).ScanStructs(&jobsInQueueRows)
	if err != nil {
		return nil, err
	}
	if len(jobsInQueueRows) == 0 {
		return nil, nil
	}

	var jobFromProto api.Job
	jobProto := jobsInQueueRows[0].OrigJobSpec
	err = jobFromProto.Unmarshal(jobProto)
	if err != nil {
		return nil, err
	}

	jobFromProto.Priority = event.NewPriority
	updatedJobProto, err := jobFromProto.Marshal()
	if err != nil {
		return nil, nil
	}
	return updatedJobProto, nil
}

func upsertJobRun(tx *goqu.TxDatabase, record goqu.Record) error {
	return upsert(tx, jobRunTable, []string{"run_id"}, []goqu.Record{record})
}

func upsertContainers(tx *goqu.TxDatabase, k8sId string, exitCodes map[string]int32) error {
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

	return upsert(tx, jobRunContainerTable, []string{"run_id", "container_name"}, containerRecords)
}

func upsertUserAnnotations(tx *goqu.TxDatabase, userAnnotationPrefix string, jobId string, annotations map[string]string) error {
	trimmedKeys := []string{}
	var annotationRecords []goqu.Record
	for key, value := range annotations {
		if strings.HasPrefix(key, userAnnotationPrefix) && len(key) > len(userAnnotationPrefix) {
			trimmedKey := key[len(userAnnotationPrefix):]
			trimmedKeys = append(trimmedKeys, trimmedKey)
			annotationRecords = append(annotationRecords, goqu.Record{
				"job_id": jobId,
				"key":    trimmedKey,
				"value":  value,
			})
		}
	}

	deleteWhereClause := []exp.Expression{annotation_jobId.Eq(jobId)}
	if len(trimmedKeys) > 0 {
		deleteWhereClause = append(deleteWhereClause, annotation_key.NotIn(trimmedKeys))
	}

	_, err := tx.Delete(userAnnotationLookupTable).Where(deleteWhereClause...).Prepared(true).Executor().Exec()
	if err != nil {
		return err
	}
	return upsert(tx, userAnnotationLookupTable, []string{"job_id", "key"}, annotationRecords)
}

func determineJobState(tx *goqu.TxDatabase) exp.CaseExpression {
	return goqu.Case().
		When(job_duplicate.Eq(true), stateAsLiteral(JobDuplicate)).
		When(job_state.Eq(stateAsLiteral(JobCancelled)), stateAsLiteral(JobCancelled)).
		When(tx.Select(goqu.I("run_states.failed").Gt(0)).
			From("run_states"), stateAsLiteral(JobFailed)).
		When(tx.Select(goqu.I("run_states.pending").Gt(0)).
			From("run_states"), stateAsLiteral(JobPending)).
		When(tx.Select(goqu.I("run_states.running").Gt(0)).
			From("run_states"), stateAsLiteral(JobRunning)).
		When(goqu.And(
			tx.Select(goqu.I("run_states.total").Gt(0)).From("run_states"),
			tx.Select(goqu.I("run_states.succeeded").Eq(goqu.I("run_states.total"))).From("run_states")), stateAsLiteral(JobSucceeded)).
		Else(stateAsLiteral(JobQueued))
}

func getRunStateCounts(tx *goqu.TxDatabase, jobId string) *goqu.SelectDataset {
	ds := tx.Select(
		goqu.COUNT("*").As("total"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 1)").As("queued"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 2)").As("pending"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 3)").As("running"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 4)").As("succeeded"),
		goqu.L("COUNT(*) FILTER (WHERE run_state = 5)").As("failed")).
		From(
			// State based on latest run for each pod
			tx.Select(
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
