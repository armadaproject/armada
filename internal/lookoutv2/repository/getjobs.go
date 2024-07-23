package repository

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

type GetJobsRepository interface {
	GetJobs(ctx *armadacontext.Context, filters []*model.Filter, order *model.Order, skip int, take int) (*GetJobsResult, error)
}

type SqlGetJobsRepository struct {
	db            *pgxpool.Pool
	lookoutTables *LookoutTables
	clock         clock.Clock
}

type GetJobsResult struct {
	Jobs []*model.Job
}

type jobRow struct {
	jobId              string
	queue              string
	owner              string
	namespace          sql.NullString
	jobSet             string
	cpu                int64
	memory             int64
	ephemeralStorage   int64
	gpu                int64
	priority           int64
	submitted          time.Time
	cancelled          sql.NullTime
	state              int
	lastTransitionTime time.Time
	duplicate          bool
	priorityClass      sql.NullString
	latestRunId        sql.NullString
	cancelReason       sql.NullString
}

func NewSqlGetJobsRepository(db *pgxpool.Pool) *SqlGetJobsRepository {
	return &SqlGetJobsRepository{
		db:            db,
		lookoutTables: NewTables(),
		clock:         clock.RealClock{},
	}
}

func (r *SqlGetJobsRepository) GetJobs(ctx *armadacontext.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) (*GetJobsResult, error) {
	return r.getJobs(ctx, filters, activeJobSets, order, skip, take)
}

func (r *SqlGetJobsRepository) getJobs(ctx *armadacontext.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) (*GetJobsResult, error) {
	query, err := NewQueryBuilder(r.lookoutTables).GetJobs(filters, activeJobSets, order, skip, take)
	if err != nil {
		return nil, err
	}
	logQuery(query, "GetJobs")
	var jobs []*model.Job

	rows, err := r.db.Query(ctx, query.Sql, query.Args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row jobRow
		var annotations sql.NullString
		var runs sql.NullString
		if err := rows.Scan(
			&row.jobId,
			&row.queue,
			&row.owner,
			&row.namespace,
			&row.jobSet,
			&row.cpu,
			&row.memory,
			&row.ephemeralStorage,
			&row.gpu,
			&row.priority,
			&row.submitted,
			&row.cancelled,
			&row.state,
			&row.lastTransitionTime,
			&row.duplicate,
			&row.priorityClass,
			&row.latestRunId,
			&row.cancelReason,
			&annotations,
			&runs,
		); err != nil {
			return nil, err
		}
		job := jobRowToModel(&row)
		if annotations.Valid {
			if err := json.Unmarshal([]byte(annotations.String), &job.Annotations); err != nil {
				return nil, err
			}
		}
		if runs.Valid {
			if err := json.Unmarshal([]byte(runs.String), &job.Runs); err != nil {
				return nil, err
			}
		}
		if len(job.Runs) > 0 {
			lastRun := job.Runs[len(job.Runs)-1] // Get the last run
			job.Node = lastRun.Node
			job.Cluster = lastRun.Cluster
			job.ExitCode = lastRun.ExitCode
			job.RuntimeSeconds = calculateJobRuntime(lastRun.Started, lastRun.Finished, r.clock)
		}
		jobs = append(jobs, job)
	}
	return &GetJobsResult{Jobs: jobs}, nil
}

func calculateJobRuntime(started, finished *model.PostgreSQLTime, clock clock.Clock) int32 {
	if started == nil {
		return 0
	}

	if finished == nil {
		now := clock.Now()
		return formatDuration(started.Time, now)
	}

	return formatDuration(started.Time, finished.Time)
}

func formatDuration(start, end time.Time) int32 {
	duration := end.Sub(start).Round(time.Second)
	return int32(duration.Seconds())
}

func jobRowToModel(row *jobRow) *model.Job {
	return &model.Job{
		Annotations:        make(map[string]string),
		Cancelled:          database.ParseNullTime(row.cancelled),
		Cpu:                row.cpu,
		Duplicate:          row.duplicate,
		EphemeralStorage:   row.ephemeralStorage,
		Gpu:                row.gpu,
		JobId:              row.jobId,
		JobSet:             row.jobSet,
		LastActiveRunId:    database.ParseNullString(row.latestRunId),
		LastTransitionTime: row.lastTransitionTime,
		Memory:             row.memory,
		Owner:              row.owner,
		Namespace:          database.ParseNullString(row.namespace),
		Priority:           row.priority,
		PriorityClass:      database.ParseNullString(row.priorityClass),
		Queue:              row.queue,
		Runs:               make([]*model.Run, 0),
		State:              string(lookout.JobStateMap[row.state]),
		Submitted:          row.submitted,
		CancelReason:       database.ParseNullString(row.cancelReason),
	}
}
