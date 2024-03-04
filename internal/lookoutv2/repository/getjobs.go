package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

type GetJobsRepository interface {
	GetJobs(ctx *armadacontext.Context, filters []*model.Filter, order *model.Order, skip int, take int) (*GetJobsResult, error)
}

type SqlGetJobsRepository struct {
	db              *pgxpool.Pool
	lookoutTables   *LookoutTables
	useJsonbBackend bool
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

type runRow struct {
	jobId       string
	runId       string
	cluster     string
	node        sql.NullString
	leased      sql.NullTime
	pending     sql.NullTime
	started     sql.NullTime
	finished    sql.NullTime
	jobRunState int
	exitCode    sql.NullInt32
}

type annotationRow struct {
	jobId           string
	annotationKey   string
	annotationValue string
}

func NewSqlGetJobsRepository(db *pgxpool.Pool, useJsonbBackend bool) *SqlGetJobsRepository {
	return &SqlGetJobsRepository{
		db:              db,
		lookoutTables:   NewTables(),
		useJsonbBackend: useJsonbBackend,
	}
}

func (r *SqlGetJobsRepository) GetJobs(ctx *armadacontext.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) (*GetJobsResult, error) {
	getJobs := r.getJobs
	if r.useJsonbBackend {
		getJobs = r.getJobsJsonb
	}
	return getJobs(ctx, filters, activeJobSets, order, skip, take)
}

func (r *SqlGetJobsRepository) getJobs(ctx *armadacontext.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) (*GetJobsResult, error) {
	var jobRows []*jobRow
	var runRows []*runRow
	var annotationRows []*annotationRow

	err := pgx.BeginTxFunc(ctx, r.db, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		createTempTableQuery, tempTableName := NewQueryBuilder(r.lookoutTables).CreateTempTable()
		logQuery(createTempTableQuery, "CreateTempTable")
		_, err := tx.Exec(ctx, createTempTableQuery.Sql, createTempTableQuery.Args...)
		if err != nil {
			return err
		}

		insertQuery, err := NewQueryBuilder(r.lookoutTables).InsertIntoTempTable(tempTableName, filters, activeJobSets, order, skip, take)
		if err != nil {
			return err
		}
		logQuery(insertQuery, "InsertIntoTempTable")
		_, err = tx.Exec(ctx, insertQuery.Sql, insertQuery.Args...)
		if err != nil {
			return err
		}

		jobRows, err = makeJobRows(ctx, tx, tempTableName)
		if err != nil {
			log.WithError(err).Error("failed getting job rows")
			return err
		}
		runRows, err = makeRunRows(ctx, tx, tempTableName)
		if err != nil {
			log.WithError(err).Error("failed getting run rows")
			return err
		}

		annotationRows, err = makeAnnotationRows(ctx, tx, tempTableName)
		if err != nil {
			log.WithError(err).Error("failed getting annotation rows")
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	jobs, err := rowsToJobs(jobRows, runRows, annotationRows)
	if err != nil {
		return nil, err
	}
	return &GetJobsResult{
		Jobs: jobs,
	}, nil
}

func (r *SqlGetJobsRepository) getJobsJsonb(ctx *armadacontext.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) (*GetJobsResult, error) {
	query, err := NewQueryBuilder(r.lookoutTables).GetJobsJsonb(filters, activeJobSets, order, skip, take)
	if err != nil {
		return nil, err
	}
	logQuery(query, "GetJobs")
	var jobs []*model.Job
	if err := pgx.BeginTxFunc(ctx, r.db, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, query.Sql, query.Args...)
		if err != nil {
			return err
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
				return err
			}
			job := jobRowToModel(&row)
			if annotations.Valid {
				if err := json.Unmarshal([]byte(annotations.String), &job.Annotations); err != nil {
					return err
				}
			}
			if runs.Valid {
				if err := json.Unmarshal([]byte(runs.String), &job.Runs); err != nil {
					return err
				}
			}
			if len(job.Runs) > 0 {
				lastRun := job.Runs[len(job.Runs)-1] // Get the last run
				job.Node = lastRun.Node
				job.Cluster = lastRun.Cluster
				job.ExitCode = lastRun.ExitCode

			}
			jobs = append(jobs, job)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &GetJobsResult{Jobs: jobs}, nil
}

func rowsToJobs(jobRows []*jobRow, runRows []*runRow, annotationRows []*annotationRow) ([]*model.Job, error) {
	jobMap := make(map[string]*model.Job) // Map from Job ID to Job
	orderedJobIds := make([]string, len(jobRows))

	for i, row := range jobRows {
		job := jobRowToModel(row)
		jobMap[row.jobId] = job
		orderedJobIds[i] = row.jobId
	}

	for _, row := range runRows {
		run := &model.Run{
			Cluster:     row.cluster,
			ExitCode:    database.ParseNullInt32(row.exitCode),
			Finished:    model.NewPostgreSQLTime(database.ParseNullTime(row.finished)),
			JobRunState: row.jobRunState,
			Node:        database.ParseNullString(row.node),
			Leased:      model.NewPostgreSQLTime(database.ParseNullTime(row.leased)),
			Pending:     model.NewPostgreSQLTime(database.ParseNullTime(row.pending)),
			RunId:       row.runId,
			Started:     model.NewPostgreSQLTime(database.ParseNullTime(row.started)),
		}
		job, ok := jobMap[row.jobId]
		if !ok {
			return nil, errors.Errorf("job row with id %s not found", row.jobId)
		}
		job.Runs = append(jobMap[row.jobId].Runs, run)
	}

	for _, row := range annotationRows {
		job, ok := jobMap[row.jobId]
		if !ok {
			return nil, errors.Errorf("job row with id %s not found", row.jobId)
		}
		job.Annotations[row.annotationKey] = row.annotationValue
	}

	jobs := make([]*model.Job, len(orderedJobIds))
	for i, jobId := range orderedJobIds {
		job := jobMap[jobId]
		sortRuns(job.Runs)
		if len(job.Runs) > 0 {
			lastRun := job.Runs[len(job.Runs)-1] // Get the last run
			job.Node = lastRun.Node
			job.Cluster = lastRun.Cluster
			job.ExitCode = lastRun.ExitCode

		}
		jobs[i] = job
	}

	return jobs, nil
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

func sortRuns(runs []*model.Run) {
	sort.Slice(runs, func(i, j int) bool {
		timeA, err := getJobRunTime(runs[i])
		if err != nil {
			log.WithError(err).Error("failed to get time for run")
			return true
		}
		timeB, err := getJobRunTime(runs[j])
		if err != nil {
			log.WithError(err).Error("failed to get time for run")
			return true
		}
		return timeA.Before(timeB)
	})
}

func getJobRunTime(run *model.Run) (time.Time, error) {
	if run.Leased != nil {
		return run.Leased.Time, nil
	}
	if run.Pending != nil {
		return run.Pending.Time, nil
	}
	return time.Time{}, errors.Errorf("error when getting run time for run with id %s", run.RunId)
}

func makeJobRows(ctx *armadacontext.Context, tx pgx.Tx, tmpTableName string) ([]*jobRow, error) {
	query := fmt.Sprintf(`
		SELECT
			j.job_id,
			j.queue,
			j.owner,
			j.namespace,
			j.jobset,
			j.cpu,
			j.memory,
			j.ephemeral_storage,
			j.gpu,
			j.priority,
			j.submitted,
			j.cancelled,
			j.state,
			j.last_transition_time,
			j.duplicate,
			j.priority_class,
			j.latest_run_id,
			j.cancel_reason
		FROM %s AS t
		INNER JOIN job AS j ON t.job_id = j.job_id
	`, tmpTableName)
	pgxRows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer pgxRows.Close()

	var rows []*jobRow
	for pgxRows.Next() {
		var row jobRow
		err := pgxRows.Scan(
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
		)
		if err != nil {
			log.WithError(err).Errorf("failed to scan job row at index %d", len(rows))
		}
		rows = append(rows, &row)
	}
	return rows, nil
}

func makeRunRows(ctx *armadacontext.Context, tx pgx.Tx, tmpTableName string) ([]*runRow, error) {
	query := fmt.Sprintf(`
		SELECT
		    jr.job_id,
			jr.run_id,
			jr.cluster,
			jr.node,
			jr.leased,
			jr.pending,
			jr.started,
			jr.finished,
			jr.job_run_state,
			jr.exit_code
		FROM %s AS t
		INNER JOIN job_run AS jr ON t.job_id = jr.job_id
	`, tmpTableName)
	pgxRows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer pgxRows.Close()

	var rows []*runRow
	for pgxRows.Next() {
		var row runRow
		err := pgxRows.Scan(
			&row.jobId,
			&row.runId,
			&row.cluster,
			&row.node,
			&row.leased,
			&row.pending,
			&row.started,
			&row.finished,
			&row.jobRunState,
			&row.exitCode,
		)
		if err != nil {
			log.WithError(err).Errorf("failed to scan run row at index %d", len(rows))
		}
		rows = append(rows, &row)
	}
	return rows, nil
}

func makeAnnotationRows(ctx *armadacontext.Context, tx pgx.Tx, tempTableName string) ([]*annotationRow, error) {
	query := fmt.Sprintf(`
		SELECT
			ual.job_id,
			ual.key,
			ual.value
		FROM %s AS t
		INNER JOIN user_annotation_lookup AS ual ON t.job_id = ual.job_id
	`, tempTableName)
	pgxRows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer pgxRows.Close()

	var rows []*annotationRow
	for pgxRows.Next() {
		var row annotationRow
		err := pgxRows.Scan(
			&row.jobId,
			&row.annotationKey,
			&row.annotationValue,
		)
		if err != nil {
			log.WithError(err).Errorf("failed to scan annotation row at index %d", len(rows))
		}
		rows = append(rows, &row)
	}
	return rows, nil
}
