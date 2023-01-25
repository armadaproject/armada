package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/common/database/lookout"
	"github.com/armadaproject/armada/internal/lookoutv2/model"
)

type GetJobsRepository interface {
	GetJobs(ctx context.Context, filters []*model.Filter, order *model.Order, skip int, take int) (*GetJobsResult, error)
}

type SqlGetJobsRepository struct {
	db            *pgxpool.Pool
	lookoutTables *LookoutTables
}

type GetJobsResult struct {
	Jobs  []*model.Job
	Count int
}

type jobRow struct {
	jobId              string
	queue              string
	owner              string
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
}

type runRow struct {
	jobId       string
	runId       string
	cluster     string
	node        sql.NullString
	pending     time.Time
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

func NewSqlGetJobsRepository(db *pgxpool.Pool) *SqlGetJobsRepository {
	return &SqlGetJobsRepository{
		db:            db,
		lookoutTables: NewTables(),
	}
}

func (r *SqlGetJobsRepository) GetJobs(ctx context.Context, filters []*model.Filter, order *model.Order, skip int, take int) (*GetJobsResult, error) {
	var jobRows []*jobRow
	var runRows []*runRow
	var annotationRows []*annotationRow
	var count int

	err := r.db.BeginTxFunc(ctx, pgx.TxOptions{
		IsoLevel:       pgx.RepeatableRead,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.Deferrable,
	}, func(tx pgx.Tx) error {
		countQuery, err := NewQueryBuilder(r.lookoutTables).JobCount(filters)
		if err != nil {
			return err
		}
		logQuery(countQuery)
		rows, err := tx.Query(ctx, countQuery.Sql, countQuery.Args...)
		if err != nil {
			return err
		}
		count, err = database.ReadInt(rows)
		if err != nil {
			return err
		}

		createTempTableQuery, tempTableName := NewQueryBuilder(r.lookoutTables).CreateTempTable()
		logQuery(createTempTableQuery)
		_, err = tx.Exec(ctx, createTempTableQuery.Sql, createTempTableQuery.Args...)
		if err != nil {
			return err
		}

		insertQuery, err := NewQueryBuilder(r.lookoutTables).InsertIntoTempTable(tempTableName, filters, order, skip, take)
		if err != nil {
			return err
		}
		logQuery(createTempTableQuery)
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
		Jobs:  jobs,
		Count: count,
	}, nil
}

func rowsToJobs(jobRows []*jobRow, runRows []*runRow, annotationRows []*annotationRow) ([]*model.Job, error) {
	jobMap := make(map[string]*model.Job) // Map from Job ID to Job
	orderedJobIds := make([]string, len(jobRows))

	for i, row := range jobRows {
		job := &model.Job{
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
			Priority:           row.priority,
			PriorityClass:      database.ParseNullString(row.priorityClass),
			Queue:              row.queue,
			Runs:               []*model.Run{},
			State:              string(lookout.JobStateMap[row.state]),
			Submitted:          row.submitted,
		}
		jobMap[row.jobId] = job
		orderedJobIds[i] = row.jobId
	}

	for _, row := range runRows {
		run := &model.Run{
			Cluster:     row.cluster,
			ExitCode:    database.ParseNullInt32(row.exitCode),
			Finished:    database.ParseNullTime(row.finished),
			JobRunState: string(lookout.JobRunStateMap[row.jobRunState]),
			Node:        database.ParseNullString(row.node),
			Pending:     row.pending,
			RunId:       row.runId,
			Started:     database.ParseNullTime(row.started),
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
		jobs[i] = job
	}

	return jobs, nil
}

func sortRuns(runs []*model.Run) {
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].Pending.Before(runs[j].Pending)
	})
}

func makeJobRows(ctx context.Context, tx pgx.Tx, tmpTableName string) ([]*jobRow, error) {
	query := fmt.Sprintf(`
		SELECT
			j.job_id,
			j.queue,
			j.owner,
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
			j.latest_run_id
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
		)
		if err != nil {
			log.WithError(err).Errorf("failed to scan job row at index %d", len(rows))
		}
		rows = append(rows, &row)
	}
	return rows, nil
}

func makeRunRows(ctx context.Context, tx pgx.Tx, tmpTableName string) ([]*runRow, error) {
	query := fmt.Sprintf(`
		SELECT
		    jr.job_id,
			jr.run_id,
			jr.cluster,
			jr.node,
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

func makeAnnotationRows(ctx context.Context, tx pgx.Tx, tempTableName string) ([]*annotationRow, error) {
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
