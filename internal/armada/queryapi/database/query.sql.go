// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.22.0
// source: query.sql

package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const getJobDetails = `-- name: GetJobDetails :many
SELECT job_id, queue, jobset, namespace, state, submitted, cancelled, cancel_reason, last_transition_time, latest_run_id, job_spec FROM job WHERE job_id = ANY($1::text[])
`

type GetJobDetailsRow struct {
	JobID              string           `db:"job_id"`
	Queue              string           `db:"queue"`
	Jobset             string           `db:"jobset"`
	Namespace          *string          `db:"namespace"`
	State              int16            `db:"state"`
	Submitted          pgtype.Timestamp `db:"submitted"`
	Cancelled          pgtype.Timestamp `db:"cancelled"`
	CancelReason       *string          `db:"cancel_reason"`
	LastTransitionTime pgtype.Timestamp `db:"last_transition_time"`
	LatestRunID        *string          `db:"latest_run_id"`
	JobSpec            []byte           `db:"job_spec"`
}

func (q *Queries) GetJobDetails(ctx context.Context, jobIds []string) ([]GetJobDetailsRow, error) {
	rows, err := q.db.Query(ctx, getJobDetails, jobIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetJobDetailsRow
	for rows.Next() {
		var i GetJobDetailsRow
		if err := rows.Scan(
			&i.JobID,
			&i.Queue,
			&i.Jobset,
			&i.Namespace,
			&i.State,
			&i.Submitted,
			&i.Cancelled,
			&i.CancelReason,
			&i.LastTransitionTime,
			&i.LatestRunID,
			&i.JobSpec,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getJobRunsByJobIds = `-- name: GetJobRunsByJobIds :many
SELECT run_id, job_id, cluster, node, pending, started, finished, job_run_state, error, exit_code, leased FROM job_run WHERE job_id = ANY($1::text[])
`

func (q *Queries) GetJobRunsByJobIds(ctx context.Context, jobIds []string) ([]JobRun, error) {
	rows, err := q.db.Query(ctx, getJobRunsByJobIds, jobIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []JobRun
	for rows.Next() {
		var i JobRun
		if err := rows.Scan(
			&i.RunID,
			&i.JobID,
			&i.Cluster,
			&i.Node,
			&i.Pending,
			&i.Started,
			&i.Finished,
			&i.JobRunState,
			&i.Error,
			&i.ExitCode,
			&i.Leased,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getJobRunsByRunIds = `-- name: GetJobRunsByRunIds :many
SELECT run_id, job_id, cluster, node, pending, started, finished, job_run_state, error, exit_code, leased FROM job_run WHERE run_id = ANY($1::text[])
`

func (q *Queries) GetJobRunsByRunIds(ctx context.Context, runIds []string) ([]JobRun, error) {
	rows, err := q.db.Query(ctx, getJobRunsByRunIds, runIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []JobRun
	for rows.Next() {
		var i JobRun
		if err := rows.Scan(
			&i.RunID,
			&i.JobID,
			&i.Cluster,
			&i.Node,
			&i.Pending,
			&i.Started,
			&i.Finished,
			&i.JobRunState,
			&i.Error,
			&i.ExitCode,
			&i.Leased,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const getJobStates = `-- name: GetJobStates :many
SELECT job_id, state FROM job WHERE job_id = ANY($1::text[])
`

type GetJobStatesRow struct {
	JobID string `db:"job_id"`
	State int16  `db:"state"`
}

func (q *Queries) GetJobStates(ctx context.Context, jobIds []string) ([]GetJobStatesRow, error) {
	rows, err := q.db.Query(ctx, getJobStates, jobIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GetJobStatesRow
	for rows.Next() {
		var i GetJobStatesRow
		if err := rows.Scan(&i.JobID, &i.State); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
