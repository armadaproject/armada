// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.16.0
// source: query.sql

package database

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const countGroup = `-- name: CountGroup :one
SELECT COUNT(*) FROM markers WHERE group_id= $1
`

func (q *Queries) CountGroup(ctx context.Context, groupID uuid.UUID) (int64, error) {
	row := q.db.QueryRow(ctx, countGroup, groupID)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const deleteOldMarkers = `-- name: DeleteOldMarkers :exec
DELETE FROM markers WHERE created < $1::timestamptz
`

func (q *Queries) DeleteOldMarkers(ctx context.Context, cutoff time.Time) error {
	_, err := q.db.Exec(ctx, deleteOldMarkers, cutoff)
	return err
}

const findActiveRuns = `-- name: FindActiveRuns :many
SELECT run_id FROM runs WHERE run_id = ANY($1::UUID[])
                         AND (succeeded = false AND failed = false AND cancelled = false)
`

func (q *Queries) FindActiveRuns(ctx context.Context, runIds []uuid.UUID) ([]uuid.UUID, error) {
	rows, err := q.db.Query(ctx, findActiveRuns, runIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []uuid.UUID
	for rows.Next() {
		var run_id uuid.UUID
		if err := rows.Scan(&run_id); err != nil {
			return nil, err
		}
		items = append(items, run_id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const insertMarker = `-- name: InsertMarker :exec
INSERT INTO markers (group_id, partition_id, created) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING
`

type InsertMarkerParams struct {
	GroupID     uuid.UUID `db:"group_id"`
	PartitionID int32     `db:"partition_id"`
	Created     time.Time `db:"created"`
}

func (q *Queries) InsertMarker(ctx context.Context, arg InsertMarkerParams) error {
	_, err := q.db.Exec(ctx, insertMarker, arg.GroupID, arg.PartitionID, arg.Created)
	return err
}

const markJobRunsFailedById = `-- name: MarkJobRunsFailedById :exec
UPDATE runs SET failed = true WHERE run_id = ANY($1::UUID[])
`

func (q *Queries) MarkJobRunsFailedById(ctx context.Context, runIds []uuid.UUID) error {
	_, err := q.db.Exec(ctx, markJobRunsFailedById, runIds)
	return err
}

const markJobRunsReturnedById = `-- name: MarkJobRunsReturnedById :exec
UPDATE runs SET returned = true WHERE run_id = ANY($1::UUID[])
`

func (q *Queries) MarkJobRunsReturnedById(ctx context.Context, runIds []uuid.UUID) error {
	_, err := q.db.Exec(ctx, markJobRunsReturnedById, runIds)
	return err
}

const markJobRunsRunningById = `-- name: MarkJobRunsRunningById :exec
UPDATE runs SET running = true WHERE run_id = ANY($1::UUID[])
`

func (q *Queries) MarkJobRunsRunningById(ctx context.Context, runIds []uuid.UUID) error {
	_, err := q.db.Exec(ctx, markJobRunsRunningById, runIds)
	return err
}

const markJobRunsSucceededById = `-- name: MarkJobRunsSucceededById :exec
UPDATE runs SET succeeded = true WHERE run_id = ANY($1::UUID[])
`

func (q *Queries) MarkJobRunsSucceededById(ctx context.Context, runIds []uuid.UUID) error {
	_, err := q.db.Exec(ctx, markJobRunsSucceededById, runIds)
	return err
}

const markJobsCancelRequestedById = `-- name: MarkJobsCancelRequestedById :exec
UPDATE jobs SET cancel_requested = true WHERE job_id = ANY($1::text[])
`

func (q *Queries) MarkJobsCancelRequestedById(ctx context.Context, jobIds []string) error {
	_, err := q.db.Exec(ctx, markJobsCancelRequestedById, jobIds)
	return err
}

const markJobsCancelRequestedBySets = `-- name: MarkJobsCancelRequestedBySets :exec
UPDATE jobs SET cancelled_by_jobset_requested = true WHERE job_set = ANY($1::text[])
`

func (q *Queries) MarkJobsCancelRequestedBySets(ctx context.Context, jobSets []string) error {
	_, err := q.db.Exec(ctx, markJobsCancelRequestedBySets, jobSets)
	return err
}

const markJobsCancelledById = `-- name: MarkJobsCancelledById :exec
UPDATE jobs SET cancelled = true WHERE job_id = ANY($1::text[])
`

func (q *Queries) MarkJobsCancelledById(ctx context.Context, jobIds []string) error {
	_, err := q.db.Exec(ctx, markJobsCancelledById, jobIds)
	return err
}

const markJobsFailedById = `-- name: MarkJobsFailedById :exec
UPDATE jobs SET failed = true WHERE job_id = ANY($1::text[])
`

func (q *Queries) MarkJobsFailedById(ctx context.Context, jobIds []string) error {
	_, err := q.db.Exec(ctx, markJobsFailedById, jobIds)
	return err
}

const markJobsSucceededById = `-- name: MarkJobsSucceededById :exec
UPDATE jobs SET succeeded = true WHERE job_id = ANY($1::text[])
`

func (q *Queries) MarkJobsSucceededById(ctx context.Context, jobIds []string) error {
	_, err := q.db.Exec(ctx, markJobsSucceededById, jobIds)
	return err
}

const selectAllExecutors = `-- name: SelectAllExecutors :many
SELECT executor_id, last_request, last_updated FROM executors
`

func (q *Queries) SelectAllExecutors(ctx context.Context) ([]Executor, error) {
	rows, err := q.db.Query(ctx, selectAllExecutors)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Executor
	for rows.Next() {
		var i Executor
		if err := rows.Scan(&i.ExecutorID, &i.LastRequest, &i.LastUpdated); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const selectAllJobIds = `-- name: SelectAllJobIds :many
SELECT job_id FROM jobs
`

func (q *Queries) SelectAllJobIds(ctx context.Context) ([]string, error) {
	rows, err := q.db.Query(ctx, selectAllJobIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var job_id string
		if err := rows.Scan(&job_id); err != nil {
			return nil, err
		}
		items = append(items, job_id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const selectAllMarkers = `-- name: SelectAllMarkers :many
SELECT group_id, partition_id, created FROM markers
`

func (q *Queries) SelectAllMarkers(ctx context.Context) ([]Marker, error) {
	rows, err := q.db.Query(ctx, selectAllMarkers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Marker
	for rows.Next() {
		var i Marker
		if err := rows.Scan(&i.GroupID, &i.PartitionID, &i.Created); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const selectAllRunErrors = `-- name: SelectAllRunErrors :many
SELECT run_id, job_id, error FROM job_run_errors
`

func (q *Queries) SelectAllRunErrors(ctx context.Context) ([]JobRunError, error) {
	rows, err := q.db.Query(ctx, selectAllRunErrors)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []JobRunError
	for rows.Next() {
		var i JobRunError
		if err := rows.Scan(&i.RunID, &i.JobID, &i.Error); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const selectAllRunIds = `-- name: SelectAllRunIds :many
SELECT run_id FROM runs
`

func (q *Queries) SelectAllRunIds(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := q.db.Query(ctx, selectAllRunIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []uuid.UUID
	for rows.Next() {
		var run_id uuid.UUID
		if err := rows.Scan(&run_id); err != nil {
			return nil, err
		}
		items = append(items, run_id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const selectExecutorUpdateTimes = `-- name: SelectExecutorUpdateTimes :many
SELECT executor_id, last_updated FROM executors
`

type SelectExecutorUpdateTimesRow struct {
	ExecutorID  string    `db:"executor_id"`
	LastUpdated time.Time `db:"last_updated"`
}

func (q *Queries) SelectExecutorUpdateTimes(ctx context.Context) ([]SelectExecutorUpdateTimesRow, error) {
	rows, err := q.db.Query(ctx, selectExecutorUpdateTimes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []SelectExecutorUpdateTimesRow
	for rows.Next() {
		var i SelectExecutorUpdateTimesRow
		if err := rows.Scan(&i.ExecutorID, &i.LastUpdated); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const selectJobsForExecutor = `-- name: SelectJobsForExecutor :many
SELECT jr.run_id, j.queue, j.job_set, j.user_id, j.groups, j.submit_message
FROM runs jr
         JOIN jobs j
              ON jr.job_id = j.job_id
WHERE jr.executor = $1
  AND jr.run_id NOT IN ($2::UUID[])
  AND jr.succeeded = false AND jr.failed = false AND jr.cancelled = false
`

type SelectJobsForExecutorParams struct {
	Executor string      `db:"executor"`
	RunIds   []uuid.UUID `db:"run_ids"`
}

type SelectJobsForExecutorRow struct {
	RunID         uuid.UUID `db:"run_id"`
	Queue         string    `db:"queue"`
	JobSet        string    `db:"job_set"`
	UserID        string    `db:"user_id"`
	Groups        []byte    `db:"groups"`
	SubmitMessage []byte    `db:"submit_message"`
}

func (q *Queries) SelectJobsForExecutor(ctx context.Context, arg SelectJobsForExecutorParams) ([]SelectJobsForExecutorRow, error) {
	rows, err := q.db.Query(ctx, selectJobsForExecutor, arg.Executor, arg.RunIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []SelectJobsForExecutorRow
	for rows.Next() {
		var i SelectJobsForExecutorRow
		if err := rows.Scan(
			&i.RunID,
			&i.Queue,
			&i.JobSet,
			&i.UserID,
			&i.Groups,
			&i.SubmitMessage,
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

const selectNewJobs = `-- name: SelectNewJobs :many
SELECT job_id, job_set, queue, user_id, submitted, groups, priority, cancel_requested, cancelled, cancelled_by_jobset_requested, succeeded, failed, submit_message, scheduling_info, serial, last_modified FROM jobs WHERE serial > $1 ORDER BY serial LIMIT $2
`

type SelectNewJobsParams struct {
	Serial int64 `db:"serial"`
	Limit  int32 `db:"limit"`
}

func (q *Queries) SelectNewJobs(ctx context.Context, arg SelectNewJobsParams) ([]Job, error) {
	rows, err := q.db.Query(ctx, selectNewJobs, arg.Serial, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Job
	for rows.Next() {
		var i Job
		if err := rows.Scan(
			&i.JobID,
			&i.JobSet,
			&i.Queue,
			&i.UserID,
			&i.Submitted,
			&i.Groups,
			&i.Priority,
			&i.CancelRequested,
			&i.Cancelled,
			&i.CancelledByJobsetRequested,
			&i.Succeeded,
			&i.Failed,
			&i.SubmitMessage,
			&i.SchedulingInfo,
			&i.Serial,
			&i.LastModified,
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

const selectNewRuns = `-- name: SelectNewRuns :many
SELECT run_id, job_id, created, job_set, executor, cancelled, running, succeeded, failed, returned, serial, last_modified FROM runs WHERE serial > $1 ORDER BY serial LIMIT $2
`

type SelectNewRunsParams struct {
	Serial int64 `db:"serial"`
	Limit  int32 `db:"limit"`
}

func (q *Queries) SelectNewRuns(ctx context.Context, arg SelectNewRunsParams) ([]Run, error) {
	rows, err := q.db.Query(ctx, selectNewRuns, arg.Serial, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Run
	for rows.Next() {
		var i Run
		if err := rows.Scan(
			&i.RunID,
			&i.JobID,
			&i.Created,
			&i.JobSet,
			&i.Executor,
			&i.Cancelled,
			&i.Running,
			&i.Succeeded,
			&i.Failed,
			&i.Returned,
			&i.Serial,
			&i.LastModified,
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

const selectNewRunsForJobs = `-- name: SelectNewRunsForJobs :many
SELECT run_id, job_id, created, job_set, executor, cancelled, running, succeeded, failed, returned, serial, last_modified FROM runs WHERE serial > $1 AND job_id = ANY($2::text[]) ORDER BY serial
`

type SelectNewRunsForJobsParams struct {
	Serial int64    `db:"serial"`
	JobIds []string `db:"job_ids"`
}

func (q *Queries) SelectNewRunsForJobs(ctx context.Context, arg SelectNewRunsForJobsParams) ([]Run, error) {
	rows, err := q.db.Query(ctx, selectNewRunsForJobs, arg.Serial, arg.JobIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []Run
	for rows.Next() {
		var i Run
		if err := rows.Scan(
			&i.RunID,
			&i.JobID,
			&i.Created,
			&i.JobSet,
			&i.Executor,
			&i.Cancelled,
			&i.Running,
			&i.Succeeded,
			&i.Failed,
			&i.Returned,
			&i.Serial,
			&i.LastModified,
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

const selectRunErrorsById = `-- name: SelectRunErrorsById :many
SELECT run_id, job_id, error FROM job_run_errors WHERE run_id = ANY($1::UUID[])
`

// Run errors
func (q *Queries) SelectRunErrorsById(ctx context.Context, runIds []uuid.UUID) ([]JobRunError, error) {
	rows, err := q.db.Query(ctx, selectRunErrorsById, runIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []JobRunError
	for rows.Next() {
		var i JobRunError
		if err := rows.Scan(&i.RunID, &i.JobID, &i.Error); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const selectUpdatedJobs = `-- name: SelectUpdatedJobs :many
SELECT job_id, job_set, queue, priority, submitted, cancel_requested, cancelled, succeeded, failed, scheduling_info, serial FROM jobs WHERE serial > $1 ORDER BY serial LIMIT $2
`

type SelectUpdatedJobsParams struct {
	Serial int64 `db:"serial"`
	Limit  int32 `db:"limit"`
}

type SelectUpdatedJobsRow struct {
	JobID           string `db:"job_id"`
	JobSet          string `db:"job_set"`
	Queue           string `db:"queue"`
	Priority        int64  `db:"priority"`
	Submitted       int64  `db:"submitted"`
	CancelRequested bool   `db:"cancel_requested"`
	Cancelled       bool   `db:"cancelled"`
	Succeeded       bool   `db:"succeeded"`
	Failed          bool   `db:"failed"`
	SchedulingInfo  []byte `db:"scheduling_info"`
	Serial          int64  `db:"serial"`
}

func (q *Queries) SelectUpdatedJobs(ctx context.Context, arg SelectUpdatedJobsParams) ([]SelectUpdatedJobsRow, error) {
	rows, err := q.db.Query(ctx, selectUpdatedJobs, arg.Serial, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []SelectUpdatedJobsRow
	for rows.Next() {
		var i SelectUpdatedJobsRow
		if err := rows.Scan(
			&i.JobID,
			&i.JobSet,
			&i.Queue,
			&i.Priority,
			&i.Submitted,
			&i.CancelRequested,
			&i.Cancelled,
			&i.Succeeded,
			&i.Failed,
			&i.SchedulingInfo,
			&i.Serial,
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

const updateJobPriorityById = `-- name: UpdateJobPriorityById :exec
UPDATE jobs SET priority = $1 WHERE job_id = $2
`

type UpdateJobPriorityByIdParams struct {
	Priority int64  `db:"priority"`
	JobID    string `db:"job_id"`
}

func (q *Queries) UpdateJobPriorityById(ctx context.Context, arg UpdateJobPriorityByIdParams) error {
	_, err := q.db.Exec(ctx, updateJobPriorityById, arg.Priority, arg.JobID)
	return err
}

const updateJobPriorityByJobSet = `-- name: UpdateJobPriorityByJobSet :exec
UPDATE jobs SET priority = $1 WHERE job_set = $2
`

type UpdateJobPriorityByJobSetParams struct {
	Priority int64  `db:"priority"`
	JobSet   string `db:"job_set"`
}

func (q *Queries) UpdateJobPriorityByJobSet(ctx context.Context, arg UpdateJobPriorityByJobSetParams) error {
	_, err := q.db.Exec(ctx, updateJobPriorityByJobSet, arg.Priority, arg.JobSet)
	return err
}

const upsertExecutor = `-- name: UpsertExecutor :exec
INSERT INTO executors (executor_id, last_request, last_updated)
VALUES($1::text, $2::bytea, $3::timestamptz)
ON CONFLICT (executor_id) DO UPDATE SET (last_request, last_updated) = (excluded.last_request,excluded.last_updated)
`

type UpsertExecutorParams struct {
	ExecutorID  string    `db:"executor_id"`
	LastRequest []byte    `db:"last_request"`
	UpdateTime  time.Time `db:"update_time"`
}

func (q *Queries) UpsertExecutor(ctx context.Context, arg UpsertExecutorParams) error {
	_, err := q.db.Exec(ctx, upsertExecutor, arg.ExecutorID, arg.LastRequest, arg.UpdateTime)
	return err
}
