// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.27.0
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

const deleteExecutorSettings = `-- name: DeleteExecutorSettings :exec
DELETE FROM executor_settings WHERE executor_id = $1::text
`

func (q *Queries) DeleteExecutorSettings(ctx context.Context, executorID string) error {
	_, err := q.db.Exec(ctx, deleteExecutorSettings, executorID)
	return err
}

const deleteOldMarkers = `-- name: DeleteOldMarkers :exec
DELETE FROM markers WHERE created < $1::timestamptz
`

func (q *Queries) DeleteOldMarkers(ctx context.Context, cutoff time.Time) error {
	_, err := q.db.Exec(ctx, deleteOldMarkers, cutoff)
	return err
}

const findActiveRuns = `-- name: FindActiveRuns :many
SELECT run_id FROM runs WHERE run_id = ANY($1::text[])
                         AND (succeeded = false AND failed = false AND cancelled = false)
`

func (q *Queries) FindActiveRuns(ctx context.Context, runIds []string) ([]string, error) {
	rows, err := q.db.Query(ctx, findActiveRuns, runIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var run_id string
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

const markJobRunsAttemptedById = `-- name: MarkJobRunsAttemptedById :exec
UPDATE runs SET run_attempted = true WHERE run_id = ANY($1::text[])
`

func (q *Queries) MarkJobRunsAttemptedById(ctx context.Context, runIds []string) error {
	_, err := q.db.Exec(ctx, markJobRunsAttemptedById, runIds)
	return err
}

const markJobRunsFailedById = `-- name: MarkJobRunsFailedById :exec
UPDATE runs SET failed = true WHERE run_id = ANY($1::text[])
`

func (q *Queries) MarkJobRunsFailedById(ctx context.Context, runIds []string) error {
	_, err := q.db.Exec(ctx, markJobRunsFailedById, runIds)
	return err
}

const markJobRunsPreemptRequestedByJobId = `-- name: MarkJobRunsPreemptRequestedByJobId :exec
UPDATE runs SET preempt_requested = true WHERE queue = $1 and job_set = $2 and job_id = ANY($3::text[]) and cancelled = false and succeeded = false and failed = false
`

type MarkJobRunsPreemptRequestedByJobIdParams struct {
	Queue  string   `db:"queue"`
	JobSet string   `db:"job_set"`
	JobIds []string `db:"job_ids"`
}

func (q *Queries) MarkJobRunsPreemptRequestedByJobId(ctx context.Context, arg MarkJobRunsPreemptRequestedByJobIdParams) error {
	_, err := q.db.Exec(ctx, markJobRunsPreemptRequestedByJobId, arg.Queue, arg.JobSet, arg.JobIds)
	return err
}

const markJobRunsReturnedById = `-- name: MarkJobRunsReturnedById :exec
UPDATE runs SET returned = true WHERE run_id = ANY($1::text[])
`

func (q *Queries) MarkJobRunsReturnedById(ctx context.Context, runIds []string) error {
	_, err := q.db.Exec(ctx, markJobRunsReturnedById, runIds)
	return err
}

const markJobRunsRunningById = `-- name: MarkJobRunsRunningById :exec
UPDATE runs SET running = true WHERE run_id = ANY($1::text[])
`

func (q *Queries) MarkJobRunsRunningById(ctx context.Context, runIds []string) error {
	_, err := q.db.Exec(ctx, markJobRunsRunningById, runIds)
	return err
}

const markJobRunsSucceededById = `-- name: MarkJobRunsSucceededById :exec
UPDATE runs SET succeeded = true WHERE run_id = ANY($1::text[])
`

func (q *Queries) MarkJobRunsSucceededById(ctx context.Context, runIds []string) error {
	_, err := q.db.Exec(ctx, markJobRunsSucceededById, runIds)
	return err
}

const markJobsCancelRequestedById = `-- name: MarkJobsCancelRequestedById :exec
UPDATE jobs SET cancel_requested = true, cancel_user = $1 WHERE queue = $2 and job_set = $3 and job_id = ANY($4::text[]) and cancelled = false and succeeded = false and failed = false
`

type MarkJobsCancelRequestedByIdParams struct {
	CancelUser *string  `db:"cancel_user"`
	Queue      string   `db:"queue"`
	JobSet     string   `db:"job_set"`
	JobIds     []string `db:"job_ids"`
}

func (q *Queries) MarkJobsCancelRequestedById(ctx context.Context, arg MarkJobsCancelRequestedByIdParams) error {
	_, err := q.db.Exec(ctx, markJobsCancelRequestedById,
		arg.CancelUser,
		arg.Queue,
		arg.JobSet,
		arg.JobIds,
	)
	return err
}

const markJobsCancelRequestedBySetAndQueuedState = `-- name: MarkJobsCancelRequestedBySetAndQueuedState :exec
UPDATE jobs SET cancel_by_jobset_requested = true, cancel_user = $1 WHERE job_set = $2 and queue = $3 and queued = ANY($4::bool[]) and cancelled = false and succeeded = false and failed = false
`

type MarkJobsCancelRequestedBySetAndQueuedStateParams struct {
	CancelUser   *string `db:"cancel_user"`
	JobSet       string  `db:"job_set"`
	Queue        string  `db:"queue"`
	QueuedStates []bool  `db:"queued_states"`
}

func (q *Queries) MarkJobsCancelRequestedBySetAndQueuedState(ctx context.Context, arg MarkJobsCancelRequestedBySetAndQueuedStateParams) error {
	_, err := q.db.Exec(ctx, markJobsCancelRequestedBySetAndQueuedState,
		arg.CancelUser,
		arg.JobSet,
		arg.Queue,
		arg.QueuedStates,
	)
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

const markRunsCancelledByJobId = `-- name: MarkRunsCancelledByJobId :exec
UPDATE runs SET cancelled = true WHERE job_id = ANY($1::text[])
`

func (q *Queries) MarkRunsCancelledByJobId(ctx context.Context, jobIds []string) error {
	_, err := q.db.Exec(ctx, markRunsCancelledByJobId, jobIds)
	return err
}

const selectAllExecutorSettings = `-- name: SelectAllExecutorSettings :many
SELECT executor_id, cordoned, cordon_reason, set_by_user, set_at_time FROM executor_settings
`

func (q *Queries) SelectAllExecutorSettings(ctx context.Context) ([]ExecutorSetting, error) {
	rows, err := q.db.Query(ctx, selectAllExecutorSettings)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ExecutorSetting
	for rows.Next() {
		var i ExecutorSetting
		if err := rows.Scan(
			&i.ExecutorID,
			&i.Cordoned,
			&i.CordonReason,
			&i.SetByUser,
			&i.SetAtTime,
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

func (q *Queries) SelectAllRunIds(ctx context.Context) ([]string, error) {
	rows, err := q.db.Query(ctx, selectAllRunIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []string
	for rows.Next() {
		var run_id string
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

const selectInitialJobs = `-- name: SelectInitialJobs :many
SELECT job_id, job_set, queue, priority, submitted, queued, queued_version, validated, cancel_requested, cancel_user, cancel_by_jobset_requested, cancelled, succeeded, failed, scheduling_info, scheduling_info_version, pools, serial FROM jobs WHERE serial > $1 AND cancelled = 'false' AND succeeded = 'false' and failed = 'false' ORDER BY serial LIMIT $2
`

type SelectInitialJobsParams struct {
	Serial int64 `db:"serial"`
	Limit  int32 `db:"limit"`
}

type SelectInitialJobsRow struct {
	JobID                   string   `db:"job_id"`
	JobSet                  string   `db:"job_set"`
	Queue                   string   `db:"queue"`
	Priority                int64    `db:"priority"`
	Submitted               int64    `db:"submitted"`
	Queued                  bool     `db:"queued"`
	QueuedVersion           int32    `db:"queued_version"`
	Validated               bool     `db:"validated"`
	CancelRequested         bool     `db:"cancel_requested"`
	CancelUser              *string  `db:"cancel_user"`
	CancelByJobsetRequested bool     `db:"cancel_by_jobset_requested"`
	Cancelled               bool     `db:"cancelled"`
	Succeeded               bool     `db:"succeeded"`
	Failed                  bool     `db:"failed"`
	SchedulingInfo          []byte   `db:"scheduling_info"`
	SchedulingInfoVersion   int32    `db:"scheduling_info_version"`
	Pools                   []string `db:"pools"`
	Serial                  int64    `db:"serial"`
}

func (q *Queries) SelectInitialJobs(ctx context.Context, arg SelectInitialJobsParams) ([]SelectInitialJobsRow, error) {
	rows, err := q.db.Query(ctx, selectInitialJobs, arg.Serial, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []SelectInitialJobsRow
	for rows.Next() {
		var i SelectInitialJobsRow
		if err := rows.Scan(
			&i.JobID,
			&i.JobSet,
			&i.Queue,
			&i.Priority,
			&i.Submitted,
			&i.Queued,
			&i.QueuedVersion,
			&i.Validated,
			&i.CancelRequested,
			&i.CancelUser,
			&i.CancelByJobsetRequested,
			&i.Cancelled,
			&i.Succeeded,
			&i.Failed,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Pools,
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

const selectInitialRuns = `-- name: SelectInitialRuns :many
SELECT run_id, job_id, created, job_set, executor, node, cancelled, running, succeeded, failed, returned, run_attempted, serial, last_modified, leased_timestamp, pending_timestamp, running_timestamp, terminated_timestamp, scheduled_at_priority, preempted, pending, preempted_timestamp, pod_requirements_overlay, preempt_requested, queue, pool FROM runs WHERE serial > $1 AND job_id = ANY($3::text[]) ORDER BY serial LIMIT $2
`

type SelectInitialRunsParams struct {
	Serial int64    `db:"serial"`
	Limit  int32    `db:"limit"`
	JobIds []string `db:"job_ids"`
}

func (q *Queries) SelectInitialRuns(ctx context.Context, arg SelectInitialRunsParams) ([]Run, error) {
	rows, err := q.db.Query(ctx, selectInitialRuns, arg.Serial, arg.Limit, arg.JobIds)
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
			&i.Node,
			&i.Cancelled,
			&i.Running,
			&i.Succeeded,
			&i.Failed,
			&i.Returned,
			&i.RunAttempted,
			&i.Serial,
			&i.LastModified,
			&i.LeasedTimestamp,
			&i.PendingTimestamp,
			&i.RunningTimestamp,
			&i.TerminatedTimestamp,
			&i.ScheduledAtPriority,
			&i.Preempted,
			&i.Pending,
			&i.PreemptedTimestamp,
			&i.PodRequirementsOverlay,
			&i.PreemptRequested,
			&i.Queue,
			&i.Pool,
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

const selectJobsByExecutorAndQueues = `-- name: SelectJobsByExecutorAndQueues :many
SELECT j.job_id, j.job_set, j.queue, j.user_id, j.submitted, j.groups, j.priority, j.queued, j.queued_version, j.cancel_requested, j.cancelled, j.cancel_by_jobset_requested, j.succeeded, j.failed, j.submit_message, j.scheduling_info, j.scheduling_info_version, j.serial, j.last_modified, j.validated, j.pools, j.bid_price, j.cancel_user
FROM runs jr
       JOIN jobs j
            ON jr.job_id = j.job_id
WHERE jr.executor = $1
  AND j.queue = ANY($2::text[])
  AND jr.succeeded = false AND jr.failed = false AND jr.cancelled = false AND jr.preempted = false
`

type SelectJobsByExecutorAndQueuesParams struct {
	Executor string   `db:"executor"`
	Queues   []string `db:"queues"`
}

func (q *Queries) SelectJobsByExecutorAndQueues(ctx context.Context, arg SelectJobsByExecutorAndQueuesParams) ([]Job, error) {
	rows, err := q.db.Query(ctx, selectJobsByExecutorAndQueues, arg.Executor, arg.Queues)
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
			&i.Queued,
			&i.QueuedVersion,
			&i.CancelRequested,
			&i.Cancelled,
			&i.CancelByJobsetRequested,
			&i.Succeeded,
			&i.Failed,
			&i.SubmitMessage,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Serial,
			&i.LastModified,
			&i.Validated,
			&i.Pools,
			&i.BidPrice,
			&i.CancelUser,
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

const selectJobsForExecutor = `-- name: SelectJobsForExecutor :many
SELECT jr.run_id, j.queue, j.job_set, j.user_id, j.groups, j.submit_message
FROM runs jr
         JOIN jobs j
              ON jr.job_id = j.job_id
WHERE jr.executor = $1
  AND jr.run_id NOT IN ($2::text[])
  AND jr.succeeded = false AND jr.failed = false AND jr.cancelled = false
`

type SelectJobsForExecutorParams struct {
	Executor string   `db:"executor"`
	RunIds   []string `db:"run_ids"`
}

type SelectJobsForExecutorRow struct {
	RunID         string `db:"run_id"`
	Queue         string `db:"queue"`
	JobSet        string `db:"job_set"`
	UserID        string `db:"user_id"`
	Groups        []byte `db:"groups"`
	SubmitMessage []byte `db:"submit_message"`
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

const selectLatestJobRunSerial = `-- name: SelectLatestJobRunSerial :one
SELECT serial FROM runs ORDER BY serial DESC LIMIT 1
`

func (q *Queries) SelectLatestJobRunSerial(ctx context.Context) (int64, error) {
	row := q.db.QueryRow(ctx, selectLatestJobRunSerial)
	var serial int64
	err := row.Scan(&serial)
	return serial, err
}

const selectLatestJobSerial = `-- name: SelectLatestJobSerial :one
SELECT serial FROM jobs ORDER BY serial DESC LIMIT 1
`

func (q *Queries) SelectLatestJobSerial(ctx context.Context) (int64, error) {
	row := q.db.QueryRow(ctx, selectLatestJobSerial)
	var serial int64
	err := row.Scan(&serial)
	return serial, err
}

const selectLeasedJobsByQueue = `-- name: SelectLeasedJobsByQueue :many
SELECT j.job_id, j.job_set, j.queue, j.user_id, j.submitted, j.groups, j.priority, j.queued, j.queued_version, j.cancel_requested, j.cancelled, j.cancel_by_jobset_requested, j.succeeded, j.failed, j.submit_message, j.scheduling_info, j.scheduling_info_version, j.serial, j.last_modified, j.validated, j.pools, j.bid_price, j.cancel_user
FROM runs jr
       JOIN jobs j
            ON jr.job_id = j.job_id
WHERE j.queue = ANY($1::text[])
  AND jr.running = false
  AND jr.pending = false
  AND jr.succeeded = false
  AND jr.failed = false
  AND jr.cancelled = false
  AND jr.preempted = false
`

func (q *Queries) SelectLeasedJobsByQueue(ctx context.Context, queue []string) ([]Job, error) {
	rows, err := q.db.Query(ctx, selectLeasedJobsByQueue, queue)
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
			&i.Queued,
			&i.QueuedVersion,
			&i.CancelRequested,
			&i.Cancelled,
			&i.CancelByJobsetRequested,
			&i.Succeeded,
			&i.Failed,
			&i.SubmitMessage,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Serial,
			&i.LastModified,
			&i.Validated,
			&i.Pools,
			&i.BidPrice,
			&i.CancelUser,
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
SELECT job_id, job_set, queue, user_id, submitted, groups, priority, queued, queued_version, cancel_requested, cancelled, cancel_by_jobset_requested, succeeded, failed, submit_message, scheduling_info, scheduling_info_version, serial, last_modified, validated, pools, bid_price, cancel_user FROM jobs WHERE serial > $1 ORDER BY serial LIMIT $2
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
			&i.Queued,
			&i.QueuedVersion,
			&i.CancelRequested,
			&i.Cancelled,
			&i.CancelByJobsetRequested,
			&i.Succeeded,
			&i.Failed,
			&i.SubmitMessage,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Serial,
			&i.LastModified,
			&i.Validated,
			&i.Pools,
			&i.BidPrice,
			&i.CancelUser,
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
SELECT run_id, job_id, created, job_set, executor, node, cancelled, running, succeeded, failed, returned, run_attempted, serial, last_modified, leased_timestamp, pending_timestamp, running_timestamp, terminated_timestamp, scheduled_at_priority, preempted, pending, preempted_timestamp, pod_requirements_overlay, preempt_requested, queue, pool FROM runs WHERE serial > $1 ORDER BY serial LIMIT $2
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
			&i.Node,
			&i.Cancelled,
			&i.Running,
			&i.Succeeded,
			&i.Failed,
			&i.Returned,
			&i.RunAttempted,
			&i.Serial,
			&i.LastModified,
			&i.LeasedTimestamp,
			&i.PendingTimestamp,
			&i.RunningTimestamp,
			&i.TerminatedTimestamp,
			&i.ScheduledAtPriority,
			&i.Preempted,
			&i.Pending,
			&i.PreemptedTimestamp,
			&i.PodRequirementsOverlay,
			&i.PreemptRequested,
			&i.Queue,
			&i.Pool,
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
SELECT run_id, job_id, created, job_set, executor, node, cancelled, running, succeeded, failed, returned, run_attempted, serial, last_modified, leased_timestamp, pending_timestamp, running_timestamp, terminated_timestamp, scheduled_at_priority, preempted, pending, preempted_timestamp, pod_requirements_overlay, preempt_requested, queue, pool FROM runs WHERE serial > $1 AND job_id = ANY($2::text[]) ORDER BY serial
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
			&i.Node,
			&i.Cancelled,
			&i.Running,
			&i.Succeeded,
			&i.Failed,
			&i.Returned,
			&i.RunAttempted,
			&i.Serial,
			&i.LastModified,
			&i.LeasedTimestamp,
			&i.PendingTimestamp,
			&i.RunningTimestamp,
			&i.TerminatedTimestamp,
			&i.ScheduledAtPriority,
			&i.Preempted,
			&i.Pending,
			&i.PreemptedTimestamp,
			&i.PodRequirementsOverlay,
			&i.PreemptRequested,
			&i.Queue,
			&i.Pool,
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

const selectPendingJobsByQueue = `-- name: SelectPendingJobsByQueue :many
SELECT j.job_id, j.job_set, j.queue, j.user_id, j.submitted, j.groups, j.priority, j.queued, j.queued_version, j.cancel_requested, j.cancelled, j.cancel_by_jobset_requested, j.succeeded, j.failed, j.submit_message, j.scheduling_info, j.scheduling_info_version, j.serial, j.last_modified, j.validated, j.pools, j.bid_price, j.cancel_user
FROM runs jr
       JOIN jobs j
            ON jr.job_id = j.job_id
WHERE j.queue = ANY($1::text[])
  AND jr.running = false
  AND jr.pending = true
  AND jr.succeeded = false
  AND jr.failed = false
  AND jr.cancelled = false
  AND jr.preempted = false
`

func (q *Queries) SelectPendingJobsByQueue(ctx context.Context, queue []string) ([]Job, error) {
	rows, err := q.db.Query(ctx, selectPendingJobsByQueue, queue)
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
			&i.Queued,
			&i.QueuedVersion,
			&i.CancelRequested,
			&i.Cancelled,
			&i.CancelByJobsetRequested,
			&i.Succeeded,
			&i.Failed,
			&i.SubmitMessage,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Serial,
			&i.LastModified,
			&i.Validated,
			&i.Pools,
			&i.BidPrice,
			&i.CancelUser,
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

const selectQueuedJobsByQueue = `-- name: SelectQueuedJobsByQueue :many
SELECT j.job_id, j.job_set, j.queue, j.user_id, j.submitted, j.groups, j.priority, j.queued, j.queued_version, j.cancel_requested, j.cancelled, j.cancel_by_jobset_requested, j.succeeded, j.failed, j.submit_message, j.scheduling_info, j.scheduling_info_version, j.serial, j.last_modified, j.validated, j.pools, j.bid_price, j.cancel_user
FROM jobs j
WHERE j.queue = ANY($1::text[])
  AND j.queued = true
`

func (q *Queries) SelectQueuedJobsByQueue(ctx context.Context, queue []string) ([]Job, error) {
	rows, err := q.db.Query(ctx, selectQueuedJobsByQueue, queue)
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
			&i.Queued,
			&i.QueuedVersion,
			&i.CancelRequested,
			&i.Cancelled,
			&i.CancelByJobsetRequested,
			&i.Succeeded,
			&i.Failed,
			&i.SubmitMessage,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Serial,
			&i.LastModified,
			&i.Validated,
			&i.Pools,
			&i.BidPrice,
			&i.CancelUser,
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
SELECT run_id, job_id, error FROM job_run_errors WHERE run_id = ANY($1::text[])
`

// Run errors
func (q *Queries) SelectRunErrorsById(ctx context.Context, runIds []string) ([]JobRunError, error) {
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

const selectRunningJobsByQueue = `-- name: SelectRunningJobsByQueue :many
SELECT j.job_id, j.job_set, j.queue, j.user_id, j.submitted, j.groups, j.priority, j.queued, j.queued_version, j.cancel_requested, j.cancelled, j.cancel_by_jobset_requested, j.succeeded, j.failed, j.submit_message, j.scheduling_info, j.scheduling_info_version, j.serial, j.last_modified, j.validated, j.pools, j.bid_price, j.cancel_user
FROM runs jr
       JOIN jobs j
            ON jr.job_id = j.job_id
WHERE j.queue = ANY($1::text[])
  AND jr.running = true
  AND jr.returned = false
  AND jr.succeeded = false
  AND jr.failed = false
  AND jr.cancelled = false
  AND jr.preempted = false
`

func (q *Queries) SelectRunningJobsByQueue(ctx context.Context, queue []string) ([]Job, error) {
	rows, err := q.db.Query(ctx, selectRunningJobsByQueue, queue)
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
			&i.Queued,
			&i.QueuedVersion,
			&i.CancelRequested,
			&i.Cancelled,
			&i.CancelByJobsetRequested,
			&i.Succeeded,
			&i.Failed,
			&i.SubmitMessage,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Serial,
			&i.LastModified,
			&i.Validated,
			&i.Pools,
			&i.BidPrice,
			&i.CancelUser,
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

const selectUpdatedJobs = `-- name: SelectUpdatedJobs :many
SELECT job_id, job_set, queue, priority, submitted, queued, queued_version, validated, cancel_requested, cancel_user, cancel_by_jobset_requested, cancelled, succeeded, failed, scheduling_info, scheduling_info_version, pools, serial FROM jobs WHERE serial > $1 ORDER BY serial LIMIT $2
`

type SelectUpdatedJobsParams struct {
	Serial int64 `db:"serial"`
	Limit  int32 `db:"limit"`
}

type SelectUpdatedJobsRow struct {
	JobID                   string   `db:"job_id"`
	JobSet                  string   `db:"job_set"`
	Queue                   string   `db:"queue"`
	Priority                int64    `db:"priority"`
	Submitted               int64    `db:"submitted"`
	Queued                  bool     `db:"queued"`
	QueuedVersion           int32    `db:"queued_version"`
	Validated               bool     `db:"validated"`
	CancelRequested         bool     `db:"cancel_requested"`
	CancelUser              *string  `db:"cancel_user"`
	CancelByJobsetRequested bool     `db:"cancel_by_jobset_requested"`
	Cancelled               bool     `db:"cancelled"`
	Succeeded               bool     `db:"succeeded"`
	Failed                  bool     `db:"failed"`
	SchedulingInfo          []byte   `db:"scheduling_info"`
	SchedulingInfoVersion   int32    `db:"scheduling_info_version"`
	Pools                   []string `db:"pools"`
	Serial                  int64    `db:"serial"`
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
			&i.Queued,
			&i.QueuedVersion,
			&i.Validated,
			&i.CancelRequested,
			&i.CancelUser,
			&i.CancelByJobsetRequested,
			&i.Cancelled,
			&i.Succeeded,
			&i.Failed,
			&i.SchedulingInfo,
			&i.SchedulingInfoVersion,
			&i.Pools,
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

const setLeasedTime = `-- name: SetLeasedTime :exec
UPDATE runs SET leased_timestamp = $1 WHERE run_id = $2
`

type SetLeasedTimeParams struct {
	LeasedTimestamp *time.Time `db:"leased_timestamp"`
	RunID           string     `db:"run_id"`
}

func (q *Queries) SetLeasedTime(ctx context.Context, arg SetLeasedTimeParams) error {
	_, err := q.db.Exec(ctx, setLeasedTime, arg.LeasedTimestamp, arg.RunID)
	return err
}

const setPendingTime = `-- name: SetPendingTime :exec
UPDATE runs SET pending_timestamp = $1 WHERE run_id = $2
`

type SetPendingTimeParams struct {
	PendingTimestamp *time.Time `db:"pending_timestamp"`
	RunID            string     `db:"run_id"`
}

func (q *Queries) SetPendingTime(ctx context.Context, arg SetPendingTimeParams) error {
	_, err := q.db.Exec(ctx, setPendingTime, arg.PendingTimestamp, arg.RunID)
	return err
}

const setRunningTime = `-- name: SetRunningTime :exec
UPDATE runs SET running_timestamp = $1 WHERE run_id = $2
`

type SetRunningTimeParams struct {
	RunningTimestamp *time.Time `db:"running_timestamp"`
	RunID            string     `db:"run_id"`
}

func (q *Queries) SetRunningTime(ctx context.Context, arg SetRunningTimeParams) error {
	_, err := q.db.Exec(ctx, setRunningTime, arg.RunningTimestamp, arg.RunID)
	return err
}

const setTerminatedTime = `-- name: SetTerminatedTime :exec
UPDATE runs SET terminated_timestamp = $1 WHERE run_id = $2
`

type SetTerminatedTimeParams struct {
	TerminatedTimestamp *time.Time `db:"terminated_timestamp"`
	RunID               string     `db:"run_id"`
}

func (q *Queries) SetTerminatedTime(ctx context.Context, arg SetTerminatedTimeParams) error {
	_, err := q.db.Exec(ctx, setTerminatedTime, arg.TerminatedTimestamp, arg.RunID)
	return err
}

const updateJobPriorityById = `-- name: UpdateJobPriorityById :exec
UPDATE jobs SET priority = $1 WHERE queue = $2 and job_set = $3 and job_id = ANY($4::text[]) and cancelled = false and succeeded = false and failed = false
`

type UpdateJobPriorityByIdParams struct {
	Priority int64    `db:"priority"`
	Queue    string   `db:"queue"`
	JobSet   string   `db:"job_set"`
	JobIds   []string `db:"job_ids"`
}

func (q *Queries) UpdateJobPriorityById(ctx context.Context, arg UpdateJobPriorityByIdParams) error {
	_, err := q.db.Exec(ctx, updateJobPriorityById,
		arg.Priority,
		arg.Queue,
		arg.JobSet,
		arg.JobIds,
	)
	return err
}

const updateJobPriorityByJobSet = `-- name: UpdateJobPriorityByJobSet :exec
UPDATE jobs SET priority = $1 WHERE job_set = $2 and queue = $3 and cancelled = false and succeeded = false and failed = false
`

type UpdateJobPriorityByJobSetParams struct {
	Priority int64  `db:"priority"`
	JobSet   string `db:"job_set"`
	Queue    string `db:"queue"`
}

func (q *Queries) UpdateJobPriorityByJobSet(ctx context.Context, arg UpdateJobPriorityByJobSetParams) error {
	_, err := q.db.Exec(ctx, updateJobPriorityByJobSet, arg.Priority, arg.JobSet, arg.Queue)
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

const upsertExecutorSettings = `-- name: UpsertExecutorSettings :exec
INSERT INTO executor_settings (executor_id, cordoned, cordon_reason, set_by_user, set_at_time)
VALUES ($1::text, $2::boolean, $3::text, $4::text, $5::timestamptz)
ON CONFLICT (executor_id) DO UPDATE
  SET
    cordoned = excluded.cordoned,
    cordon_reason = excluded.cordon_reason,
    set_by_user = excluded.set_by_user,
    set_at_time = excluded.set_at_time
`

type UpsertExecutorSettingsParams struct {
	ExecutorID   string    `db:"executor_id"`
	Cordoned     bool      `db:"cordoned"`
	CordonReason string    `db:"cordon_reason"`
	SetByUser    string    `db:"set_by_user"`
	SetAtTime    time.Time `db:"set_at_time"`
}

func (q *Queries) UpsertExecutorSettings(ctx context.Context, arg UpsertExecutorSettingsParams) error {
	_, err := q.db.Exec(ctx, upsertExecutorSettings,
		arg.ExecutorID,
		arg.Cordoned,
		arg.CordonReason,
		arg.SetByUser,
		arg.SetAtTime,
	)
	return err
}
