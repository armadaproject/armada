package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/gogo/protobuf/types"
	"github.com/sirupsen/logrus"

	"github.com/G-Research/armada/pkg/api/lookout"
)

type countsRow struct {
	Jobs        uint32 `db:"jobs"`
	JobsCreated uint32 `db:"jobs_created"`
	JobsStarted uint32 `db:"jobs_started"`
}

func (r *SQLJobRepository) GetQueueInfos(ctx context.Context) ([]*lookout.QueueInfo, error) {
	queries, err := r.getQueuesSql()
	if err != nil {
		return nil, err
	}

	rows, err := r.goquDb.Db.QueryContext(ctx, queries)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			logrus.Fatalf("Failed to close SQL connection: %v", err)
		}
	}()

	result, err := r.rowsToQueues(rows)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *SQLJobRepository) getQueuesSql() (string, error) {
	countsSubDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobId,
			job_queue,
			jobRun_created,
			jobRun_started).
		Where(goqu.And(
			job_cancelled.IsNull(),
			jobRun_finished.IsNull(),
			jobRun_unableToSchedule.IsNull())).
		// GroupBy(job_jobId).
		// Having(goqu.And(goqu.MAX(jobRun_finished).IsNull(), job_cancelled.IsNull())).
		As("counts_sub") // Identify unique created and started jobs

	countsDs := r.goquDb.
		From(countsSubDs).
		Select(
			goqu.I("counts_sub.queue"),
			goqu.COUNT("*").As("jobs"),
			goqu.COUNT(
				goqu.COALESCE(
					goqu.I("counts_sub.created"),
					goqu.I("counts_sub.started"))).As("jobs_created"),
			goqu.COUNT(goqu.I("counts_sub.started")).As("jobs_started")).
		GroupBy(goqu.I("counts_sub.queue")).
		As("counts")

	oldestQueuedDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobId,
			job_jobset,
			job_queue,
			job_owner,
			job_priority,
			job_submitted,
			jobRun_created,
			jobRun_started,
			jobRun_finished).
		Distinct(job_queue).
		Where(goqu.And(
			job_submitted.IsNotNull(),
			job_cancelled.IsNull(),
			jobRun_created.IsNull(),
			jobRun_started.IsNull(),
			jobRun_finished.IsNull())).
		Order(job_queue.Asc(), job_submitted.Asc()).
		As("oldest_queued")

	longestRunningSubDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobId,
			job_jobset,
			job_queue,
			job_owner,
			job_priority,
			job_submitted,
			goqu.MAX(jobRun_started).As("started")).
		Distinct(job_queue).
		GroupBy(job_jobId).
		Having(goqu.And(FiltersForState[JobStates.Running]...)).
		Order(job_queue.Asc(), goqu.I("started").Asc()).
		As("longest_running_sub") // Identify longest Running jobs

	longestRunningDs := r.goquDb.
		From(longestRunningSubDs).
		LeftJoin(jobRunTable, goqu.On(goqu.I("longest_running_sub.job_id").Eq(jobRun_jobId))).
		Select(
			goqu.I("longest_running_sub.job_id"),
			goqu.I("longest_running_sub.jobset"),
			goqu.I("longest_running_sub.queue"),
			goqu.I("longest_running_sub.owner"),
			goqu.I("longest_running_sub.priority"),
			goqu.I("longest_running_sub.submitted"),
			jobRun_runId,
			jobRun_cluster,
			jobRun_node,
			jobRun_created,
			jobRun_started,
			jobRun_finished).
		As("longest_running")

	countsSql, _, err := countsDs.ToSQL()
	if err != nil {
		return "", err
	}
	oldestQueuedSql, _, err := oldestQueuedDs.ToSQL()
	if err != nil {
		return "", err
	}
	longestRunningSql, _, err := longestRunningDs.ToSQL()
	if err != nil {
		return "", err
	}

	// Execute three unprepared statements sequentially.
	// There are no parameters and we don't care if updates happen between queries.
	return strings.Join([]string{countsSql, oldestQueuedSql, longestRunningSql}, " ; "), nil
}

func (r *SQLJobRepository) rowsToQueues(rows *sql.Rows) ([]*lookout.QueueInfo, error) {
	queueInfoMap := make(map[string]*lookout.QueueInfo)

	// Job counts
	err := setJobCounts(rows, queueInfoMap)
	if err != nil {
		return nil, err
	}

	// Oldest queued
	if rows.NextResultSet() {
		err = r.setOldestQueuedJob(rows, queueInfoMap)
		if err != nil {
			return nil, err
		}
	} else if rows.Err() != nil {
		return nil, fmt.Errorf("expected result set for oldest queued job: %v", rows.Err())
	}

	// Longest Running
	if rows.NextResultSet() {
		err = r.setLongestRunningJob(rows, queueInfoMap)
		if err != nil {
			return nil, err
		}
	} else if rows.Err() != nil {
		return nil, fmt.Errorf("expected result set for longest Running job: %v", rows.Err())
	}

	result := getSortedQueueInfos(queueInfoMap)
	return result, nil
}

func (r *SQLJobRepository) setOldestQueuedJob(rows *sql.Rows, queueInfoMap map[string]*lookout.QueueInfo) error {
	for rows.Next() {
		var row JobRow
		err := rows.Scan(
			&row.JobId,
			&row.JobSet,
			&row.Queue,
			&row.Owner,
			&row.Priority,
			&row.Submitted,
			&row.Created,
			&row.Started,
			&row.Finished)
		if err != nil {
			return err
		}
		if row.Queue.Valid {
			if queueInfo, ok := queueInfoMap[row.Queue.String]; queueInfo != nil && ok {
				queueInfo.OldestQueuedJob = &lookout.JobInfo{
					Job:       makeJobFromRow(&row),
					Runs:      []*lookout.RunInfo{},
					Cancelled: nil,
					JobState:  JobStates.Queued,
				}
				currentTime := r.clock.Now()
				submissionTime := queueInfo.OldestQueuedJob.Job.Created
				queueInfo.OldestQueuedDuration = types.DurationProto(currentTime.Sub(submissionTime).Round(time.Second))
			}
		}
	}
	return nil
}

func (r *SQLJobRepository) setLongestRunningJob(rows *sql.Rows, queueInfoMap map[string]*lookout.QueueInfo) error {
	for rows.Next() {
		var row JobRow
		err := rows.Scan(
			&row.JobId,
			&row.JobSet,
			&row.Queue,
			&row.Owner,
			&row.Priority,
			&row.Submitted,
			&row.RunId,
			&row.Cluster,
			&row.Node,
			&row.Created,
			&row.Started,
			&row.Finished)
		if err != nil {
			return err
		}
		if row.Queue.Valid {
			if queueInfo, ok := queueInfoMap[row.Queue.String]; queueInfo != nil && ok {
				if queueInfo.LongestRunningJob != nil {
					queueInfo.LongestRunningJob.Runs = append(queueInfo.LongestRunningJob.Runs, makeRunFromRow(&row))
				} else {
					queueInfo.LongestRunningJob = &lookout.JobInfo{
						Job:       makeJobFromRow(&row),
						Runs:      []*lookout.RunInfo{makeRunFromRow(&row)},
						Cancelled: nil,
						JobState:  JobStates.Running,
					}
				}
			}
		}
	}

	// Set duration of longest Running job for each queue
	for _, queueInfo := range queueInfoMap {
		startTime := getJobStartTime(queueInfo.LongestRunningJob)
		if startTime != nil {
			currentTime := r.clock.Now()
			queueInfo.LongestRunningDuration = types.DurationProto(currentTime.Sub(*startTime).Round(time.Second))
		}
	}

	return nil
}

func setJobCounts(rows *sql.Rows, queueInfoMap map[string]*lookout.QueueInfo) error {
	for rows.Next() {
		var (
			queue string
			row   countsRow
		)
		err := rows.Scan(&queue, &row.Jobs, &row.JobsCreated, &row.JobsStarted)
		if err != nil {
			return err
		}
		queueInfoMap[queue] = &lookout.QueueInfo{
			Queue:             queue,
			JobsQueued:        row.Jobs - row.JobsCreated,
			JobsPending:       row.JobsCreated - row.JobsStarted,
			JobsRunning:       row.JobsStarted,
			OldestQueuedJob:   nil,
			LongestRunningJob: nil,
		}
	}
	return nil
}

func getSortedQueueInfos(resultMap map[string]*lookout.QueueInfo) []*lookout.QueueInfo {
	var queues []string
	for queue := range resultMap {
		queues = append(queues, queue)
	}
	sort.Strings(queues)

	var result []*lookout.QueueInfo
	for _, queue := range queues {
		result = append(result, resultMap[queue])
	}
	return result
}

// Returns the time a given job started Running, based on latest job run
func getJobStartTime(job *lookout.JobInfo) *time.Time {
	if job == nil || len(job.Runs) == 0 {
		return nil
	}
	latestRun := job.Runs[len(job.Runs)-1]
	return latestRun.Started
}
