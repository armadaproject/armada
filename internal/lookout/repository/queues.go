package repository

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/gogo/protobuf/types"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/pkg/api/lookout"
)

type countsRow struct {
	Queued  uint32 `db:"queued"`
	Pending uint32 `db:"pending"`
	Running uint32 `db:"running"`
}

type rowsSql struct {
	Counts         string
	OldestQueued   string
	LongestRunning string
}

func (r *SQLJobRepository) GetQueueInfos(ctx context.Context) ([]*lookout.QueueInfo, error) {
	queries, err := r.getQueuesSql()
	if err != nil {
		return nil, err
	}

	var countRows *sql.Rows
	var longestRunningRows *sql.Rows
	var oldestQueuedRows *sql.Rows

	defer func() {
		if countRows != nil {
			err := countRows.Close()
			if err != nil {
				logrus.Errorf("Failed to close SQL connection: %v", err)
			}
		}

		if longestRunningRows != nil {
			err := longestRunningRows.Close()
			if err != nil {
				logrus.Errorf("Failed to close SQL connection: %v", err)
			}
		}

		if oldestQueuedRows != nil {
			err := oldestQueuedRows.Close()
			if err != nil {
				logrus.Errorf("Failed to close SQL connection: %v", err)
			}
		}
	}()
	countRows, err = r.goquDb.Db.QueryContext(ctx, queries.Counts)
	if err != nil {
		return nil, err
	}

	longestRunningRows, err = r.goquDb.Db.QueryContext(ctx, queries.LongestRunning)
	if err != nil {
		return nil, err
	}

	oldestQueuedRows, err = r.goquDb.Db.QueryContext(ctx, queries.OldestQueued)
	if err != nil {
		return nil, err
	}

	result, err := r.rowsToQueues(countRows, oldestQueuedRows, longestRunningRows)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *SQLJobRepository) getQueuesSql() (rowsSql, error) {
	countsDs := r.goquDb.
		From(jobTable).
		Select(
			job_queue,
			goqu.L("COUNT(*) FILTER (WHERE job.state = 1)").As("queued"),
			goqu.L("COUNT(*) FILTER (WHERE job.state = 2)").As("pending"),
			goqu.L("COUNT(*) FILTER (WHERE job.state = 3)").As("running")).
		Where(job_state.In(JobStateToIntMap[JobQueued], JobStateToIntMap[JobPending], JobStateToIntMap[JobRunning])).
		GroupBy(job_queue).
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
			job_orig_job_spec,
			jobRun_created,
			jobRun_started,
			jobRun_finished).
		Distinct(job_queue).
		Where(job_state.Eq(JobStateToIntMap[JobQueued])).
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
			job_orig_job_spec,
			jobRun_started).
		Distinct(job_queue).
		Where(job_state.Eq(JobStateToIntMap[JobRunning])).
		Order(job_queue.Asc(), jobRun_started.Asc()).
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
			goqu.I("longest_running_sub.orig_job_spec"),
			jobRun_runId,
			jobRun_cluster,
			jobRun_node,
			jobRun_created,
			jobRun_started,
			jobRun_finished).
		Order(jobRun_runId.Asc(), jobRun_started.Asc()).
		As("longest_running")

	countsSql, _, err := countsDs.ToSQL()
	if err != nil {
		return rowsSql{}, err
	}
	oldestQueuedSql, _, err := oldestQueuedDs.ToSQL()
	if err != nil {
		return rowsSql{}, err
	}
	longestRunningSql, _, err := longestRunningDs.ToSQL()
	if err != nil {
		return rowsSql{}, err
	}
	return rowsSql{countsSql, oldestQueuedSql, longestRunningSql}, nil
}

func (r *SQLJobRepository) rowsToQueues(counts *sql.Rows, oldestQueued *sql.Rows, longestRunning *sql.Rows) ([]*lookout.QueueInfo, error) {
	queueInfoMap := make(map[string]*lookout.QueueInfo)

	// Job counts
	err := setJobCounts(counts, queueInfoMap)
	if err != nil {
		return nil, err
	}

	// Oldest queued
	err = r.setOldestQueuedJob(oldestQueued, queueInfoMap)
	if err != nil {
		return nil, err
	}

	// Longest Running
	err = r.setLongestRunningJob(longestRunning, queueInfoMap)
	if err != nil {
		return nil, err
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
			&row.OrigJobSpec,
			&row.Created,
			&row.Started,
			&row.Finished)
		if err != nil {
			return err
		}
		if row.Queue.Valid {
			if queueInfo, ok := queueInfoMap[row.Queue.String]; queueInfo != nil && ok {
				job, jobJson, err := makeJobFromRow(&row)
				if err != nil {
					return err
				}
				queueInfo.OldestQueuedJob = &lookout.JobInfo{
					Job:       job,
					Runs:      []*lookout.RunInfo{},
					Cancelled: nil,
					JobState:  string(JobQueued),
					JobJson:   jobJson,
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
			&row.OrigJobSpec,
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
					job, jobJson, err := makeJobFromRow(&row)
					if err != nil {
						return err
					}
					queueInfo.LongestRunningJob = &lookout.JobInfo{
						Job:       job,
						Runs:      []*lookout.RunInfo{makeRunFromRow(&row)},
						Cancelled: nil,
						JobState:  string(JobRunning),
						JobJson:   jobJson,
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
		err := rows.Scan(&queue, &row.Queued, &row.Pending, &row.Running)
		if err != nil {
			return err
		}
		queueInfoMap[queue] = &lookout.QueueInfo{
			Queue:             queue,
			JobsQueued:        row.Queued,
			JobsPending:       row.Pending,
			JobsRunning:       row.Running,
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
