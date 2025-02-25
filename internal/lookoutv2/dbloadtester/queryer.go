package dbloadtester

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/lookoutv2/configuration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

type ReadTestConfig struct {
	Queues       []any
	Jobsets      []any
	Ids          []any
	QueueCounts  map[string]int64
	JobsetCounts map[string]int64
}

func (r ReadTestConfig) Validate() error {
	return nil
}

var queryToSummaryName = map[string]string{
	getJobByID:                            "getJobByID",
	getLookoutFrontPage:                   "getLookoutFrontPage",
	getQueueActiveJobsets:                 "getQueueActiveJobsets",
	getQueueAllJobs:                       "getQueueAllJobs",
	getJobsRunningInQueue:                 "getJobsRunningInQueue",
	getJobsetGroupedByState:               "getJobsetGroupedByState",
	getJobsRunningInQueueOrderBySubmitted: "getJobsRunningInQueueOrderBySubmitted",
}

type QueryWithParams struct {
	Query     string
	Arguments []any
}

var filterTypeToQueryMap = map[string][]string{
	"queue": []string{
		getQueueActiveJobsets,
		getQueueAllJobs,
		getJobsRunningInQueue,
		getJobsRunningInQueueOrderBySubmitted,
	},
	"jobset": []string{
		getJobsetGroupedByState,
	},
	"id": []string{
		getJobByID,
	},
	"none": []string{
		getLookoutFrontPage,
	},
}

var parallelism = 2

func DoQueries(config configuration.LookoutV2Config, readConfig ReadTestConfig, ctx *armadacontext.Context) (string, error) {
	db, err := database.OpenPgxPool(config.Postgres)
	if err != nil {
		return "", err
	}

	filterTypeToArgs := map[string][]any{
		"none":   []any{nil},
		"queue":  readConfig.Queues,
		"jobset": readConfig.Jobsets,
		"id":     readConfig.Ids,
	}

	queryTotalDuration := map[string]time.Duration{}
	queryTotalRows := map[string]int64{}
	queryTotal := map[string]int64{}

	ch := make(chan *QueryWithParams)

	queryPool := []*queryer{}
	for i := 0; i < parallelism; i++ {
		queryPool = append(queryPool, &queryer{
			db,
			ctx,
			map[string]time.Duration{},
			map[string]int64{},
			nil,
		})
	}

	var wg sync.WaitGroup
	for _, queryer := range queryPool {
		wg.Add(1)

		go func() {
			defer wg.Done()
			queryer.paralleliseQueries(ch)
		}()
	}

	for filterType, args := range filterTypeToArgs {
		for _, arg := range args {
			for _, query := range filterTypeToQueryMap[filterType] {
				if arg == nil {
					ch <- &QueryWithParams{query, []any{}}
				} else {
					ch <- &QueryWithParams{query, []any{arg}}
				}

			}
		}
	}
	close(ch)
	wg.Wait()

	for _, queryer := range queryPool {
		for query, duration := range queryer.queryTotalDuration {
			queryTotalDuration[query] += duration
			queryTotal[query] += 1
		}

		for query, rows := range queryer.queryTotalRows {
			queryTotalRows[query] += rows
		}
	}

	reportLines := []string{}

	for k, v := range queryTotalDuration {
		reportLines = append(
			reportLines,
			fmt.Sprintf("%s: Total Duration: %s - Total Rows Returned: %d, Average Duration: %s",
				queryToSummaryName[k],
				v.String(),
				queryTotalRows[k],
				(v/time.Duration(queryTotal[k])).String(),
			),
		)
	}

	jobsetCounts, err := queryPool[0].reportJobsetCount(filterTypeToArgs["jobset"], readConfig.JobsetCounts)
	if err != nil {
		return "", err
	}

	queueCounts, err := queryPool[0].reportQueueCount(filterTypeToArgs["queue"], readConfig.QueueCounts)
	if err != nil {
		return "", err
	}

	var totalQueueCounts int64
	for _, v := range queueCounts {
		totalQueueCounts += v
	}

	var totalJobsetCounts int64
	for _, v := range jobsetCounts {
		totalJobsetCounts += v
	}

	reportLines = append(
		reportLines,
		fmt.Sprintf("\nTotal Queues queried: %d", len(filterTypeToArgs["queue"])),
		fmt.Sprintf("Total IDs queried: %d", len(filterTypeToArgs["id"])),
		fmt.Sprintf("Total Jobsets queried: %d", len(filterTypeToArgs["jobset"])),
		fmt.Sprintf("Total Rows of Queues: %d, Total Rows of Jobsets: %d",
			totalQueueCounts,
			totalJobsetCounts,
		),
	)

	return strings.Join(reportLines, "\n"), err
}

type queryer struct {
	db                 *pgxpool.Pool
	ctx                *armadacontext.Context
	queryTotalDuration map[string]time.Duration
	queryTotalRows     map[string]int64
	err                error
}

func (q *queryer) reportJobsetCount(jobsets []any, precalculated map[string]int64) (map[string]int64, error) {
	jobsetToCount := map[string]int64{}
	for _, jobset := range jobsets {
		if _, ok := precalculated[jobset.(string)]; ok {
			jobsetToCount[jobset.(string)] = precalculated[jobset.(string)]
			continue
		}

		log.Infof("Counting rows available to be queried with jobset: %s", jobset)
		rows, err := q.db.Query(
			q.ctx,
			`SELECT COUNT(*)
				FROM job as j
				WHERE j.jobset = $1`,
			jobset,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to query jobset rows: %v", err)
		}

		var rowCount int64
		if rows.Next() {
			err = rows.Scan(&rowCount)
			if err != nil {
				return nil, fmt.Errorf("failed to count jobset rows: %v", err)
			}
		}
		rows.Close()

		jobsetToCount[jobset.(string)] = rowCount
	}

	return jobsetToCount, nil
}

func (q *queryer) reportQueueCount(queues []any, precalculated map[string]int64) (map[string]int64, error) {
	queueToCount := map[string]int64{}
	for _, queue := range queues {
		if _, ok := precalculated[queue.(string)]; ok {
			queueToCount[queue.(string)] = precalculated[queue.(string)]
			continue
		}

		log.Infof("Counting rows available to be queried with queue: %s", queue)
		rows, err := q.db.Query(
			q.ctx,
			`SELECT COUNT(*)
				FROM job as j
				WHERE j.queue = $1`,
			queue,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to query queue rows: %v", err)
		}

		var rowCount int64
		if rows.Next() {
			err = rows.Scan(&rowCount)
			if err != nil {
				return nil, fmt.Errorf("failed to count queue rows: %v", err)
			}
		}
		rows.Close()

		queueToCount[queue.(string)] = rowCount
	}

	return queueToCount, nil
}

func (q *queryer) paralleliseQueries(ch chan *QueryWithParams) {
	for queryWithArgs := range ch {
		duration, rows, err := doQuery(q.db, q.ctx, queryWithArgs.Query, queryWithArgs.Arguments)
		if err != nil {
			q.err = err
			return
		}
		q.queryTotalRows[queryWithArgs.Query] += rows
		q.queryTotalDuration[queryWithArgs.Query] += duration
		log.Infof("Query Type: %s, Query Duration: %s, Rows Returned: %d", queryToSummaryName[queryWithArgs.Query], duration.String(), rows)
	}

	return

}

func doQuery(db *pgxpool.Pool, ctx *armadacontext.Context, query string, args []any) (queryDuration time.Duration, rowsReturned int64, err error) {
	var startTime time.Time
	startTimeFunc := func(rows pgx.Row) error {
		return rows.Scan(&startTime)
	}

	var endTime time.Time
	endTimeFunc := func(rows pgx.Row) error {
		return rows.Scan(&endTime)
	}

	var totalRows int64
	totalRowFunc := func(rows pgx.Rows) error {
		for rows.Next() {
			totalRows += 1
		}
		return nil
	}
	tx, err := db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, 0, err
	}

	log.Infof("Beginning query of type with args: %s, %v", queryToSummaryName[query], args)

	batch := &pgx.Batch{}
	batch.Queue("SELECT clock_timestamp()").QueryRow(startTimeFunc)
	batch.Queue(query, args...).Query(totalRowFunc)
	batch.Queue("SELECT clock_timestamp()").QueryRow(endTimeFunc)
	err = tx.SendBatch(ctx, batch).Close()
	if err != nil {
		return 0, 0, err
	}

	if tx.Commit(ctx) != nil {
		return 0, 0, err
	}

	duration := endTime.Sub(startTime)

	return duration, totalRows, nil
}
