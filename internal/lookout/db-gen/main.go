package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"
)

const (
	QUEUED    int = 1
	PENDING   int = 2
	RUNNING   int = 3
	SUCCEEDED int = 4
	FAILED    int = 5
	CANCELLED int = 6
)

var (
	ClustersN int = 5
	QueuesN   int = 10
	OwnersN   int = 100
	JobSetsN  int = 100
	NodesN    int = 100
)

type JobRun struct {
	runId            string
	jobId            string
	cluster          string
	node             string
	podNumber        int
	unableToSchedule string
}

type Job struct {
	jobId      string
	queue      string
	owner      string
	jobset     string
	priority   int
	submitted  time.Time
	state      int
	jobUpdated time.Time
}

var wg sync.WaitGroup

func main() {
	rand.Seed(time.Now().UnixNano())

	JobsN := flag.Int("jobs", 10000, "number of jobs to be generated")

	db := setupDB()
	defer db.Close()

	for i := 0; i < *JobsN; i++ {
		job_id := uuid.New().String()[:32]
		newJob := Job{
			jobId:     job_id,
			queue:     concat("queue-", QueuesN),
			owner:     concat("owner-", OwnersN),
			jobset:    concat("job-set-", JobSetsN),
			priority:  rand.Intn(10),
			submitted: Now(),
			state:     QUEUED,
		}
		newJobRun := JobRun{
			runId:            uuid.NewString()[:32],
			jobId:            job_id,
			cluster:          concat("Cluster-", ClustersN),
			node:             concat("node-", NodesN),
			podNumber:        rand.Intn(10),
			unableToSchedule: "false",
		}

		insert_job := `INSERT INTO job (job_id, queue, owner, jobset, priority, submitted, job_updated, state) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);`
		insert_job_run := `INSERT INTO job_run (run_id, job_id, cluster, node, pod_number, unable_to_schedule) VALUES ($1, $2, $3, $4, $5, $6);`

		_, err := db.Exec(insert_job, newJob.jobId, newJob.queue, newJob.owner, newJob.jobset, newJob.priority, newJob.submitted, newJob.jobUpdated, newJob.state)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't insert into job table\n%v\n", err)
			continue
		}

		_, err = db.Exec(insert_job_run, newJobRun.runId, newJobRun.jobId, newJobRun.cluster, newJobRun.node, newJobRun.podNumber, newJobRun.unableToSchedule)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't insert into job_run table\n%v\n", err)
			continue
		}
		wg.Add(1)
		go updateJobState(&job_id, db)
	}
	wg.Wait()
}

/* Update job state from Queued -> Pending -> Running -> pick random jobs to either fail or cancel, otherwise succeed */
func updateJobState(job_id *string, db *sql.DB) {
	defer wg.Done()

	pending(job_id, db)
	running(job_id, db)
	switch r := rand.Intn(6); r {
	case 0:
		fail(job_id, db)
	case 1:
		cancel(job_id, db)
	default:
		success(job_id, db)
	}
}

func pending(job_id *string, db *sql.DB) {
	time.Sleep(3 * time.Second)
	updateTime := Now()

	updateState := `UPDATE job SET state = $1, job_updated = $2 WHERE job_id = $3;`
	_, err := db.Exec(updateState, PENDING, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	updateCreated := `UPDATE job_run SET created = $1 WHERE job_id = $2;`
	_, err = db.Exec(updateCreated, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}

func running(job_id *string, db *sql.DB) {
	time.Sleep(3 * time.Second)
	updateTime := Now()

	updateState := `UPDATE job SET state = $1, job_updated = $2 WHERE job_id = $3;`
	_, err := db.Exec(updateState, RUNNING, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	updateStarted := `UPDATE job_run SET started = $1 WHERE job_id = $2`
	_, err = db.Exec(updateStarted, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}

func success(job_id *string, db *sql.DB) {
	time.Sleep(3 * time.Second)
	updateTime := Now()

	updateState := `UPDATE job SET state = $1, job_updated = $2 WHERE job_id = $3;`
	_, err := db.Exec(updateState, SUCCEEDED, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	updateFinishedSucceeded := `UPDATE job_run SET finished = $1, succeeded = true WHERE job_id = $2;`
	_, err = db.Exec(updateFinishedSucceeded, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}

func fail(job_id *string, db *sql.DB) {
	time.Sleep(3 * time.Second)
	updateTime := Now()

	updateState := `UPDATE job SET state = $1, job_updated = $2 WHERE job_id = $3;`
	_, err := db.Exec(updateState, FAILED, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}

	updateFinishedFailed := `UPDATE job_run SET finished = $1, succeeded = false, error = 'Unexpected error' WHERE job_id = $2;`
	_, err = db.Exec(updateFinishedFailed, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}

func cancel(job_id *string, db *sql.DB) {
	time.Sleep(3 * time.Second)
	updateTime := Now()

	updateState := `UPDATE job SET state = $1, job_updated = $2, cancelled = $2 WHERE job_id = $3;`
	_, err := db.Exec(updateState, CANCELLED, updateTime, job_id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}
}

/* Some helper functions */
func setupDB() *sql.DB {
	host := flag.String("host", "localhost", "database host")
	port := flag.String("port", "5432", "database port")
	user := flag.String("user", "postgres", "database user")
	password := flag.String("password", "psw", "database password")
	flag.Parse()
	connectionString := fmt.Sprintf("host=%s port=%s user=%s password=%s sslmode=disable", *host, *port, *user, *password)
	db, err := sql.Open("pgx", connectionString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't connect to the database\n%v\n", err)
		os.Exit(1)
	}
	db.SetConnMaxLifetime(100)
	db.SetConnMaxIdleTime(10)
	return db
}

func concat(element string, limit int) string {
	n := rand.Intn(limit) + 1
	return element + strconv.Itoa(n)
}

func Now() time.Time {
	format := "2006-01-02T15:04:05.000Z"
	formattedTime := time.Now().UTC().Format(format)
	res, _ := time.Parse(format, formattedTime)
	return res
}
