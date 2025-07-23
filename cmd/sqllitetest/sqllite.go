package main

import (
	"context"
	"database/sql"
	"fmt"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/util"
	_ "github.com/mattn/go-sqlite3"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	JobId      string
	JobSet     string
	State      string
	Submitted  time.Time
	Owner      string
	Queue      string
	Cpu        int64
	Memory     int64
	Cluster    string
	RuntimeSec int32
}

func main() {
	db, err := sql.Open("sqlite3", "file:sharedmem?mode=memory&cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE jobs (
			job_id TEXT,
			job_set TEXT,
			state TEXT,
			submitted TIMESTAMP,
			owner TEXT,
			queue TEXT,
			cpu BIGINT,
			memory BIGINT,
			cluster TEXT,
			runtime_seconds INTEGER
		)
	`)
	if err != nil {
		log.Fatal(err)
	}

	// Add indexes to speed up GROUP BY and ORDER BY
	_, err = db.Exec(`
		CREATE INDEX idx_jobs_jobset_state ON jobs(job_set, state);
		CREATE INDEX idx_jobs_submitted ON jobs(submitted);
	`)
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	jobs := make([]Job, 0, 10_000_000)
	now := time.Now()

	for i := 0; i < 10_000_000; i++ {
		jobSet := "jobsetA"
		switch {
		case i >= 800_000 && i <= 1_200_000:
			jobSet = "jobsetB"
		case i >= 1_200_000 && i <= 5_200_000:
			jobSet = "jobsetC"
		case i >= 5_200_000:
			jobSet = "jobsetD"
		}
		jobs = append(jobs, Job{
			JobId:      util.NewULID(),
			JobSet:     jobSet,
			State:      getState(),
			Submitted:  now.Add(-time.Duration(i/100) * time.Second),
			Owner:      "alice",
			Queue:      "main",
			Cpu:        2000,
			Memory:     4096,
			Cluster:    "cluster-1",
			RuntimeSec: 300,
		})
	}
	fmt.Printf("Job generation complete: %s\n", time.Since(start))

	// Bulk insert in transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare(`
		INSERT INTO jobs (
			job_id, job_set, state, submitted,
			owner, queue, cpu, memory,
			cluster, runtime_seconds
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Fatal(err)
	}
	for _, job := range jobs {
		_, err := stmt.Exec(
			job.JobId, job.JobSet, job.State, job.Submitted,
			job.Owner, job.Queue, job.Cpu, job.Memory,
			job.Cluster, job.RuntimeSec,
		)
		if err != nil {
			log.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Insert complete: %s\n", time.Since(start))

	// Validate
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total rows loaded: %d\n", count)

	// Group
	groupStart := time.Now()
	rows, err := db.Query(`SELECT job_set, state, COUNT(*) FROM jobs GROUP BY job_set, state`)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var jobSet, state string
		var count int
		err := rows.Scan(&jobSet, &state, &count)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("JobSet:", jobSet, "State:", state, "Count:", count)
	}
	fmt.Printf("Group complete: %s\n", time.Since(groupStart))

	// Sort and limit
	sortStart := time.Now()
	rows, err = db.Query(`SELECT job_id, job_set, state FROM jobs ORDER BY submitted ASC LIMIT 5`)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var jobId, jobSet, state string
		err := rows.Scan(&jobId, &jobSet, &state)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("JobId:", jobId, "JobSet:", jobSet, "State:", state)
	}
	fmt.Printf("Select complete: %s\n", time.Since(sortStart))

	// 5 known existing job IDs (replace with actual values)
	jobIDs := make([]string, 1000)
	for i := 0; i < 200; i++ {
		knownExistingIDs := []string{
			"01k03vw1nzkmznk3dzejd9hkpm",
			"01k03vv9vrq9h035sfbrb2z0vs",
			"01k03vv9vrq9h035sfbrb2z0vs",
			"01k03vwxp24epyy9y5ednja6hd",
			"01k03vvdxrrcrgs70yrp2n0tk3",
		}
		jobIDs = append(jobIDs, knownExistingIDs...)
	}

	fmt.Println("Starting parallel job_id lookups...")
	lookupStart := time.Now()

	var wg sync.WaitGroup
	var mu sync.Mutex

	found := make([]string, 0, 5)
	notFound := make([]string, 0, 5)

	for _, j := range jobs[0:1000] {
		wg.Add(1)
		go func(jobId string) {
			defer wg.Done()
			var jobSet, state string
			conn, err := db.Conn(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			err = conn.QueryRowContext(context.Background(), `SELECT job_set, state FROM jobs WHERE job_id = ?`, jobId).Scan(&jobSet, &state)
			mu.Lock()
			defer mu.Unlock()
			if err == sql.ErrNoRows {
				notFound = append(notFound, jobId)
			} else if err != nil {
				log.Errorf("Lookup failed for %s: %v", jobId, err)
			} else {
				found = append(found, jobId)
			}
		}(j.JobId)
	}

	wg.Wait()
	fmt.Printf("Parallel lookups complete: %s\n", time.Since(lookupStart))
	fmt.Printf("Found: %d, Not found: %d\n", len(found), len(notFound))

	fmt.Printf("Total time: %s\n", time.Since(start))

}

func getState() string {
	switch rand.Intn(4) {
	case 0:
		return "QUEUED"
	case 1:
		return "LEASED"
	case 2:
		return "PENDING"
	case 3:
		return "RUNNING"
	default:
		return "SUCCEEDED"
	}
}
