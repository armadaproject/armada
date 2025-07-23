package main

import (
	"database/sql"
	"fmt"
	log "github.com/armadaproject/armada/internal/common/logging"
	_ "github.com/marcboeker/go-duckdb"
	"math/rand"
	"sync"
	"time"
)

type Job struct {
	Annotations        map[string]string
	Cancelled          *time.Time
	Cpu                int64
	Duplicate          bool
	EphemeralStorage   int64
	Gpu                int64
	JobId              string
	JobSet             string
	LastActiveRunId    *string
	LastTransitionTime time.Time
	Memory             int64
	Owner              string
	Namespace          *string
	Priority           int64
	PriorityClass      *string
	Queue              string
	State              string
	Submitted          time.Time
	CancelReason       *string
	CancelUser         *string
	Node               *string
	Cluster            string
	ExitCode           *int32
	RuntimeSeconds     int32
}

func main() {
	// Step 1: Open DuckDB in-memory
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Step 2: Define the jobs table schema
	_, err = db.Exec(`
		CREATE TABLE jobs (
			annotations TEXT,
			cancelled TIMESTAMP,
			cpu BIGINT,
			duplicate BOOLEAN,
			ephemeral_storage BIGINT,
			gpu BIGINT,
			job_id TEXT,
			job_set TEXT,
			last_active_run_id TEXT,
			last_transition_time TIMESTAMP,
			memory BIGINT,
			owner TEXT,
			namespace TEXT,
			priority BIGINT,
			priority_class TEXT,
			queue TEXT,
			state TEXT,
			submitted TIMESTAMP,
			cancel_reason TEXT,
			cancel_user TEXT,
			node TEXT,
			cluster TEXT,
			exit_code INTEGER,
			runtime_seconds INTEGER
		)
	`)
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	//// Step 3: Create a CSV file
	//jobsCsv, err := os.Create("jobs.csv")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//
	//writer := csv.NewWriter(jobsCsv)
	//now := time.Now()
	//
	//// Step 4: Write rows to CSV
	//for i := 0; i < 100_000_000; i++ {
	//	jobSet := "jobsetA"
	//	if i >= 800_000 {
	//		jobSet = "jobsetB"
	//	}
	//	if i >= 1_200_000 {
	//		jobSet = "jobsetC"
	//	}
	//	if i >= 5_200_000 {
	//		jobSet = "jobsetD"
	//	}
	//	record := []string{
	//		`{"team":"infra","env":"prod"}`, // annotations
	//		"",                              // cancelled
	//		"2000",                          // cpu
	//		"false",                         // duplicate
	//		"1000000000",                    // ephemeral_storage
	//		"0",                             // gpu
	//		util.NewULID(),                  // job_id
	//		jobSet,
	//		"", // last_active_run_id
	//		now.Format(time.RFC3339Nano),
	//		"4096",
	//		"alice",
	//		"",
	//		"10",
	//		"",
	//		"main",
	//		getState(),
	//		now.Add(-time.Duration(i/100) * time.Second).Add(time.Duration(rand.Intn(1000)) * time.Millisecond).Format(time.RFC3339Nano),
	//		"",
	//		"",
	//		"",
	//		"cluster-1",
	//		"",
	//		"300",
	//	}
	//	if err := writer.Write(record); err != nil {
	//		log.Fatal(err)
	//	}
	//}
	//writer.Flush()
	//_ = jobsCsv.Close()
	//
	//fmt.Printf("CSV generation complete: %s\n", time.Since(start))
	//
	// Step 5: Bulk load into DuckDB using COPY
	copyStart := time.Now()
	_, err = db.Exec(`COPY jobs FROM '` + "jobs.csv" + `' (HEADER FALSE);`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("COPY INTO complete: %s\n", time.Since(copyStart))

	// Step 6: Validate
	var count int
	err = db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total rows loaded: %d\n", count)

	// Step 7: Group by job set and sate
	groupStart := time.Now()
	rows, _ := db.Query(`SELECT job_set, state, COUNT(*) FROM jobs GROUP BY job_set, state`)
	for rows.Next() {
		var jobSet string
		var state string
		var count int
		err := rows.Scan(&jobSet, &state, &count)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("JobSet:", jobSet, "State:", state, "Count:", count)
	}
	fmt.Printf("Group complete: %s\n", time.Since(groupStart))

	// Step 8: Sort all jobs by submitted and pick the top 500
	sortStart := time.Now()
	rows, err = db.Query(`SELECT job_id, job_set, state FROM jobs ORDER BY submitted ASC LIMIT 5`)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var jobId string
		var jobSet string
		var state string
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

	for _, id := range jobIDs {
		wg.Add(1)
		go func(jobId string) {
			defer wg.Done()
			var jobSet, state string
			err := db.QueryRow(`SELECT job_set, state FROM jobs WHERE job_id = ?`, jobId).Scan(&jobSet, &state)
			mu.Lock()
			defer mu.Unlock()
			if err == sql.ErrNoRows {
				notFound = append(notFound, jobId)
			} else if err != nil {
				log.Errorf("Lookup failed for %s: %v", jobId, err)
			} else {
				found = append(found, jobId)
			}
		}(id)
	}

	wg.Wait()
	fmt.Printf("Parallel lookups complete: %s\n", time.Since(lookupStart))
	fmt.Printf("Found: %d, Not found: %d\n", len(found), len(notFound))

	fmt.Printf("Total time: %s\n", time.Since(start))
}

func getState() string {
	r := rand.Int63n(4)
	if r == 0 {
		return "QUEUED"
	}
	if r == 1 {
		return "LEASED"
	}
	if r == 2 {
		return "PENDING"
	}
	if r == 3 {
		return "RUNNING"
	}
	return "SUCCEEDED"
}
