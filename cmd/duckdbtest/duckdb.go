package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/util"
	_ "github.com/marcboeker/go-duckdb"
	"os"
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

	// Step 3: Create a temporary CSV file
	tmpFile, err := os.CreateTemp("", "jobs_*.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	writer := csv.NewWriter(tmpFile)
	now := time.Now()

	start := time.Now()

	// Step 4: Write 1M rows to CSV
	for i := 0; i < 10_000_000; i++ {
		jobSet := "jobsetA"
		if i >= 800_000 {
			jobSet = "jobsetB"
		}
		if i >= 1_200_000 {
			jobSet = "jobsetC"
		}
		if i >= 5_200_000 {
			jobSet = "jobsetD"
		}
		record := []string{
			`{"team":"infra","env":"prod"}`, // annotations
			"",                              // cancelled
			"2000",                          // cpu
			"false",                         // duplicate
			"1000000000",                    // ephemeral_storage
			"0",                             // gpu
			util.NewULID(),                  // job_id
			jobSet,
			"", // last_active_run_id
			now.Format(time.RFC3339Nano),
			"4096",
			"alice",
			"",
			"10",
			"",
			"main",
			"Running",
			now.Add(-5 * time.Minute).Format(time.RFC3339Nano),
			"",
			"",
			"",
			"cluster-1",
			"",
			"300",
		}
		if err := writer.Write(record); err != nil {
			log.Fatal(err)
		}
	}
	writer.Flush()
	tmpFile.Close()

	fmt.Printf("CSV generation complete: %s\n", time.Since(start))

	// Step 5: Bulk load into DuckDB using COPY
	copyStart := time.Now()
	_, err = db.Exec(`COPY jobs FROM '` + tmpFile.Name() + `' (HEADER FALSE);`)
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

	// Step 7: Optional — Check distribution
	groupStart := time.Now()
	rows, _ := db.Query(`SELECT job_set, COUNT(*) FROM jobs GROUP BY job_set`)
	for rows.Next() {
		var jobSet string
		var count int
		rows.Scan(&jobSet, &count)
		fmt.Println("JobSet:", jobSet, "Count:", count)
	}
	fmt.Printf("Group complete: %s\n", time.Since(groupStart))
	fmt.Printf("Total time: %s\n", time.Since(start))
}
