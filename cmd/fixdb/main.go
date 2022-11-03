package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/pkg/api"
	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	common.ConfigureLogging()

	const dbHost = "xxxxxx"
	const user = "xxxxxxx"
	const password = "xxxxxx"
	const database ="armada-lookout"

	// create a compressor
	compressor, err := compress.NewZlibCompressor(1024)
	if err != nil {
		log.Fatal(err)
	}

	// create a db
	db, err := pgx.Connect(context.Background(),  fmt.Sprintf("postgres://%s:%s@%s:5432?dbname=%s&sslmode=require", user, password,dbHost, database))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(context.Background())
	oneWeekAgo := time.Now().Add(-1 * 25 * time.Hour)
	var numFixed = 0
	for{
		rows, err := db.Query(context.Background(), "SELECT job_id, job from job where  job.orig_job_spec is null and  submitted >= $1 LIMIT 1", oneWeekAgo)
		if err != nil {
			log.Fatal(err)
		}
		jobId := ""
		jobYaml := ""
		numFixedBeforeQuery := numFixed
		for rows.Next(){

			// extract the job
			err = rows.Scan(&jobId, &jobYaml)
			if err != nil {
				log.Fatal(err)
			}
			apiJob := &api.Job{}
			err = json.Unmarshal([]byte(jobYaml), apiJob)
			if err != nil {
				log.Fatal(err)
			}
			apiJob.CompressedQueueOwnershipUserGroups = nil

			// convert to proto
			jobProtoUncompressed, err := proto.Marshal(apiJob)
			if err != nil {
				log.Fatal(err)
			}

			// compress
			orig_job_spec, err := compressor.Compress(jobProtoUncompressed)
			if err != nil {
				log.Fatal(err)
			}
			// insert
			db.Exec(context.Background(), "INSERT INTO job SET orig_job_spec = $1 WHERE job_id = $2", orig_job_spec, jobId)
			numFixed++
		}
		if numFixed % 1000 == 0{
			log.Infof("Fixed %d", numFixed)
		}
		if numFixed == numFixedBeforeQuery{
			log.Infof("No rows returned from query.  Exiting")
			break
		}
	}

}
