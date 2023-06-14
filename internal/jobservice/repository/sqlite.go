//go:generate moq -out sql_job_service_moq.go . JobTableUpdater
package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/jobservice/configuration"
	js "github.com/armadaproject/armada/pkg/api/jobservice"
)

// JSRepoSQLite for persisting to DB.
type JSRepoSQLite struct {
	jobServiceConfig *configuration.JobServiceConfiguration
	db               *sql.DB
	lock             sync.RWMutex
}

func NewJSRepoSQLite(config *configuration.JobServiceConfiguration, log *log.Entry) (error, *JSRepoSQLite, func()) {
	var err error

	dbDir := filepath.Dir(config.DatabasePath)
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		if errMkDir := os.Mkdir(dbDir, 0o755); errMkDir != nil {
			errMsg := fmt.Sprintf("error: could not make directory at %s for sqlite db: %v", dbDir, errMkDir)
			return errors.New(errMsg), nil, func() {}
		}
	}

	sqliteDb, err := sql.Open("sqlite", config.DatabasePath)
	if err != nil {
		errMsg := fmt.Sprintf("error opening sqlite DB from %s %v", config.DatabasePath, err)
		return errors.New(errMsg), nil, func() {}
	}

	return nil, &JSRepoSQLite{jobServiceConfig: config, db: sqliteDb}, func() {
		if err := sqliteDb.Close(); err != nil {
			log.Warnf("error closing database: %v", err)
		}
	}
}

// Set up the DB for use, create tables
func (s *JSRepoSQLite) Setup(ctx context.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	setupStmts := []string{
		"PRAGMA journal_mode=WAL",
		`PRAGMA foreign_keys = ON`,
		`DROP TABLE IF EXISTS jobs`,
		`DROP TABLE IF EXISTS jobsets`,
		`CREATE TABLE jobsets (
			Queue TEXT,
			Id TEXT,
			Timestamp INT,
			ConnectionError TEXT,
			FromMessageId TEXT,
			UNIQUE(Queue,Id),
			PRIMARY KEY(Id)
		)`,
		`CREATE INDEX idx_jobsets_timestamp ON jobsets (Timestamp)`,
		`CREATE TABLE jobs (
			Queue TEXT,
			JobSetId TEXT,
			Id TEXT,
			JobResponseState TEXT,
			JobResponseError TEXT,
			Timestamp INT,
			PRIMARY KEY(Id),
			FOREIGN KEY(JobSetId) REFERENCES jobsets(Id) ON DELETE CASCADE
		)`,
		`CREATE INDEX idx_job_set_queue ON jobs (Queue, JobSetId)`,
		`CREATE INDEX idx_jobs_timestamp ON jobs (Timestamp)`,
		`DROP TRIGGER IF EXISTS trigger_delete_expired_jobsets`,
	}

	for _, stmt := range setupStmts {
		_, err := s.db.Exec(stmt)
		if err != nil {
			panic(err)
		}
	}
}

// Get the JobStatus given the jodId
func (s *JSRepoSQLite) GetJobStatus(ctx context.Context, jobId string) (*js.JobServiceResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var queue, jobSetId, jobState, jobError string
	sqlStmt := "SELECT Queue, JobSetId, JobResponseState, JobResponseError FROM jobs WHERE Id = ?"

	row := s.db.QueryRow(sqlStmt, jobId)
	err := row.Scan(&queue, &jobSetId, &jobState, &jobError)

	if err == sql.ErrNoRows {
		return &js.JobServiceResponse{State: js.JobServiceResponse_JOB_ID_NOT_FOUND}, nil
	} else if err != nil {
		return nil, err
	}

	// indicate connnection error for jobset/queue subscription where present
	connErr, err := s.GetSubscriptionError(ctx, queue, jobSetId)
	if err != nil {
		return nil, err
	}
	if connErr != "" {
		return &js.JobServiceResponse{
			Error: connErr,
			State: js.JobServiceResponse_CONNECTION_ERR,
		}, nil
	}

	jobJSRState, err := JobStateStrToJSRState(jobState)
	if err != nil {
		return nil, err
	}

	return &js.JobServiceResponse{Error: jobError, State: jobJSRState}, nil
}

// Update database with JobTable.
func (s *JSRepoSQLite) UpdateJobServiceDb(ctx context.Context, jobTable *JobStatus) error {
	// SQLite only allows one write at a time. Therefore we must serialize
	// writes in order to avoid SQL_BUSY errors.
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "INSERT OR REPLACE INTO jobs VALUES (?, ?, ?, ?, ?, ?)"
	stmt, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, errExec := stmt.Exec(jobTable.queue, jobTable.jobSetId, jobTable.jobId,
		jobTable.jobResponse.State.String(), jobTable.jobResponse.Error, jobTable.timeStamp)
	return errExec
}

// We should check if a JobSet exists first before updating the database and return an error if it doesn't exist.
// However, The only caller of this function, in jobservice/server/server.go, does this check before calling.
// Adding the check here will be redundant and a performance botteneck.
// TODO: We should descend the check here and adjust the JobSet subscription logic in jobservice/server/server.go
func (s *JSRepoSQLite) UpdateJobSetDb(ctx context.Context, queue string, jobSet string, fromMessageId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?, ?)"

	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	_, jobSetErr := jobSetState.Exec(queue, jobSet, time.Now().Unix(), "", &fromMessageId)
	if jobSetErr != nil {
		return jobSetErr
	}
	return nil
}

func (s *JSRepoSQLite) HealthCheck(ctx context.Context) (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	row := s.db.QueryRow("SELECT 1")
	var col int
	err := row.Scan(&col)
	if err == nil {
		return true, nil
	} else {
		return false, fmt.Errorf("SQL health check failed: %v", err)
	}
}

// Check if JobSet is in our map.
func (s *JSRepoSQLite) IsJobSetSubscribed(ctx context.Context, queue string, jobSet string) (bool, string, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "SELECT Queue, Id, FromMessageId FROM jobsets WHERE Queue = ? AND Id = ?"
	row := s.db.QueryRow(sqlStmt, queue, jobSet)
	var queueScan, jobSetIdScan, fromMessageId string

	err := row.Scan(&queueScan, &jobSetIdScan, &fromMessageId)

	if err == sql.ErrNoRows {
		return false, "", nil
	} else if err != nil {
		return false, "", err
	}
	return true, fromMessageId, nil
}

// Clear subscription error if present
func (s *JSRepoSQLite) AddMessageIdAndClearSubscriptionError(ctx context.Context, queue string,
	jobSet string, fromMessageId string,
) error {
	return s.SetSubscriptionError(ctx, queue, jobSet, "", fromMessageId)
}

// Set subscription error if present
func (s *JSRepoSQLite) SetSubscriptionError(ctx context.Context, queue string, jobSet string,
	connErr string, fromMessageId string,
) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?, ?)"
	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()

	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, jobSet, subscribeTable.lastRequestTimeStamp,
		connErr, fromMessageId)
	if jobSetErr != nil {
		return jobSetErr
	}
	return jobSetErr
}

// Get subscription error if present
func (s *JSRepoSQLite) GetSubscriptionError(ctx context.Context, queue string, jobSet string) (string, error) {
	sqlStmt := "SELECT ConnectionError FROM jobsets WHERE Queue = ? AND Id = ?"
	var connError string

	row := s.db.QueryRow(sqlStmt, queue, jobSet)
	err := row.Scan(&connError)

	if err == sql.ErrNoRows {
		return "", nil
	} else if err != nil {
		return "", err
	}
	return connError, nil
}

// Mark our JobSet as being subscribed
// SubscribeTable contains Queue, JobSet and time when it was created.
func (s *JSRepoSQLite) SubscribeJobSet(ctx context.Context, queue string, jobSet string, fromMessageId string) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "INSERT OR REPLACE INTO jobsets VALUES(?, ?, ?, ?, ?)"

	jobSetState, err := s.db.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer jobSetState.Close()
	subscribeTable := NewSubscribeTable(queue, jobSet)
	_, jobSetErr := jobSetState.Exec(subscribeTable.queue, subscribeTable.jobSet,
		subscribeTable.lastRequestTimeStamp, "", fromMessageId)
	return jobSetErr
}

// Checks JobSet table to make determine if we should unsubscribe from JobSet
// configTimeWithoutUpdates is a configurable value that is read from the config
// We allow unsubscribing if the jobset hasn't been updated in configTime
// TODO implement this
func (s *JSRepoSQLite) CheckToUnSubscribe(ctx context.Context, queue string, jobSet string,
	configTimeWithoutUpdates int64,
) (bool, error) {
	jobSetFound, _, err := s.IsJobSetSubscribed(ctx, queue, jobSet)
	if err != nil {
		return false, nil
	}
	if !jobSetFound {
		return false, nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "SELECT Timestamp FROM jobsets WHERE Queue = ? AND Id = ?"
	row := s.db.QueryRow(sqlStmt, queue, jobSet)
	var timeStamp int

	timeErr := row.Scan(&timeStamp)

	if timeErr == sql.ErrNoRows {
		return false, nil
	} else if err != nil {
		return false, err
	}

	currentTime := time.Now().Unix()
	if (currentTime - configTimeWithoutUpdates) > int64(timeStamp) {
		return true, nil
	}
	return false, nil
}

// Deletes the corresponding jobset along with it's associated jobs due to
// the CASCADE DELETE constraint on the foreign-key relationship.
func (s *JSRepoSQLite) UnsubscribeJobSet(ctx context.Context, queue, jobSet string) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "DELETE FROM jobsets WHERE Queue = ? AND Id = ?"

	result, err := s.db.Exec(sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Delete Jobs in the database
func (s *JSRepoSQLite) DeleteJobsInJobSet(ctx context.Context, queue string, jobSet string) (int64, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	sqlStmt := "DELETE FROM jobs WHERE Queue = ? AND JobSetId = ?"

	result, err := s.db.Exec(sqlStmt, queue, jobSet)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *JSRepoSQLite) GetSubscribedJobSets(ctx context.Context) ([]SubscribedTuple, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	rows, err := s.db.Query("SELECT Queue, Id, FromMessageId FROM jobsets")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tuples []SubscribedTuple

	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var st SubscribedTuple
		if err := rows.Scan(&st.Queue, &st.JobSet, &st.FromMessageId); err != nil {
			return tuples, err
		}
		tuples = append(tuples, st)
	}
	if err = rows.Err(); err != nil {
		return tuples, err
	}
	return tuples, nil
}

// PurgeExpiredJobSets purges all expired JobSets from the database
// An expired JobSet is a JobSet that has not been updated within the specified PurgeJobSetTime period.
// All children Jobs of the expired JobSets will also be deleted by the Cascade deletion relationship.
// This function should be called from a dedicated goroutine.
func (s *JSRepoSQLite) PurgeExpiredJobSets(ctx context.Context) {
	sqlStmt := fmt.Sprintf(`DELETE FROM jobsets WHERE Timestamp < (UNIXEPOCH() - %d);`, s.jobServiceConfig.PurgeJobSetTime)
	ticker := time.NewTicker(time.Duration(s.jobServiceConfig.PurgeJobSetTime) * time.Second)
	log := log.WithField("JobService", "ExpiredJobSetsPurge")

	log.Info("Starting purge of expired jobsets")
	for range ticker.C {
		s.lock.Lock()
		result, err := s.db.Exec(sqlStmt)
		if err != nil {
			log.Error("error deleting expired jobsets: ", err)
		} else {
			count, err := result.RowsAffected()
			if err != nil {
				log.Error("error getting affected rows for expired jobsets delete operation: ", err)
			}
			log.Debugf("Deleted %d expired jobsets", count)
		}
		s.lock.Unlock()
	}
}
