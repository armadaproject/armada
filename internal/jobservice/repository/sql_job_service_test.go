package repository

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/database"
	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/pkg/api/jobservice"
)

func TestConstructInMemoryDoesNotExist(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseExpected := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		jobStatus := NewJobStatus("test", "job-set-1", "job-id", *responseExpected)
		err := r.UpdateJobServiceDb(jobStatus)
		require.NoError(t, err)

		resp, err := r.GetJobStatus("job-set-1")
		assert.NoError(t, err)
		assert.Equal(t, resp, responseExpected)
	})
}

func TestSubscriptionError(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		err := r.SubscribeJobSet("queue-1", "job-set-1", "")
		require.NoError(t, err)
		err = r.SetSubscriptionError("queue-1", "job-set-1", "conn-error", "test")
		require.NoError(t, err)
		conErr, subErr := r.GetSubscriptionError("queue-1", "job-set-1")
		require.NoError(t, subErr)
		assert.Equal(t, conErr, "conn-error")
	})
}

func TestUpdateJobSetDb(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		err := r.SubscribeJobSet("test", "job-set-1", "test")
		require.NoError(t, err)
		err = r.UpdateJobSetDb("test", "job-set-1", "test")
		require.NoError(t, err)
	})
}

func TestConstructInMemoryServiceFailed(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseExpected := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		jobStatus := NewJobStatus("test", "job-set-1", "job-id", *responseExpected)

		err := r.UpdateJobServiceDb(jobStatus)
		require.NoError(t, err)

		resp, err := r.GetJobStatus("job-id")
		require.NoError(t, err)
		require.Equal(t, resp, responseExpected)
	})
}

func TestConstructInMemoryServiceNoJob(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseExpected := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		resp, err := r.GetJobStatus("job-set-1")
		require.NoError(t, err)
		require.Equal(t, resp, responseExpected)
	})
}

func TestIsJobSubscribed(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		resp, _, err := r.IsJobSetSubscribed("queue-1", "job-set-1")
		require.NoError(t, err)
		require.False(t, resp)
		err = r.SubscribeJobSet("queue-1", "job-set-1", "")
		require.NoError(t, err)
		resp2, _, err := r.IsJobSetSubscribed("queue-1", "job-set-1")
		require.NoError(t, err)
		require.True(t, resp2)
		err = r.SubscribeJobSet("queue-1", "job-set-1", "")
		require.NoError(t, err)
	})
}

func TestSubscribeList(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		err := r.SubscribeJobSet("queue", "job-set-1", "")
		require.NoError(t, err)
		err = r.SubscribeJobSet("queue", "job-set-2", "")
		require.NoError(t, err)

		subscribeList, err := r.GetSubscribedJobSets()
		require.NoError(t, err)
		for _, val := range subscribeList {
			if val.Queue == "queue" && val.JobSet == "job-set-1" {
				require.Equal(t, val.Queue, "queue")
				require.Equal(t, val.JobSet, "job-set-1")
			} else if val.Queue == "queue" && val.JobSet == "job-set-2" {
				require.Equal(t, val.Queue, "queue")
				require.Equal(t, val.JobSet, "job-set-2")
			} else {
				require.True(t, false)
			}
		}
	})
}

func TestCleanupJobSetAndJobsIfNonExist(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		rowsAffected, err := r.CleanupJobSetAndJobs("queue", "job-set-1")
		require.NoError(t, err)
		subscribe, _, err := r.IsJobSetSubscribed("queue", "job-set-1")
		require.NoError(t, err)
		require.False(t, subscribe)
		require.Equal(t, rowsAffected, int64(0))
		require.NoError(t, err)
	})
}

func TestCleanupJobSetAndJobsHappy(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		err := r.SubscribeJobSet("queue", "job-set-1", "")
		require.NoError(t, err)
		respHappy, _, _ := r.IsJobSetSubscribed("queue", "job-set-1")
		require.True(t, respHappy)
		rowsAffected, err := r.CleanupJobSetAndJobs("queue", "job-set-1")
		subscribe, _, _ := r.IsJobSetSubscribed("queue", "job-set-1")
		require.False(t, subscribe)
		require.Equal(t, rowsAffected, int64(0))
		require.NoError(t, err)
	})
}

func TestDeleteJobsInJobSet(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseExpected1 := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseExpected1)

		err := r.UpdateJobServiceDb(jobStatus1)
		require.NoError(t, err)

		jobResponse1, _ := r.GetJobStatus("job-id")
		require.Equal(t, jobResponse1, responseExpected1)

		err = r.SubscribeJobSet("test", "job-set-1", "")
		require.NoError(t, err)
		rows, err := r.DeleteJobsInJobSet("test", "job-set-1")
		require.Equal(t, rows, int64(1))
		require.NoError(t, err)
		jobResponseDelete1, _ := r.GetJobStatus("job-id")
		responseDoesNotExist := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		require.Equal(t, jobResponseDelete1, responseDoesNotExist)

		rowsEmpty, errEmpty := r.DeleteJobsInJobSet("test", "job-set-1")
		require.Equal(t, rowsEmpty, int64(0))
		require.NoError(t, errEmpty)
	})
}

func TestCheckToUnSubscribe(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseExpected1 := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseExpected1)

		err := r.UpdateJobServiceDb(jobStatus1)
		require.NoError(t, err)

		err = r.SubscribeJobSet("test", "job-set-1", "")
		require.NoError(t, err)
		subscribe, _, err := r.IsJobSetSubscribed("test", "job-set-1")
		require.NoError(t, err)
		assert.True(t, subscribe)
		flag, errTrue := r.CheckToUnSubscribe("test", "job-set-1", 100000)
		require.NoError(t, errTrue)
		flagFalse, errFalse := r.CheckToUnSubscribe("test", "job-set-1", -1)
		require.NoError(t, errFalse)
		assert.False(t, flag)
		assert.True(t, flagFalse)
	})
}

func TestCheckToUnSubscribeWithoutSubscribing(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseExpected1 := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		responseExpected2 := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseExpected1)
		jobStatus2 := NewJobStatus("test", "job-set-2", "job-id-3", *responseExpected2)

		err := r.UpdateJobServiceDb(jobStatus1)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(jobStatus2)
		assert.NoError(t, err)

		subscribe, _, err := r.IsJobSetSubscribed("test", "job-set-1")
		require.NoError(t, err)
		assert.False(t, subscribe)
		flag, err := r.CheckToUnSubscribe("test", "job-set-1", 100000)
		require.NoError(t, err)
		assert.False(t, flag)
	})
}

func TestUnsubscribe(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		err := r.SubscribeJobSet("test", "testjobset", "")
		require.NoError(t, err)
		numberOfJobSets, err := r.UnsubscribeJobSet("test", "testjobset")
		require.NoError(t, err)
		assert.Equal(t, numberOfJobSets, int64(1))
		subscribe, _, err := r.IsJobSetSubscribed("test", "testjobset")
		require.NoError(t, err)
		assert.False(t, subscribe)
	})
}

func TestUpdateJobSetTime(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		err := r.SubscribeJobSet("test", "job-set-1", "")
		require.NoError(t, err)
		err = r.UpdateJobSetDb("test", "job-set-1", "")
		require.NoError(t, err)
	})
}

func TestUpdateJobSetTimeWithoutSubscribe(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		updateErr := r.UpdateJobSetDb("test", "job-set-1", "")
		assert.EqualError(t, updateErr, "queue test jobSet job-set-1 is already unsubscribed")
	})
}

func TestGetJobStatusAllStates(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseFailed := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		responseDuplicate := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND}
		responseRunning := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}
		responseSubmitted := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUBMITTED}
		responseCancelled := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED}
		responseDoesNotExist := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseFailed)
		jobStatus2 := NewJobStatus("test", "job-set-1", "job-id-2", *responseSuccess)
		jobStatus3 := NewJobStatus("test", "job-set-1", "job-id-3", *responseDuplicate)
		jobStatus4 := NewJobStatus("test", "job-set-1", "job-id-4", *responseRunning)
		jobStatus5 := NewJobStatus("test", "job-set-1", "job-id-5", *responseSubmitted)
		jobStatus6 := NewJobStatus("test", "job-set-1", "job-id-6", *responseCancelled)
		jobStatus7 := NewJobStatus("test", "job-set-1", "job-id-7", *responseDoesNotExist)

		err := r.UpdateJobServiceDb(jobStatus1)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(jobStatus2)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(jobStatus3)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(jobStatus4)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(jobStatus5)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(jobStatus6)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(jobStatus7)
		require.NoError(t, err)

		actualFailed, errFailed := r.GetJobStatus("job-id")
		actualSuccess, errSuccess := r.GetJobStatus("job-id-2")
		actualDuplicate, errDup := r.GetJobStatus("job-id-3")
		actualRunning, errRunning := r.GetJobStatus("job-id-4")
		actualSubmitted, errSubmitted := r.GetJobStatus("job-id-5")
		actualCancelled, errCancel := r.GetJobStatus("job-id-6")
		actualNotExist, errNotExist := r.GetJobStatus("job-id-7")

		require.NoError(t, errFailed)
		require.Equal(t, responseFailed, actualFailed)
		require.NoError(t, errSuccess)
		require.Equal(t, responseSuccess, actualSuccess)
		require.NoError(t, errDup)
		require.Equal(t, responseDuplicate, actualDuplicate)
		require.NoError(t, errRunning)
		require.Equal(t, responseRunning, actualRunning)
		require.NoError(t, errSubmitted)
		require.Equal(t, responseSubmitted, actualSubmitted)
		require.NoError(t, errCancel)
		require.Equal(t, responseCancelled, actualCancelled)
		require.NoError(t, errNotExist)
		require.Equal(t, responseDoesNotExist, actualNotExist)
	})
}

func TestDeleteJobsBeforePersistingRaceError(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		noExist := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		var expectedNumberOfJobs int64 = 1
		jobStatus1 := NewJobStatus("test-race", "job-set-race", "job-race", *responseSuccess)
		err := r.UpdateJobServiceDb(jobStatus1)
		require.NoError(t, err)
		err = r.SubscribeJobSet("test-race", "job-set-race", "")
		require.NoError(t, err)
		numberOfJobs, deleteErr := r.CleanupJobSetAndJobs("test-race", "job-set-race")
		assert.Equal(t, expectedNumberOfJobs, numberOfJobs)
		require.NoError(t, deleteErr)
		actualSuccess, actualError := r.GetJobStatus("job-race")
		assert.Equal(t, actualSuccess, noExist)
		require.NoError(t, actualError)
		sqlNoExist, sqlError := r.GetJobStatus("job-race")
		assert.Equal(t, sqlNoExist, noExist)
		require.NoError(t, sqlError)
	})
}

func TestGetJobStatusAfterPersisting(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseSuccess)
		err := r.UpdateJobServiceDb(jobStatus1)
		require.NoError(t, err)
		actual, actualErr := r.GetJobStatus("job-id")
		assert.Nil(t, actualErr)
		assert.Equal(t, actual, responseSuccess)
	})
}

func TestDuplicateIdDatabaseInsert(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseRunning := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseRunning)
		err := r.UpdateJobServiceDb(jobStatus1)
		require.NoError(t, err)
		actualSql, actualErr := r.GetJobStatus("job-id")
		assert.Equal(t, actualSql, responseRunning)
		require.NoError(t, actualErr)
		jobStatus2 := NewJobStatus("test", "job-set-1", "job-id", *responseSuccess)
		err = r.UpdateJobServiceDb(jobStatus2)
		require.NoError(t, err)
		actualSuccessSql, actualSuccessErr := r.GetJobStatus("job-id")
		assert.Equal(t, actualSuccessSql, responseSuccess)
		require.NoError(t, actualSuccessErr)
	})
}

func TestHealthCheck(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		healthCheck, err := r.HealthCheck()
		assert.True(t, healthCheck)
		require.NoError(t, err)
	})
}

// This test will fail if sqlite writes are not serialised somehow due to
// SQLITE_BUSY errors.
func TestConcurrentJobStatusUpdating(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		responseRunning := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}

		concurrency := 10
		wg := sync.WaitGroup{}
		wg.Add(concurrency)

		startWg := sync.WaitGroup{}
		startWg.Add(1)

		for i := 0; i < concurrency; i++ {
			go func(num int) {
				defer wg.Done()

				jobId := fmt.Sprintf("job-id-%d", num)
				jobStatus := NewJobStatus("test", "job-set-1", jobId, *responseRunning)

				startWg.Wait()
				err := r.UpdateJobServiceDb(jobStatus)
				assert.Nil(t, err)
				actualSql, actualErr := r.GetJobStatus(jobId)
				assert.Equal(t, actualSql, responseRunning)
				assert.Nil(t, actualErr)
			}(i)
		}

		startWg.Done()
		wg.Wait()
	})
}

func WithSqlServiceRepo(action func(r *SQLJobService)) {
	var db *sql.DB
	var err error
	config := &configuration.JobServiceConfiguration{}

	// If JSDBTYPE is not specified in the environment, default to 'sqlite'
	jsDatabase := "sqlite"

	if os.Getenv("JSDBTYPE") == "postgres" {
		jsDatabase = "postgres"
	}

	if jsDatabase == "sqlite" {
		config.DatabaseType = "sqlite"

		db, err = sql.Open("sqlite", "test.db")
		if err != nil {
			panic(err)
		}
	} else if jsDatabase == "postgres" {
		config.DatabaseType = "postgres"
		config.PostgresConfig = configuration.PostgresConfig{
			MaxOpenConns:    20,
			MaxIdleConns:    5,
			ConnMaxLifetime: 30 * time.Second,
			Connection: map[string]string{
				"host":     "localhost",
				"port":     "5432",
				"user":     "postgres",
				"password": "psw",
				"dbname":   "postgres",
				"sslmode":  "disable",
			},
		}

		db, err = sql.Open("pgx", database.CreateConnectionString(config.PostgresConfig.Connection))
		if err != nil {
			panic(err)
		}
		db.SetMaxOpenConns(config.PostgresConfig.MaxOpenConns)
		db.SetMaxIdleConns(config.PostgresConfig.MaxIdleConns)
		db.SetConnMaxLifetime(config.PostgresConfig.ConnMaxLifetime)
	}

	repo := NewSQLJobService(config, db)
	repo.Setup()
	action(repo)
	db.Close()

	if config.DatabaseType == "sqlite" {
		os.Remove("test.db")
	}
}
