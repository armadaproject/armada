package repository

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/jobservice/configuration"
	"github.com/armadaproject/armada/pkg/api/jobservice"
)

const purgeTime = 0

func TestConstructInMemoryDoesNotExist(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseExpected := &jobservice.JobServiceResponse{
			State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND,
		}
		err := r.SubscribeJobSet(ctx, "test", "job-set-1", "test")
		require.NoError(t, err)
		jobStatus := NewJobStatus("test", "job-set-1", "job-id", *responseExpected)
		err = r.UpdateJobServiceDb(ctx, jobStatus)
		require.NoError(t, err)

		resp, err := r.GetJobStatus(ctx, "job-set-1")
		assert.NoError(t, err)
		assert.Equal(t, resp, responseExpected)
	})
}

func TestSubscriptionError(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		err := r.SubscribeJobSet(ctx, "queue-1", "job-set-1", "")
		require.NoError(t, err)
		err = r.SetSubscriptionError(ctx, "queue-1", "job-set-1", "conn-error", "test")
		require.NoError(t, err)
		conErr, subErr := r.GetSubscriptionError(ctx, "queue-1", "job-set-1")
		require.NoError(t, subErr)
		assert.Equal(t, conErr, "conn-error")
	})
}

func TestUpdateJobSetDb(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		err := r.SubscribeJobSet(ctx, "test", "job-set-1", "test")
		require.NoError(t, err)
		err = r.UpdateJobSetDb(ctx, "test", "job-set-1", "test")
		require.NoError(t, err)
	})
}

func TestConstructInMemoryServiceFailed(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseExpected := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}

		err := r.SubscribeJobSet(ctx, "test", "job-set-1", "test")
		require.NoError(t, err)

		jobStatus := NewJobStatus("test", "job-set-1", "job-id", *responseExpected)

		err = r.UpdateJobServiceDb(ctx, jobStatus)
		require.NoError(t, err)

		resp, err := r.GetJobStatus(ctx, "job-id")
		require.NoError(t, err)
		require.Equal(t, resp, responseExpected)
	})
}

func TestConstructInMemoryServiceNoJob(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseExpected := &jobservice.JobServiceResponse{
			State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND,
		}
		resp, err := r.GetJobStatus(ctx, "job-set-1")
		require.NoError(t, err)
		require.Equal(t, resp, responseExpected)
	})
}

func TestIsJobSubscribed(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		resp, _, err := r.IsJobSetSubscribed(ctx, "queue-1", "job-set-1")
		require.NoError(t, err)
		require.False(t, resp)
		err = r.SubscribeJobSet(ctx, "queue-1", "job-set-1", "")
		require.NoError(t, err)
		resp2, _, err := r.IsJobSetSubscribed(ctx, "queue-1", "job-set-1")
		require.NoError(t, err)
		require.True(t, resp2)
	})
}

func TestSubscribeList(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		err := r.SubscribeJobSet(ctx, "queue", "job-set-1", "")
		require.NoError(t, err)
		err = r.SubscribeJobSet(ctx, "queue", "job-set-2", "")
		require.NoError(t, err)

		subscribeList, err := r.GetSubscribedJobSets(ctx)
		require.NoError(t, err)
		for _, val := range subscribeList {
			if val.Queue == "queue" && val.JobSetId == "job-set-1" {
				require.Equal(t, val.Queue, "queue")
				require.Equal(t, val.JobSetId, "job-set-1")
			} else if val.Queue == "queue" && val.JobSetId == "job-set-2" {
				require.Equal(t, val.Queue, "queue")
				require.Equal(t, val.JobSetId, "job-set-2")
			} else {
				require.True(t, false)
			}
		}
	})
}

func TestCleanupJobSetAndJobsIfNonExist(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		rowsAffected, err := r.UnsubscribeJobSet(ctx, "queue", "job-set-1")
		require.NoError(t, err)
		subscribe, _, err := r.IsJobSetSubscribed(ctx, "queue", "job-set-1")
		require.NoError(t, err)
		require.False(t, subscribe)
		require.Equal(t, rowsAffected, int64(0))
		require.NoError(t, err)
	})
}

func TestPurgeExpiredJobSets(t *testing.T) {
	realPurgeTime := int64(1)
	WithSqlServiceRepo(realPurgeTime, func(r SQLJobService) {
		ctx := context.Background()
		go r.PurgeExpiredJobSets(ctx)
		// test multiple iterations of the purging goroutine
		for i := 1; i <= 3; i++ {
			jobsetId := fmt.Sprintf("job-set-%d", i)
			jobId := fmt.Sprintf("job-%d", i)
			responseExpected1 := &jobservice.JobServiceResponse{
				State: jobservice.JobServiceResponse_RUNNING,
			}
			err := r.SubscribeJobSet(ctx, "queue", jobsetId, "")
			require.NoError(t, err)
			jobStatus1 := NewJobStatus("test", jobsetId, jobId, *responseExpected1)
			err = r.UpdateJobServiceDb(ctx, jobStatus1)
			require.NoError(t, err)
			subscribe, _, err := r.IsJobSetSubscribed(ctx, "queue", jobsetId)
			require.True(t, subscribe)
			require.NoError(t, err)
			response, err := r.GetJobStatus(ctx, jobId)
			require.Equal(t, responseExpected1, response)
			require.NoError(t, err)

			// wait until the purge ticker starts a new cycle
			time.Sleep(time.Duration(realPurgeTime+1) * time.Second)

			subscribe, _, err = r.IsJobSetSubscribed(ctx, "queue", jobsetId)
			require.False(t, subscribe)
			require.NoError(t, err)
			responseExpected2 := &jobservice.JobServiceResponse{
				State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND,
			}
			response, err = r.GetJobStatus(ctx, jobId)
			require.Equal(t, responseExpected2, response)
			require.NoError(t, err)
		}
	})
}

func TestCleanupJobSetAndJobsHappy(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		err := r.SubscribeJobSet(ctx, "queue", "job-set-1", "")
		require.NoError(t, err)
		respHappy, _, _ := r.IsJobSetSubscribed(ctx, "queue", "job-set-1")
		require.True(t, respHappy)
		rowsAffected, err := r.UnsubscribeJobSet(ctx, "queue", "job-set-1")
		subscribe, _, _ := r.IsJobSetSubscribed(ctx, "queue", "job-set-1")
		require.False(t, subscribe)
		require.Equal(t, rowsAffected, int64(1))
		require.NoError(t, err)
	})
}

func TestDeleteJobsInJobSet(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseExpected1 := &jobservice.JobServiceResponse{
			State: jobservice.JobServiceResponse_FAILED, Error: "TestFail",
		}

		err := r.SubscribeJobSet(ctx, "test", "job-set-1", "test")
		require.NoError(t, err)

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseExpected1)

		err = r.UpdateJobServiceDb(ctx, jobStatus1)
		require.NoError(t, err)

		jobResponse1, _ := r.GetJobStatus(ctx, "job-id")
		require.Equal(t, jobResponse1, responseExpected1)

		rows, err := r.DeleteJobsInJobSet(ctx, "test", "job-set-1")
		require.Equal(t, rows, int64(1))
		require.NoError(t, err)
		jobResponseDelete1, _ := r.GetJobStatus(ctx, "job-id")
		responseDoesNotExist := &jobservice.JobServiceResponse{
			State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND,
		}
		require.Equal(t, jobResponseDelete1, responseDoesNotExist)

		rowsEmpty, errEmpty := r.DeleteJobsInJobSet(ctx, "test", "job-set-1")
		require.Equal(t, rowsEmpty, int64(0))
		require.NoError(t, errEmpty)
	})
}

func TestCheckToUnSubscribe(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseExpected1 := &jobservice.JobServiceResponse{
			State: jobservice.JobServiceResponse_FAILED, Error: "TestFail",
		}

		err := r.SubscribeJobSet(ctx, "test", "job-set-1", "")
		require.NoError(t, err)

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseExpected1)

		err = r.UpdateJobServiceDb(ctx, jobStatus1)
		require.NoError(t, err)

		subscribe, _, err := r.IsJobSetSubscribed(ctx, "test", "job-set-1")
		require.NoError(t, err)
		assert.True(t, subscribe)
		flag, errTrue := r.CheckToUnSubscribe(ctx, "test", "job-set-1", time.Second*time.Duration(10000))
		require.NoError(t, errTrue)
		assert.False(t, flag)
	})
}

func TestUnsubscribe(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		err := r.SubscribeJobSet(ctx, "test", "testjobset", "")
		require.NoError(t, err)
		numberOfJobSets, err := r.UnsubscribeJobSet(ctx, "test", "testjobset")
		require.NoError(t, err)
		assert.Equal(t, numberOfJobSets, int64(1))
		subscribe, _, err := r.IsJobSetSubscribed(ctx, "test", "testjobset")
		require.NoError(t, err)
		assert.False(t, subscribe)
	})
}

func TestUpdateJobSetTime(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		err := r.SubscribeJobSet(ctx, "test", "job-set-1", "")
		require.NoError(t, err)
		err = r.UpdateJobSetDb(ctx, "test", "job-set-1", "")
		require.NoError(t, err)
	})
}

func TestGetJobStatusAllStates(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseFailed := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		responseDuplicate := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND}
		responseRunning := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}
		responseSubmitted := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUBMITTED}
		responseCancelled := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED}
		responseDoesNotExist := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}

		err := r.SubscribeJobSet(ctx, "test", "job-set-1", "test")
		require.NoError(t, err)

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseFailed)
		jobStatus2 := NewJobStatus("test", "job-set-1", "job-id-2", *responseSuccess)
		jobStatus3 := NewJobStatus("test", "job-set-1", "job-id-3", *responseDuplicate)
		jobStatus4 := NewJobStatus("test", "job-set-1", "job-id-4", *responseRunning)
		jobStatus5 := NewJobStatus("test", "job-set-1", "job-id-5", *responseSubmitted)
		jobStatus6 := NewJobStatus("test", "job-set-1", "job-id-6", *responseCancelled)
		jobStatus7 := NewJobStatus("test", "job-set-1", "job-id-7", *responseDoesNotExist)

		err = r.UpdateJobServiceDb(ctx, jobStatus1)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(ctx, jobStatus2)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(ctx, jobStatus3)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(ctx, jobStatus4)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(ctx, jobStatus5)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(ctx, jobStatus6)
		require.NoError(t, err)
		err = r.UpdateJobServiceDb(ctx, jobStatus7)
		require.NoError(t, err)

		actualFailed, errFailed := r.GetJobStatus(ctx, "job-id")
		actualSuccess, errSuccess := r.GetJobStatus(ctx, "job-id-2")
		actualDuplicate, errDup := r.GetJobStatus(ctx, "job-id-3")
		actualRunning, errRunning := r.GetJobStatus(ctx, "job-id-4")
		actualSubmitted, errSubmitted := r.GetJobStatus(ctx, "job-id-5")
		actualCancelled, errCancel := r.GetJobStatus(ctx, "job-id-6")
		actualNotExist, errNotExist := r.GetJobStatus(ctx, "job-id-7")

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
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		noExist := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		var expectedNumberOfJobs int64 = 1
		err := r.SubscribeJobSet(ctx, "test-race", "job-set-race", "")
		require.NoError(t, err)
		jobStatus1 := NewJobStatus("test-race", "job-set-race", "job-race", *responseSuccess)
		err = r.UpdateJobServiceDb(ctx, jobStatus1)
		require.NoError(t, err)
		numberOfJobs, deleteErr := r.DeleteJobsInJobSet(ctx, "test-race", "job-set-race")
		assert.Equal(t, expectedNumberOfJobs, numberOfJobs)
		require.NoError(t, deleteErr)
		actualSuccess, actualError := r.GetJobStatus(ctx, "job-race")
		assert.Equal(t, actualSuccess, noExist)
		require.NoError(t, actualError)
		sqlNoExist, sqlError := r.GetJobStatus(ctx, "job-race")
		assert.Equal(t, sqlNoExist, noExist)
		require.NoError(t, sqlError)
	})
}

func TestGetJobStatusAfterPersisting(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		err := r.SubscribeJobSet(ctx, "queue", "job-set-1", "")
		require.NoError(t, err)

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseSuccess)
		err = r.UpdateJobServiceDb(ctx, jobStatus1)
		require.NoError(t, err)
		actual, actualErr := r.GetJobStatus(ctx, "job-id")
		assert.Nil(t, actualErr)
		assert.Equal(t, actual, responseSuccess)
	})
}

func TestDuplicateIdDatabaseInsert(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseRunning := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}
		responseSuccess := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		err := r.SubscribeJobSet(ctx, "queue", "job-set-1", "")
		require.NoError(t, err)

		jobStatus1 := NewJobStatus("test", "job-set-1", "job-id", *responseRunning)
		err = r.UpdateJobServiceDb(ctx, jobStatus1)
		require.NoError(t, err)
		actualSql, actualErr := r.GetJobStatus(ctx, "job-id")
		assert.Equal(t, actualSql, responseRunning)
		require.NoError(t, actualErr)
		jobStatus2 := NewJobStatus("test", "job-set-1", "job-id", *responseSuccess)
		err = r.UpdateJobServiceDb(ctx, jobStatus2)
		require.NoError(t, err)
		actualSuccessSql, actualSuccessErr := r.GetJobStatus(ctx, "job-id")
		assert.Equal(t, actualSuccessSql, responseSuccess)
		require.NoError(t, actualSuccessErr)
	})
}

func TestHealthCheck(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		healthCheck, err := r.HealthCheck(context.Background())
		assert.True(t, healthCheck)
		require.NoError(t, err)
	})
}

// This test will fail if sqlite writes are not serialised somehow due to
// SQLITE_BUSY errors.
func TestConcurrentJobStatusUpdating(t *testing.T) {
	WithSqlServiceRepo(purgeTime, func(r SQLJobService) {
		ctx := context.Background()
		responseRunning := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}

		concurrency := 10
		wg := sync.WaitGroup{}
		wg.Add(concurrency)

		startWg := sync.WaitGroup{}
		startWg.Add(1)

		err := r.SubscribeJobSet(ctx, "queue", "job-set-1", "")
		require.NoError(t, err)

		for i := 0; i < concurrency; i++ {
			go func(num int) {
				defer wg.Done()

				jobId := fmt.Sprintf("job-id-%d", num)
				jobStatus := NewJobStatus("test", "job-set-1", jobId, *responseRunning)

				startWg.Wait()
				err := r.UpdateJobServiceDb(ctx, jobStatus)
				assert.Nil(t, err)
				actualSql, actualErr := r.GetJobStatus(ctx, jobId)
				assert.Equal(t, actualSql, responseRunning)
				assert.Nil(t, actualErr)
			}(i)
		}

		startWg.Done()
		wg.Wait()
	})
}

func WithSqlServiceRepo(purgeTime int64, action func(r SQLJobService)) {
	var repo SQLJobService
	config := &configuration.JobServiceConfiguration{
		PurgeJobSetTime: purgeTime,
	}
	log := log.WithField("JobService", "Startup")

	if os.Getenv("JSDBTYPE") == "sqlite" {
		config.DatabaseType = "sqlite"
		config.DatabasePath = "test.db"
	} else if os.Getenv("JSDBTYPE") == "postgres" {
		config.DatabaseType = "postgres"
		config.PostgresConfig = configuration.PostgresConfig{
			PoolMaxOpenConns:    20,
			PoolMaxIdleConns:    5,
			PoolMaxConnLifetime: 30 * time.Second,
			Connection: map[string]string{
				"host":     "localhost",
				"port":     "5432",
				"user":     "postgres",
				"password": "psw",
				"dbname":   "postgres",
				"sslmode":  "disable",
			},
		}
	}

	err, repo, dbCallbackFn := NewSQLJobService(config, log)
	if err != nil {
		panic(err)
	}
	defer dbCallbackFn()

	repo.Setup(context.Background())
	action(repo)

	if config.DatabaseType == "sqlite" {
		// Besides the base sqlite storage file (e.g. "test.db"), there
		// are also two others to be removed ("test.db-shm", "test.db-wal")
		for _, suffix := range []string{"", "-shm", "-wal"} {
			os.Remove(config.DatabasePath + suffix)
		}
	}
}
