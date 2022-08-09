package repository

import (
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

func TestConstructInMemoryDoesNotExist(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)
		r.UpdateJobServiceDb(jobTable)

		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestConstructInMemoryServiceFailed(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)

		r.UpdateJobServiceDb(jobTable)

		resp, err := r.GetJobStatus("job-id")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestConstructInMemoryServiceNoJob(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestIsJobSubscribed(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		resp := r.IsJobSetSubscribed("queue-1", "job-set-1")
		assert.False(t, resp)
		r.SubscribeJobSet("queue-1", "job-set-1")
		resp2 := r.IsJobSetSubscribed("queue-1", "job-set-1")
		assert.True(t, resp2)
		r.SubscribeJobSet("queue-1", "job-set-1")
	})
}

func TestSubscribeList(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		r.SubscribeJobSet("queue", "job-set-1")
		r.SubscribeJobSet("queue", "job-set-2")

		subscribeList := r.GetSubscribedJobSets()

		for _, val := range subscribeList {

			if val.Queue == "queue" && val.JobSet == "job-set-1" {
				assert.Equal(t, val.Queue, "queue")
				assert.Equal(t, val.JobSet, "job-set-1")
			} else if val.Queue == "queue" && val.JobSet == "job-set-2" {
				assert.Equal(t, val.Queue, "queue")
				assert.Equal(t, val.JobSet, "job-set-2")
			} else {
				assert.True(t, false)
			}
		}
	})
}

func TestCleanupJobSetAndJobsIfNonExist(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		rowsAffected, err := r.CleanupJobSetAndJobs("queue", "job-set-1")
		assert.False(t, r.IsJobSetSubscribed("queue", "job-set-1"))
		assert.Equal(t, rowsAffected, int64(0))
		assert.Nil(t, err)
	})
}
func TestCleanupJobSetAndJobsHappy(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		r.SubscribeJobSet("queue", "job-set-1")
		respHappy := r.IsJobSetSubscribed("queue", "job-set-1")
		assert.True(t, respHappy)
		rowsAffected, err := r.CleanupJobSetAndJobs("queue", "job-set-1")
		assert.False(t, r.IsJobSetSubscribed("queue", "job-set-1"))
		assert.Equal(t, rowsAffected, int64(0))
		assert.Nil(t, err)
	})
}

func TestDeleteJobsInJobSet(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)

		r.UpdateJobServiceDb(jobTable1)
		jobResponse1, _ := r.GetJobStatus("job-id")

		assert.Equal(t, jobResponse1, responseExpected1)

		r.SubscribeJobSet("test", "job-set-1")
		rows, err := r.DeleteJobsInJobSet("test", "job-set-1")
		assert.Equal(t, rows, int64(1))
		assert.Nil(t, err)
		jobResponseDelete1, _ := r.GetJobStatus("job-id")
		var responseDoesNotExist = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		assert.Equal(t, jobResponseDelete1, responseDoesNotExist)

		rowsEmpty, errEmpty := r.DeleteJobsInJobSet("test", "job-set-1")
		assert.Equal(t, rowsEmpty, int64(0))
		assert.Nil(t, errEmpty)

	})
}

func TestCheckToUnSubscribe(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)

		r.UpdateJobServiceDb(jobTable1)
		r.SubscribeJobSet("test", "job-set-1")
		assert.True(t, r.IsJobSetSubscribed("test", "job-set-1"))
		assert.False(t, r.CheckToUnSubscribe("test", "job-set-1", 100000))
		assert.True(t, r.CheckToUnSubscribe("test", "job-set-1", -1))
	})
}
func TestCheckToUnSubscribeWithoutSubscribing(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-2", "job-id-3", *responseExpected2)

		r.UpdateJobServiceDb(jobTable1)
		r.UpdateJobServiceDb(jobTable2)
		assert.False(t, r.IsJobSetSubscribed("test", "job-set-1"))
		assert.False(t, r.CheckToUnSubscribe("test", "job-set-1", 100000))

	})
}
func TestUpdateJobSetTime(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		r.SubscribeJobSet("test", "job-set-1")
		r.UpdateJobSetTime("test", "job-set-1")
		_, ok := r.jobSetSubscribe.subscribeMap["testjob-set-1"]
		assert.True(t, ok)
	})
}

func TestUpdateJobSetTimeWithoutSubscribe(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		updateErr := r.UpdateJobSetTime("test", "job-set-1")
		assert.EqualError(t, updateErr, "queue test jobSet job-set-1 is already unsubscribed")
		_, ok := r.jobSetSubscribe.subscribeMap["testjob-set-1"]
		assert.False(t, ok)

	})
}

func TestGetJobStatusAllStates(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseFailed = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseSuccess = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		var responseDuplicate = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_DUPLICATE_FOUND}
		var responseRunning = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}
		var responseSubmitted = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUBMITTED}
		var responseCancelled = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_CANCELLED}
		var responseDoesNotExist = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseFailed)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id-2", *responseSuccess)
		jobTable3 := NewJobTable("test", "job-set-1", "job-id-3", *responseDuplicate)
		jobTable4 := NewJobTable("test", "job-set-1", "job-id-4", *responseRunning)
		jobTable5 := NewJobTable("test", "job-set-1", "job-id-5", *responseSubmitted)
		jobTable6 := NewJobTable("test", "job-set-1", "job-id-6", *responseCancelled)
		jobTable7 := NewJobTable("test", "job-set-1", "job-id-7", *responseDoesNotExist)

		r.UpdateJobServiceDb(jobTable1)
		r.UpdateJobServiceDb(jobTable2)
		r.UpdateJobServiceDb(jobTable3)
		r.UpdateJobServiceDb(jobTable4)
		r.UpdateJobServiceDb(jobTable5)
		r.UpdateJobServiceDb(jobTable6)
		r.UpdateJobServiceDb(jobTable7)

		actualFailed, errFailed := r.GetJobStatus("job-id")
		actualSuccess, errSuccess := r.GetJobStatus("job-id-2")
		actualDuplicate, errDup := r.GetJobStatus("job-id-3")
		actualRunning, errRunning := r.GetJobStatus("job-id-4")
		actualSubmitted, errSubmitted := r.GetJobStatus("job-id-5")
		actualCancelled, errCancel := r.GetJobStatus("job-id-6")
		actualNotExist, errNotExist := r.GetJobStatus("job-id-7")

		assert.Nil(t, errFailed)
		assert.Equal(t, responseFailed, actualFailed)
		assert.Nil(t, errSuccess)
		assert.Equal(t, responseSuccess, actualSuccess)
		assert.Nil(t, errDup)
		assert.Equal(t, responseDuplicate, actualDuplicate)
		assert.Nil(t, errRunning)
		assert.Equal(t, responseRunning, actualRunning)
		assert.Nil(t, errSubmitted)
		assert.Equal(t, responseSubmitted, actualSubmitted)
		assert.Nil(t, errCancel)
		assert.Equal(t, responseCancelled, actualCancelled)
		assert.Nil(t, errNotExist)
		assert.Equal(t, responseDoesNotExist, actualNotExist)

	})
}

func TestDeleteJobsBeforePersistingRaceError(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseSuccess = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		noExist := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}

		jobTable1 := NewJobTable("test-race", "job-set-race", "job-race", *responseSuccess)
		r.UpdateJobServiceDb(jobTable1)
		r.SubscribeJobSet("test-race", "job-set-race")
		r.CleanupJobSetAndJobs("test-race", "job-set-race")
		actualSuccess, actualError := r.GetJobStatus("job-race")
		assert.Equal(t, actualSuccess, noExist)
		assert.Nil(t, actualError)
		sqlNoExist, sqlError := r.GetJobStatus("job-race")
		assert.Equal(t, sqlNoExist, noExist)
		assert.Nil(t, sqlError)
	})
}

func TestGetJobStatusAfterPersisting(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseSuccess = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseSuccess)
		r.UpdateJobServiceDb(jobTable1)
		actual, actualErr := r.GetJobStatus("job-id")
		assert.Nil(t, actualErr)
		assert.Equal(t, actual, responseSuccess)

	})
}

func TestDuplicateIdDatabaseInsert(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		var responseRunning = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}
		var responseSuccess = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseRunning)
		r.UpdateJobServiceDb(jobTable1)
		actualSql, actualErr := r.GetJobStatus("job-id")
		assert.Equal(t, actualSql, responseRunning)
		assert.Nil(t, actualErr)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id", *responseSuccess)
		r.UpdateJobServiceDb(jobTable2)
		actualSuccessSql, actualSuccessErr := r.GetJobStatus("job-id")
		assert.Equal(t, actualSuccessSql, responseSuccess)
		assert.Nil(t, actualSuccessErr)

	})
}

func TestHealthCheck(t *testing.T) {
	WithSqlServiceRepo(func(r *SQLJobService) {
		healthCheck, err := r.HealthCheck()
		assert.True(t, healthCheck)
		assert.Nil(t, err)
	})
}

func WithSqlServiceRepo(action func(r *SQLJobService)) {
	jobSet := make(map[string]*SubscribeTable)
	config := &configuration.JobServiceConfiguration{}
	jobStatusMap := NewJobSetSubscriptions(jobSet)
	db, err := sql.Open("sqlite", "test.db")
	if err != nil {
		panic(err)
	}
	repo := NewSQLJobService(jobStatusMap, config, db)
	repo.CreateTable()
	action(repo)
	db.Close()
	os.Remove("test.db")
}
