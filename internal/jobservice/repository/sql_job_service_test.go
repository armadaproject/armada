package repository

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

func TestConstructInMemoryDoesNotExist(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)
		r.UpdateJobServiceDb(jobTable)

		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestConstructInMemoryServiceFailed(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)

		r.UpdateJobServiceDb(jobTable)

		resp, err := r.GetJobStatus("job-id")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestConstructInMemoryServiceNoJob(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestIsJobSubscribed(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		resp := r.IsJobSetSubscribed("job-set-1")
		assert.False(t, resp)
		r.SubscribeJobSet("job-set-1")
		resp2 := r.IsJobSetSubscribed("job-set-1")
		assert.True(t, resp2)
		r.SubscribeJobSet("job-set-1")
	})
}

func TestSubscribeList(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		r.SubscribeJobSet("job-set-1")
		r.SubscribeJobSet("job-set-2")

		subscribeList := r.GetSubscribedJobSets()

		for _, val := range subscribeList {

			if val == "job-set-1" {
				assert.Equal(t, val, "job-set-1")
			} else if val == "job-set-2" {
				assert.Equal(t, val, "job-set-2")
			} else {
				assert.True(t, false)
			}
		}
	})
}

func TestUnscribeJobSetIfNonExist(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		r.UnSubscribeJobSet("job-set-1")
		assert.False(t, r.IsJobSetSubscribed("job-set-1"))
	})
}
func TestUnSubscribeJobSetHappy(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		r.SubscribeJobSet("job-set-1")
		respHappy := r.IsJobSetSubscribed("job-set-1")
		assert.True(t, respHappy)
		r.UnSubscribeJobSet("job-set-1")
		assert.False(t, r.IsJobSetSubscribed("job-set-1"))
	})
}

func TestPrintAllJobs(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)

		r.UpdateJobServiceDb(jobTable)
	})
}

func TestDeleteJobsInJobSet(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		var responseExpected3 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id-2", *responseExpected2)
		jobTable3 := NewJobTable("test", "job-set-2", "job-id-3", *responseExpected3)

		r.UpdateJobServiceDb(jobTable1)
		r.UpdateJobServiceDb(jobTable2)
		r.UpdateJobServiceDb(jobTable3)
		r.PersistDataToDatabase()
		jobResponse1, _ := r.GetJobStatus("job-id")
		jobResponse2, _ := r.GetJobStatus("job-id-2")
		jobResponse3, _ := r.GetJobStatus("job-id-3")

		assert.Equal(t, jobResponse1, responseExpected1)
		assert.Equal(t, jobResponse2, responseExpected2)
		assert.Equal(t, jobResponse3, responseExpected3)

		err := r.DeleteJobsInJobSet("job-set-1")
		assert.Nil(t, err)
		jobResponseDelete1, _ := r.GetJobStatus("job-id")
		jobResponseDelete2, _ := r.GetJobStatus("job-id-2")
		jobResponseDelete3, _ := r.GetJobStatus("job-id-3")
		var responseDoesNotExist = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		assert.Equal(t, jobResponseDelete1, responseDoesNotExist)
		assert.Equal(t, jobResponseDelete2, responseDoesNotExist)
		assert.Equal(t, jobResponseDelete3, responseExpected3)

		err = r.DeleteJobsInJobSet("job-set-1")
		assert.Nil(t, err)

	})
}

func TestCheckToUnSubscribe(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		var responseExpected3 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id-2", *responseExpected2)
		jobTable3 := NewJobTable("test", "job-set-2", "job-id-3", *responseExpected3)

		r.UpdateJobServiceDb(jobTable1)
		r.UpdateJobServiceDb(jobTable2)
		r.UpdateJobServiceDb(jobTable3)
		r.SubscribeJobSet("job-set-1")
		assert.True(t, r.IsJobSetSubscribed("job-set-1"))
		assert.False(t, r.CheckToUnSubscribe("job-set-1", 100000))

		time.Sleep(1 * time.Second)

		assert.True(t, r.CheckToUnSubscribe("job-set-1", 0))
	})
}
func TestCheckToUnSubscribeWithoutSubscribing(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-2", "job-id-3", *responseExpected2)

		r.UpdateJobServiceDb(jobTable1)
		r.UpdateJobServiceDb(jobTable2)
		assert.False(t, r.IsJobSetSubscribed("job-set-1"))
		assert.False(t, r.CheckToUnSubscribe("job-set-1", 100000))

	})
}
func TestUpdateJobSetTime(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		r.SubscribeJobSet("job-set-1")
		r.UpdateJobSetTime("job-set-1")
		_, ok := r.jobStatus.subscribeMap["job-set-1"]
		assert.True(t, ok)
	})
}

func TestUpdateJobSetTimeWithoutSubscribe(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		updateErr := r.UpdateJobSetTime("job-set-1")
		assert.EqualError(t, updateErr, "JobSet job-set-1 is already unsubscribed")
		_, ok := r.jobStatus.subscribeMap["job-set-1"]
		assert.False(t, ok)

	})
}

func TestPersistToDatabase(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id-2", *responseExpected2)

		r.UpdateJobServiceDb(jobTable1)
		r.UpdateJobServiceDb(jobTable2)

		persistErr := r.PersistDataToDatabase()
		assert.NoError(t, persistErr)
	})
}

func TestGetJobStatusSQL(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
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

		persistErr := r.PersistDataToDatabase()
		assert.NoError(t, persistErr)

		actualFailed, errFailed := r.GetJobStatusSQL("job-id")
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

func TestHealthCheck(t *testing.T) {
	WithInMemoryRepo(func(r *SQLJobServiceRepository) {
		healthCheck, err := r.HealthCheck()
		assert.True(t, healthCheck)
		assert.Nil(t, err)
	})
}

func WithInMemoryRepo(action func(r *SQLJobServiceRepository)) {
	jobMap := make(map[string]*JobTable)
	jobSet := make(map[string]*SubscribeTable)
	config := &configuration.JobServiceConfiguration{}
	config.PersistenceInterval = 1
	jobStatusMap := NewJobStatus(jobMap, jobSet)
	db, err := sql.Open("sqlite", "test.db")
	if err != nil {
		panic(err)
	}
	repo := NewSQLJobServiceRepository(jobStatusMap, config, db)
	repo.CreateTable()
	action(repo)
	db.Close()
	os.Remove("test.db")
}
