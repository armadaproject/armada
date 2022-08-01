package repository

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/pkg/api/jobservice"
)

func TestConstructInMemoryDoesNotExist(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)
		err := r.UpdateJobServiceDb("job-set-1", jobTable)
		assert.Nil(t, err)

		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestConstructInMemoryServiceFailed(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)

		err := r.UpdateJobServiceDb("job-set-1", jobTable)
		assert.Nil(t, err)

		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestConstructInMemoryServiceNoJob(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestIsJobSubscribed(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		resp := r.IsJobSetSubscribed("queue-1job-set-1")
		assert.False(t, resp)
		r.SubscribeJobSet("queue-1job-set-1")
		resp2 := r.IsJobSetSubscribed("queue-1job-set-1")
		assert.True(t, resp2)
	})
}

func TestSubscribeList(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		queueJobSet1 := "queue-1job-set-1"
		queueJobSet2 := "queue-1-job-set-2"
		r.SubscribeJobSet(queueJobSet1)
		r.SubscribeJobSet(queueJobSet2)

		subscribeList := r.GetSubscribedJobSets()

		for _, val := range subscribeList {

			if val == queueJobSet1 {
				assert.Equal(t, val, queueJobSet1)
			} else if val == queueJobSet2 {
				assert.Equal(t, val, queueJobSet2)
			} else {
				assert.True(t, false)
			}
		}
	})
}

func TestUnscribeJobSetIfNonExist(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		r.UnSubscribeJobSet("queuejob-set-1")
		assert.False(t, r.IsJobSetSubscribed("queuejob-set-1"))
	})
}
func TestUnSubscribeJobSetHappy(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		r.SubscribeJobSet("queuejob-set-1")
		respHappy := r.IsJobSetSubscribed("queuejob-set-1")
		assert.True(t, respHappy)
		r.UnSubscribeJobSet("queuejob-set-1")
		assert.False(t, r.IsJobSetSubscribed("queuejob-set-1"))
	})
}

func TestDeleteJobsInJobSet(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		var responseExpected3 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id-2", *responseExpected2)
		jobTable3 := NewJobTable("test", "job-set-2", "job-id-3", *responseExpected3)

		r.UpdateJobServiceDb("job-id", jobTable1)
		r.UpdateJobServiceDb("job-id-2", jobTable2)
		r.UpdateJobServiceDb("job-id-3", jobTable3)
		jobResponse1, _ := r.GetJobStatus("job-id")
		jobResponse2, _ := r.GetJobStatus("job-id-2")
		jobResponse3, _ := r.GetJobStatus("job-id-3")

		assert.Equal(t, jobResponse1, responseExpected1)
		assert.Equal(t, jobResponse2, responseExpected2)
		assert.Equal(t, jobResponse3, responseExpected3)

		r.SubscribeJobSet("testjob-set-1")
		r.DeleteJobsInJobSet("testjob-set-1")
		jobResponseDelete1, _ := r.GetJobStatus("job-id")
		jobResponseDelete2, _ := r.GetJobStatus("job-id-2")
		jobResponseDelete3, _ := r.GetJobStatus("job-id-3")
		var responseDoesNotExist = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		assert.Equal(t, jobResponseDelete1, responseDoesNotExist)
		assert.Equal(t, jobResponseDelete2, responseDoesNotExist)
		assert.Equal(t, jobResponseDelete3, responseExpected3)

	})
}

func TestCheckToUnSubscribe(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}
		var responseExpected3 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_RUNNING}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id-2", *responseExpected2)
		jobTable3 := NewJobTable("test", "job-set-2", "job-id-3", *responseExpected3)

		r.UpdateJobServiceDb("job-id", jobTable1)
		r.UpdateJobServiceDb("job-id-2", jobTable2)
		r.UpdateJobServiceDb("job-id-3", jobTable3)
		r.SubscribeJobSet("job-set-1")
		assert.True(t, r.IsJobSetSubscribed("job-set-1"))
		assert.False(t, r.CheckToUnSubscribe("job-set-1", 100000))

		time.Sleep(1 * time.Second)

		assert.True(t, r.CheckToUnSubscribe("job-set-1", 0))
	})
}
func TestCheckToUnSubscribeWithoutSubscribing(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-2", "job-id-3", *responseExpected2)

		r.UpdateJobServiceDb("job-id", jobTable1)
		r.UpdateJobServiceDb("job-id-2", jobTable2)
		assert.False(t, r.IsJobSetSubscribed("job-set-1"))
		assert.False(t, r.CheckToUnSubscribe("job-set-1", 100000))

	})
}
func TestUpdateJobSetTime(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		r.SubscribeJobSet("job-set-1")
		r.UpdateJobSetTime("job-set-1")
		_, ok := r.jobStatus.subscribeMap["job-set-1"]
		assert.True(t, ok)
	})
}

func TestUpdateJobSetTimeWithoutSubscribe(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		updateErr := r.UpdateJobSetTime("job-set-1")
		assert.EqualError(t, updateErr, "JobSet job-set-1 is already unsubscribed")
		_, ok := r.jobStatus.subscribeMap["job-set-1"]
		assert.False(t, ok)

	})
}
func TestPersistToDatabase(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected1 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		var responseExpected2 = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_SUCCEEDED}

		jobTable1 := NewJobTable("test", "job-set-1", "job-id", *responseExpected1)
		jobTable2 := NewJobTable("test", "job-set-1", "job-id-2", *responseExpected2)

		r.UpdateJobServiceDb("job-id", jobTable1)
		r.UpdateJobServiceDb("job-id-2", jobTable2)

		persistErr := r.PersistDataToDatabase()
		assert.NoError(t, persistErr)
	})
}

func TestHealthCheck(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		healthCheck := r.HealthCheck()
		assert.True(t, healthCheck)
	})
}
func WithInMemoryRepo(action func(r *InMemoryJobServiceRepository)) {
	jobMap := make(map[string]*JobTable)
	jobSet := make(map[string]*SubscribeTable)
	config := &configuration.JobServiceConfiguration{}
	config.PersistenceInterval = 1
	jobStatusMap := NewJobStatus(jobMap, jobSet)
	repo := NewInMemoryJobServiceRepository(jobStatusMap, config)
	action(repo)
}
