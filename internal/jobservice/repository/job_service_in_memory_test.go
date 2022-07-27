package repository

import (
	"testing"

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
		resp := r.IsJobSetSubscribed("job-set-1")
		assert.False(t, resp)
		r.SubscribeJobSet("job-set-1")
		resp2 := r.IsJobSetSubscribed("job-set-1")
		assert.True(t, resp2)
	})
}

func TestUnscribeJobSetIfNonExist(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		r.UnSubscribeJobSet("job-set-1")
		assert.False(t, r.IsJobSetSubscribed("job-set-1"))
	})
}
func TestUnSubscribeJobSetHappy(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		r.SubscribeJobSet("job-set-1")
		respHappy := r.IsJobSetSubscribed("job-set-1")
		assert.True(t, respHappy)
		r.UnSubscribeJobSet("job-set-1")
		assert.False(t, r.IsJobSetSubscribed("job-set-1"))
	})
}

func TestPrintAllJobs(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		jobTable := NewJobTable("test", "job-set-1", "job-id", *responseExpected)

		r.UpdateJobServiceDb("job-set-1", jobTable)
		r.PrintAllItems()
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

		r.DeleteJobsInJobSet("job-set-1")
		jobResponseDelete1, _ := r.GetJobStatus("job-id")
		jobResponseDelete2, _ := r.GetJobStatus("job-id-2")
		jobResponseDelete3, _ := r.GetJobStatus("job-id-3")
		var responseDoesNotExist = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		assert.Equal(t, jobResponseDelete1, responseDoesNotExist)
		assert.Equal(t, jobResponseDelete2, responseDoesNotExist)
		assert.Equal(t, jobResponseDelete3, responseExpected3)

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
	jobSet := make(map[string]*string)
	config := &configuration.JobServiceConfiguration{}
	config.PersistenceInterval = 1
	jobStatusMap := NewJobStatus(jobMap, jobSet)
	repo := NewInMemoryJobServiceRepository(jobStatusMap, config)
	action(repo)
}
