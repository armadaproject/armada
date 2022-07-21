package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api/jobservice"
)

func TestConstructInMemoryDoesNotExist(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		err := r.UpdateJobServiceDb("job-set-1", &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND})
		assert.Nil(t, err)

		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}

func TestConstructInMemoryServiceFailed(t *testing.T) {
	WithInMemoryRepo(func(r *InMemoryJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"}
		err := r.UpdateJobServiceDb("job-set-1", &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND})
		assert.Nil(t, err)
		var failedErr = r.UpdateJobServiceDb("job-set-1", &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_FAILED, Error: "TestFail"})
		assert.Nil(t, failedErr)

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
		resp := r.IsJobSetAlreadySubscribed("job-set-1")
		assert.False(t, resp)
		resp2 := r.IsJobSetAlreadySubscribed("job-set-1")
		assert.True(t, resp2)
	})
}

func WithInMemoryRepo(action func(r *InMemoryJobServiceRepository)) {
	jobMap := make(map[string]*jobservice.JobServiceResponse)
	jobSet := make(map[string]string)
	repo := NewInMemoryJobServiceRepository(jobMap, jobSet)
	action(repo)
}
