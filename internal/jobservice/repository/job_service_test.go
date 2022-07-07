package repository

import (
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api/jobservice"
)
func TestConstructJobService(t *testing.T) {
	withJobServiceRepo(func(r *RedisJobServiceRepository) {
		var responseExpected = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		err := r.UpdateJobServiceDb("job-set-1", &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND})
		assert.Nil(t, err)

		resp, err := r.GetJobStatus("job-set-1")
		assert.Nil(t, err)
		assert.Equal(t, resp, responseExpected)

	})
}


func withJobServiceRepo(action func(r *RedisJobServiceRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6380", DB: 1})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := NewRedisJobServiceRepository(client)
	action(repo)
}
