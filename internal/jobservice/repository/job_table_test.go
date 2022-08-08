package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api/jobservice"
)

func TestJobTable(t *testing.T) {
	jobTable := NewJobTable("test-queue", "test-job-set", "test-job-id", jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND})
	assert.Equal(t, jobTable.queue, "test-queue")
	assert.Equal(t, jobTable.jobSetId, "test-job-set")
	assert.Equal(t, jobTable.jobId, "test-job-id")
	assert.Equal(t, jobTable.jobResponse, jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND})
	assert.True(t, jobTable.timeStamp > 0)
}
