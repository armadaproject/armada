package repository

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/pkg/api/jobservice"
)

func TestJobTable(t *testing.T) {
	jobStatus := NewJobStatus("test-queue", "test-job-set", "test-job-id", jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND})
	assert.Equal(t, jobStatus.queue, "test-queue")
	assert.Equal(t, jobStatus.jobSetId, "test-job-set")
	assert.Equal(t, jobStatus.jobId, "test-job-id")
	assert.Equal(t, jobStatus.jobResponse, jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND})
	assert.True(t, jobStatus.timeStamp > 0)
}
