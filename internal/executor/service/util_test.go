package service

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func TestChunkPods(t *testing.T) {
	j := &job.RunningJob{}
	chunks := chunkJobs([]*job.RunningJob{j, j, j}, 2)
	assert.Equal(t, [][]*job.RunningJob{{j, j}, {j}}, chunks)
}

func TestExtractEssentialJobMetadata(t *testing.T) {
	jobId, runId, jobRunLease := createValidJobRunLease(t, "queue-1", "job-set-1")

	expected := &job.RunMeta{
		JobId:  jobId,
		RunId:  runId,
		Queue:  "queue-1",
		JobSet: "job-set-1",
	}

	result, err := ExtractEssentialJobMetadata(jobRunLease)
	assert.NoError(t, err)
	assert.Equal(t, result, expected)
}

func TestExtractEssentialJobMetadata_InvalidInput(t *testing.T) {
	// Invalid run id
	_, _, jobRunLease := createValidJobRunLease(t, "queue-1", "job-set-1")
	jobRunLease.JobRunId = nil
	result, err := ExtractEssentialJobMetadata(jobRunLease)
	assert.Nil(t, result)
	assert.Error(t, err)

	// Empty queue
	_, _, jobRunLease = createValidJobRunLease(t, "queue-1", "job-set-1")
	jobRunLease.Queue = ""
	result, err = ExtractEssentialJobMetadata(jobRunLease)
	assert.Nil(t, result)
	assert.Error(t, err)

	// Empty jobset
	_, _, jobRunLease = createValidJobRunLease(t, "queue-1", "job-set-1")
	jobRunLease.Jobset = ""
	result, err = ExtractEssentialJobMetadata(jobRunLease)
	assert.Nil(t, result)
	assert.Error(t, err)

	// Nil job
	_, _, jobRunLease = createValidJobRunLease(t, "queue-1", "job-set-1")
	jobRunLease.Job = nil
	result, err = ExtractEssentialJobMetadata(jobRunLease)
	assert.Nil(t, result)
	assert.Error(t, err)

	// Invalid jobId
	_, _, jobRunLease = createValidJobRunLease(t, "queue-1", "job-set-1")
	jobRunLease.Job.JobId = nil
	result, err = ExtractEssentialJobMetadata(jobRunLease)
	assert.Nil(t, result)
	assert.Error(t, err)
}

func createValidJobRunLease(t *testing.T, queue string, jobSet string) (string, string, *executorapi.JobRunLease) {
	jobId := util.NewULID()
	runId := uuid.New().String()
	protoJobId, err := armadaevents.ProtoUuidFromUlidString(jobId)
	require.NoError(t, err)
	protoRunId, err := armadaevents.ProtoUuidFromUuidString(runId)
	require.NoError(t, err)

	return jobId, runId, &executorapi.JobRunLease{
		JobRunId: protoRunId,
		Queue:    queue,
		Jobset:   jobSet,
		Job: &armadaevents.SubmitJob{
			JobId: protoJobId,
		},
	}
}
