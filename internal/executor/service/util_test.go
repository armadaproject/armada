package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/executor/job"
)

func TestChunkPods(t *testing.T) {
	j := &job.RunningJob{}
	chunks := chunkJobs([]*job.RunningJob{j, j, j}, 2)
	assert.Equal(t, [][]*job.RunningJob{{j, j}, {j}}, chunks)
}
