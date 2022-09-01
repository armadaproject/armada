package scheduler

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMergeIn(t *testing.T) {
	jobId1 := uuid.New()
	jobId2 := uuid.New()
	jobId3 := uuid.New()
	markJobsCancelled1 := MarkJobsCancelled{jobId1: false, jobId2: false}
	markJobsCancelled2 := MarkJobsCancelled{jobId2: true, jobId3: true}
	ok := markJobsCancelled1.MergeIn(markJobsCancelled2)
	assert.True(t, ok)
	assert.Equal(t, MarkJobsCancelled{jobId1: false, jobId2: true, jobId3: true}, markJobsCancelled1)

	jobId4 := uuid.New()
	markJobsSucceeded1 := MarkJobsSucceeded{jobId1: true, jobId4: true}
	ok = markJobsCancelled1.MergeIn(markJobsSucceeded1)
	assert.False(t, ok)
	assert.Equal(t, MarkJobsCancelled{jobId1: false, jobId2: true, jobId3: true}, markJobsCancelled1)
}
