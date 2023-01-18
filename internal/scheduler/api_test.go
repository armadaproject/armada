package scheduler

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecutorApi_LeaseJobRuns(t *testing.T) {
	tests := map[string]struct {
		initialJobs          []*SchedulerJob
		idsToDelete          []string
		expectedRemainingIds []string
	}{
		"Delete all jobs": {
			initialJobs: []*SchedulerJob{job1, job2},
			idsToDelete: []string{job1.JobId, job2.JobId},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Set up JobDb
			jobDb := createPopulatedJobDb(t, tc.initialJobs)

			// delete
			txn := jobDb.WriteTxn()
			err := jobDb.BatchDelete(txn, tc.idsToDelete)
			require.NoError(t, err)

			// check that db is as we expect
			allJobs, err := jobDb.GetAll(txn)
			outstandingIds := stringSet(tc.expectedRemainingIds)
			require.NoError(t, err)
			for _, job := range allJobs {
				_, ok := outstandingIds[job.JobId]
				assert.True(t, ok)
				delete(outstandingIds, job.JobId)
			}
			assert.Equal(t, 0, len(outstandingIds))
		})
	}
}
