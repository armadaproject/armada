package job

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/executor/domain"
)

func TestExtractJobRunMeta(t *testing.T) {
	tests := map[string]struct {
		jobId          string
		runId          string
		queue          string
		jobSet         string
		expectedOutput *RunMetaInfo
		expectError    bool
	}{
		"Valid": {
			jobId:       "job-id",
			runId:       "run-id",
			queue:       "queue",
			jobSet:      "job-set",
			expectError: false,
			expectedOutput: &RunMetaInfo{
				JobId:  "job-id",
				RunId:  "run-id",
				Queue:  "queue",
				JobSet: "job-set",
			},
		},
		"MissingJobId": {
			runId:       "run-id",
			queue:       "queue",
			jobSet:      "job-set",
			expectError: true,
		},
		"MissingRunId": {
			jobId:       "job-id",
			queue:       "queue",
			jobSet:      "job-set",
			expectError: true,
		},
		"MissingQueue": {
			jobId:       "job-id",
			runId:       "run-id",
			jobSet:      "job-set",
			expectError: true,
		},
		"MissingJobSet": {
			jobId:       "job-id",
			runId:       "run-id",
			queue:       "queue",
			expectError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						domain.JobId:    tc.jobId,
						domain.JobRunId: tc.runId,
						domain.Queue:    tc.queue,
					},
					Annotations: map[string]string{
						domain.JobSetId: tc.jobSet,
					},
				},
			}
			result, err := ExtractJobRunMeta(pod)
			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectedOutput, result)
			}
		})
	}
}
