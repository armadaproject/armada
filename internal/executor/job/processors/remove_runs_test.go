package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/job"
)

func TestRun_RemoveRunProcessor(t *testing.T) {
	pod := createPod()

	terminalPod := pod.DeepCopy()
	terminalPod.Status.Phase = v1.PodFailed

	terminalPodWithStatusReported := pod.DeepCopy()
	terminalPodWithStatusReported.Annotations[string(v1.PodFailed)] = "value"
	terminalPodWithStatusReported.Status.Phase = v1.PodFailed

	runMeta, err := job.ExtractJobRunMeta(pod)
	require.NoError(t, err)

	activeJobRun := &job.RunState{
		Meta:  runMeta,
		Phase: job.Active,
	}

	cancelledJobRun := &job.RunState{
		Meta:            runMeta,
		Phase:           job.Active,
		CancelRequested: true,
	}

	tests := map[string]struct {
		initialPod         *v1.Pod
		initialRunState    *job.RunState
		expectPodDeleted   bool
		expectedRunDeleted bool
	}{
		"Delete pod if run cancelled": {
			initialPod:       pod,
			initialRunState:  cancelledJobRun,
			expectPodDeleted: true,
		},
		"Delete pod if run cancelled - terminal pod": {
			initialPod:       terminalPod,
			initialRunState:  cancelledJobRun,
			expectPodDeleted: true,
		},
		"Deletes run if run cancelled and no pod exists": {
			initialPod:         nil,
			initialRunState:    cancelledJobRun,
			expectedRunDeleted: true,
		},
		"Deletes run but leaves pod if pod is terminal and reported": {
			initialPod:         terminalPodWithStatusReported,
			initialRunState:    cancelledJobRun,
			expectedRunDeleted: true,
		},
		"Does nothing if run is not cancelled": {
			initialPod:      terminalPod,
			initialRunState: activeJobRun,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			runProcessor, executorContext, jobRunState := setupRemoveRunProcessorTest(t, tc.initialPod, tc.initialRunState)
			runProcessor.Run()

			if tc.expectPodDeleted {
				assert.Len(t, executorContext.Pods, 0)
			}

			if tc.expectedRunDeleted {
				assert.Equal(t, len(jobRunState.GetAll()), 0)
			} else {
				assert.Equal(t, len(jobRunState.GetAll()), 1)
			}
		})
	}
}

func setupRemoveRunProcessorTest(
	t *testing.T,
	existingPod *v1.Pod,
	existingJobRuns *job.RunState,
) (*RemoveRunProcessor, *fakecontext.SyncFakeClusterContext, *job.JobRunStateStore) {
	executorContext := fakecontext.NewSyncFakeClusterContext()
	jobRunState := job.NewJobRunStateStoreWithInitialState([]*job.RunState{existingJobRuns})
	if existingPod != nil {
		_, err := executorContext.SubmitPod(existingPod, "test", []string{})
		assert.NoError(t, err)
	}

	removeRunProcessor := NewRemoveRunProcessor(executorContext, jobRunState)
	return removeRunProcessor, executorContext, jobRunState
}
