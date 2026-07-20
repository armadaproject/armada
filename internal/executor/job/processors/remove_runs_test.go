package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/reporter/mocks"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
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
			runProcessor, executorContext, jobRunState, _ := setupRemoveRunProcessorTest(t, tc.initialPod, tc.initialRunState)
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

func TestRun_RemoveRunProcessor_ReportsDebugWhenMainContainerNeverStarted(t *testing.T) {
	pod := createPod() // No container statuses - main container never started
	runMeta, err := job.ExtractJobRunMeta(pod)
	require.NoError(t, err)
	cancelledJobRun := &job.RunState{Meta: runMeta, Phase: job.Active, CancelRequested: true}

	runProcessor, executorContext, _, eventReporter := setupRemoveRunProcessorTest(t, pod, cancelledJobRun)
	executorContext.Events[util.ExtractJobId(pod)] = []*v1.Event{{Message: "0/8 nodes are available", Type: "Warning"}}

	runProcessor.Run()

	assert.Len(t, executorContext.Pods, 0, "pod should still be deleted")
	require.Len(t, eventReporter.ReceivedEvents, 1)
	require.Len(t, eventReporter.ReceivedEvents[0].Event.Events, 1)
	debugInfo, ok := eventReporter.ReceivedEvents[0].Event.Events[0].Event.(*armadaevents.EventSequence_Event_JobCancelledDebugInfo)
	require.True(t, ok, "expected a JobCancelledDebugInfo event")
	assert.Contains(t, debugInfo.JobCancelledDebugInfo.DebugMessage, "0/8 nodes are available")
}

func TestRun_RemoveRunProcessor_NoDebugWhenNoPodEvents(t *testing.T) {
	pod := createPod() // Main container never started, but no k8s events recorded
	runMeta, err := job.ExtractJobRunMeta(pod)
	require.NoError(t, err)
	cancelledJobRun := &job.RunState{Meta: runMeta, Phase: job.Active, CancelRequested: true}

	runProcessor, executorContext, _, eventReporter := setupRemoveRunProcessorTest(t, pod, cancelledJobRun)
	// No events seeded for this pod

	runProcessor.Run()

	assert.Len(t, executorContext.Pods, 0, "pod should still be deleted")
	assert.Len(t, eventReporter.ReceivedEvents, 0, "no debug event should be emitted when there are no pod events")
}

func TestRun_RemoveRunProcessor_NoDebugWhenMainContainerStarted(t *testing.T) {
	pod := createPod()
	pod.Status.ContainerStatuses = []v1.ContainerStatus{
		{Name: "main", State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 0}}},
	}
	runMeta, err := job.ExtractJobRunMeta(pod)
	require.NoError(t, err)
	cancelledJobRun := &job.RunState{Meta: runMeta, Phase: job.Active, CancelRequested: true}

	runProcessor, executorContext, _, eventReporter := setupRemoveRunProcessorTest(t, pod, cancelledJobRun)
	executorContext.Events[util.ExtractJobId(pod)] = []*v1.Event{{Message: "0/8 nodes are available", Type: "Warning"}}

	runProcessor.Run()

	assert.Len(t, executorContext.Pods, 0)
	assert.Len(t, eventReporter.ReceivedEvents, 0, "no debug event should be emitted when the main container started")
}

func setupRemoveRunProcessorTest(
	t *testing.T,
	existingPod *v1.Pod,
	existingJobRuns *job.RunState,
) (*RemoveRunProcessor, *fakecontext.SyncFakeClusterContext, *job.JobRunStateStore, *mocks.FakeEventReporter) {
	executorContext := fakecontext.NewSyncFakeClusterContext()
	jobRunState := job.NewJobRunStateStoreWithInitialState([]*job.RunState{existingJobRuns})
	if existingPod != nil {
		_, err := executorContext.SubmitPod(existingPod, "test", []string{})
		assert.NoError(t, err)
	}

	eventReporter := mocks.NewFakeEventReporter()
	removeRunProcessor := NewRemoveRunProcessor(executorContext, jobRunState, eventReporter)
	return removeRunProcessor, executorContext, jobRunState, eventReporter
}
