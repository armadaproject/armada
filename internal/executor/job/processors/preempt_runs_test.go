package processors

import (
	"fmt"
	"testing"

	util2 "github.com/armadaproject/armada/internal/common/util"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
	fake2 "github.com/armadaproject/armada/internal/executor/job/fake"
	"github.com/armadaproject/armada/internal/executor/reporter/fake"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestRun_PreemptedRunProcessor(t *testing.T) {
	pod := createPod()

	reportedPod := pod.DeepCopy()
	reportedPod.Annotations[domain.JobPreemptedAnnotation] = ""
	reportedPod.Annotations[string(v1.PodFailed)] = ""

	terminalPod := pod.DeepCopy()
	terminalPod.Status.Phase = v1.PodFailed

	runMeta, err := job.ExtractJobRunMeta(pod)
	require.NoError(t, err)

	activeJobRun := &job.RunState{
		Meta:  runMeta,
		Phase: job.Active,
	}

	preemptedJobRun := &job.RunState{
		Meta:                runMeta,
		Phase:               job.Active,
		PreemptionRequested: true,
	}

	tests := map[string]struct {
		initialPod           *v1.Pod
		initialRunState      *job.RunState
		expectEvents         bool
		expectAddAnnotations bool
		expectPodDeleted     bool
	}{
		"Reports preempted events and deletes pod": {
			initialPod:           pod,
			initialRunState:      preemptedJobRun,
			expectAddAnnotations: true,
			expectEvents:         true,
			expectPodDeleted:     true,
		},
		"Only deletes pod if already reported": {
			initialPod:       reportedPod,
			initialRunState:  preemptedJobRun,
			expectPodDeleted: true,
		},
		"Does nothing if no pod": {
			initialPod:      nil,
			initialRunState: preemptedJobRun,
		},
		"Does nothing if pod is already terminal": {
			initialPod:      terminalPod,
			initialRunState: preemptedJobRun,
		},
		"Does nothing if run is not marked for preemption": {
			initialPod:      pod,
			initialRunState: activeJobRun,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			runProcessor, executorContext, eventReporter := setupPreemptRunProcessorTest(t, tc.initialPod, tc.initialRunState)
			runProcessor.Run()

			if tc.expectPodDeleted {
				assert.Len(t, executorContext.Pods, 0)
			}

			if tc.expectAddAnnotations {
				addedAnnotations, exist := executorContext.AnnotationsAdded[util.ExtractJobId(tc.initialPod)]
				assert.True(t, exist)
				assert.Len(t, addedAnnotations, 2)
				assert.True(t, addedAnnotations[domain.JobPreemptedAnnotation] != "")
				assert.True(t, addedAnnotations[string(v1.PodFailed)] != "")
			} else {
				assert.Len(t, executorContext.AnnotationsAdded, 0)
			}

			if tc.expectEvents {
				events := eventReporter.ReceivedEvents
				assert.Len(t, events, 2)
				_, ok := eventReporter.ReceivedEvents[0].Event.(*api.JobPreemptedEvent)
				assert.True(t, ok)
				_, ok = eventReporter.ReceivedEvents[1].Event.(*api.JobFailedEvent)
				assert.True(t, ok)
			} else {
				assert.Len(t, eventReporter.ReceivedEvents, 0)
			}
		})
	}
}

func setupPreemptRunProcessorTest(
	t *testing.T,
	existingPod *v1.Pod,
	existingJobRuns *job.RunState) (*RunPreemptedProcessor, *fakecontext.SyncFakeClusterContext, *fake.FakeEventReporter) {
	executorContext := fakecontext.NewSyncFakeClusterContext()
	if existingPod != nil {
		_, err := executorContext.SubmitPod(existingPod, "test", []string{})
		assert.NoError(t, err)
	}

	eventReporter := fake.NewFakeEventReporter()
	jobRunState := fake2.NewStubRunStateStore([]*job.RunState{existingJobRuns})
	preemptRunProcessor := NewRunPreemptedProcessor(executorContext, jobRunState, eventReporter)
	return preemptRunProcessor, executorContext, eventReporter
}

func createPod() *v1.Pod {
	jobId := util2.NewULID()
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       types.UID(util2.NewULID()),
			Name:      fmt.Sprintf("armada-%s-0", jobId),
			Namespace: util2.NewULID(),
			Labels: map[string]string{
				domain.JobId:    jobId,
				domain.JobRunId: util2.NewULID(),
				domain.Queue:    util2.NewULID(),
			},
			Annotations: map[string]string{
				domain.JobSetId: fmt.Sprintf("job-set-%s", util2.NewULID()),
			},
		},
	}
}
