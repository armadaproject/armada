package processors

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/job"
)

func TestCreateRunPodInfos(t *testing.T) {
	pod := makePodWithRunId("run-1")
	run := makeRunStateWithRunId("run-1")

	result := createRunPodInfos([]*job.RunState{run}, []*v1.Pod{pod})

	assert.Len(t, result, 1)
	assert.Equal(t, result[0].Run, run)
	assert.Equal(t, result[0].Pod, pod)
}

func TestCreateRunPodInfos_WhenRunHasNoMatchingPod(t *testing.T) {
	run := makeRunStateWithRunId("run-1")
	pod := makePodWithRunId("run-2")
	result := createRunPodInfos([]*job.RunState{run}, []*v1.Pod{pod})

	assert.Len(t, result, 1)
	assert.Equal(t, result[0].Run, run)
	assert.Nil(t, result[0].Pod)
}

func TestCreateRunPodInfos_WhenNoRunStates_ReturnsEmpty(t *testing.T) {
	result := createRunPodInfos(nil, []*v1.Pod{makePodWithRunId("run-1")})
	assert.Empty(t, result)
}

func makePodWithRunId(runId string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				domain.JobRunId: runId,
			},
		},
	}
}

func makeRunStateWithRunId(runId string) *job.RunState {
	return &job.RunState{
		Meta: &job.RunMeta{
			RunId: runId,
		},
	}
}
