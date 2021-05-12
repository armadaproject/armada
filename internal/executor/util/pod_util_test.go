package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/armada/internal/executor/domain"
)

func TestIsInTerminalState_ShouldReturnTrueWhenPodInSucceededPhase(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)
	assert.True(t, inTerminatedState)
}

func TestIsInTerminalState_ShouldReturnTrueWhenPodInFailedPhase(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)
	assert.True(t, inTerminatedState)
}

func TestIsInTerminalState_ShouldReturnFalseWhenPodInNonTerminalState(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	inTerminatedState := IsInTerminalState(&pod)

	assert.False(t, inTerminatedState)
}

func TestHasIngress(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{domain.HasIngress: "true"},
		},
	}

	assert.True(t, HasIngress(pod))
}

func TestHasIngress_WhenAnnotationNotPresent(t *testing.T) {
	pod := &v1.Pod{}
	assert.False(t, HasIngress(pod))
}

func TestHasIngress_WhenAnnotationNotSetToTrue(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{domain.HasIngress: "other value"},
		},
	}

	assert.False(t, HasIngress(pod))
}

func TestIsManagedPod_ReturnsTrueIfJobIdLabelPresent(t *testing.T) {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{domain.JobId: "label"},
		},
	}

	result := IsManagedPod(&pod)

	assert.True(t, result)
}

func TestIsManagedPod_ReturnsFalseIfJobIdLabelNotPresent(t *testing.T) {
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{},
		},
	}

	result := IsManagedPod(&pod)

	assert.False(t, result)
}

func TestIsManagedPod_ReturnsFalseIfNoLabelsPresent(t *testing.T) {
	pod := v1.Pod{}

	result := IsManagedPod(&pod)

	assert.False(t, result)
}

func TestFilterCompletedPods(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	completedPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result := FilterCompletedPods([]*v1.Pod{&runningPod, &completedPod})

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], &completedPod)
}

func TestFilterCompletedPods_ShouldReturnEmptyIfNoCompletedPods(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	pendingPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodPending,
		},
	}

	result := FilterCompletedPods([]*v1.Pod{&runningPod, &pendingPod})

	assert.Equal(t, len(result), 0)
}

func TestFilterNonCompletedPods(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	completedPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result := FilterNonCompletedPods([]*v1.Pod{&runningPod, &completedPod})

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], &runningPod)
}

func TestFilterNonCompletedPods_ShouldReturnEmptyIfAllPodsCompleted(t *testing.T) {
	succeededPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	failedPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}

	result := FilterNonCompletedPods([]*v1.Pod{&succeededPod, &failedPod})

	assert.Equal(t, len(result), 0)
}

func TestFilterPodsWithPhase(t *testing.T) {
	runningPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	completedPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	result := FilterPodsWithPhase([]*v1.Pod{&runningPod, &completedPod}, v1.PodRunning)

	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], &runningPod)
}

func TestFilterPodsWithPhase_ShouldReturnEmptyIfNoPodWithPhaseExists(t *testing.T) {
	succeededPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodSucceeded,
		},
	}

	failedPod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodFailed,
		},
	}

	result := FilterPodsWithPhase([]*v1.Pod{&succeededPod, &failedPod}, v1.PodPending)

	assert.Equal(t, len(result), 0)
}

func TestExtractJobIds(t *testing.T) {
	jobIds := []string{"1", "2", "3", "4"}
	pods := makePodsWithJobIds(jobIds)

	result := ExtractJobIds(pods)
	assert.Equal(t, result, jobIds)
}

func TestExtractJobIds_HandlesEmptyList(t *testing.T) {
	expected := []string{}
	pods := []*v1.Pod{}

	result := ExtractJobIds(pods)
	assert.Equal(t, result, expected)
}

func TestExtractJobIds_SkipsWhenJobIdNotPresent(t *testing.T) {
	expected := []string{}
	podWithNoJobId := v1.Pod{}
	pods := []*v1.Pod{&podWithNoJobId}

	result := ExtractJobIds(pods)
	assert.Equal(t, result, expected)
}

func TestExtractJobId(t *testing.T) {
	pod := makePodsWithJobIds([]string{"1"})[0]

	result := ExtractJobId(pod)
	assert.Equal(t, result, "1")
}

func TestExtractJobId_ReturnsEmpty_WhenJobIdNotPresent(t *testing.T) {
	pod := v1.Pod{}

	result := ExtractJobId(&pod)
	assert.Equal(t, result, "")
}

func makePodsWithJobIds(jobIds []string) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(jobIds))

	for _, jobId := range jobIds {
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{domain.JobId: jobId},
			},
		}
		pods = append(pods, &pod)
	}

	return pods
}

func TestIsReportingPhaseRequired(t *testing.T) {
	assert.Equal(t, true, IsReportingPhaseRequired(v1.PodRunning))
	assert.Equal(t, true, IsReportingPhaseRequired(v1.PodSucceeded))
	assert.Equal(t, true, IsReportingPhaseRequired(v1.PodFailed))

	assert.Equal(t, false, IsReportingPhaseRequired(v1.PodPending))
	assert.Equal(t, false, IsReportingPhaseRequired(v1.PodUnknown))
}

func TestMergePodList(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Pod1",
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Pod2",
		},
	}

	result := MergePodList([]*v1.Pod{pod1}, []*v1.Pod{pod2})

	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0], pod1)
	assert.Equal(t, result[1], pod2)
}

func TestMergePodList_DoesNotAddDuplicates(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Pod1",
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Pod2",
		},
	}

	result := MergePodList([]*v1.Pod{pod1, pod2}, []*v1.Pod{pod2})

	assert.Equal(t, len(result), 2)
	assert.Equal(t, result[0], pod1)
	assert.Equal(t, result[1], pod2)
}

func TestMergePodList_HandlesListsBeingEmpty(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Pod1",
		},
	}

	result := MergePodList([]*v1.Pod{pod1}, []*v1.Pod{})
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], pod1)

	result = MergePodList([]*v1.Pod{}, []*v1.Pod{pod1})
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0], pod1)
}

func TestMergePodList_DoesNotModifyOriginalList(t *testing.T) {
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Pod1",
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "Pod2",
		},
	}

	list1 := []*v1.Pod{pod1}

	result := MergePodList(list1, []*v1.Pod{pod2})

	assert.Equal(t, len(list1), 1)
	assert.Equal(t, len(result), 2)
}

func TestLastStatusChange_ReturnsExpectedValue(t *testing.T) {
	earliest := time.Date(2019, 11, 20, 9, 30, 3, 0, time.UTC)
	middle := time.Date(2019, 11, 20, 9, 31, 4, 0, time.UTC)
	latest := time.Date(2019, 11, 20, 9, 31, 5, 0, time.UTC)
	conditions := []v1.PodCondition{
		{
			LastTransitionTime: metav1.NewTime(earliest),
		},
		{
			LastTransitionTime: metav1.NewTime(latest),
		},
		{
			LastTransitionTime: metav1.NewTime(middle),
		},
	}

	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: conditions,
		},
	}

	result, _ := LastStatusChange(&pod)

	assert.Equal(t, result, latest)
}

func TestLastStatusChange_ReturnsError_WhenNoStateChangesFound(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Conditions: make([]v1.PodCondition, 0),
		},
	}

	_, err := LastStatusChange(&pod)

	assert.NotNil(t, err)
}

func TestLastStatusChange_ReturnsCreatedTime_WhenNoStateChangesFoundForPendingPod(t *testing.T) {
	creationTime := time.Date(2019, 11, 20, 9, 31, 5, 0, time.UTC)
	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(creationTime),
		},
		Status: v1.PodStatus{
			Phase:      v1.PodPending,
			Conditions: make([]v1.PodCondition, 0),
		},
	}
	result, _ := LastStatusChange(&pod)

	assert.Equal(t, result, creationTime)
}

func TestLastStatusChange_ReturnsError_WhenNoStateChangesFoundAndCreatedTimeIsNotSetForPendingPod(t *testing.T) {
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase:      v1.PodPending,
			Conditions: make([]v1.PodCondition, 0),
		},
	}
	_, err := LastStatusChange(&pod)

	assert.NotNil(t, err)
}

func TestLastStatusChange_ReportsTimeFromContainerStatus(t *testing.T) {
	now := time.Now()
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{{
				State: v1.ContainerState{Running: &v1.ContainerStateRunning{
					StartedAt: metav1.NewTime(now),
				}},
			}},
		},
	}
	result, err := LastStatusChange(&pod)
	assert.Equal(t, result, now)
	assert.Nil(t, err)

	pod.Status.ContainerStatuses = []v1.ContainerStatus{{
		State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
			FinishedAt: metav1.NewTime(now),
		}},
	}}
	result, err = LastStatusChange(&pod)
	assert.Equal(t, result, now)
	assert.Nil(t, err)
}
