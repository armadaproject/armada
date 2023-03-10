package job

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	util2 "github.com/armadaproject/armada/internal/common/util"
	fakecontext "github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
)

var defaultRunInfoMeta = &RunMeta{
	RunId:  "run-1",
	JobId:  "job-1",
	Queue:  "queue-1",
	JobSet: "job-set-1",
}

func TestOnStartUp_ReconcilesWithKubernetes(t *testing.T) {
	existingPod := createPod()

	jobRunStateManager, _ := setup(t, []*v1.Pod{existingPod})
	allKnownJobRuns := jobRunStateManager.GetAll()

	assert.Len(t, allKnownJobRuns, 1)
	assert.Equal(t, allKnownJobRuns[0].Meta.JobId, util.ExtractJobId(existingPod))
	assert.Equal(t, allKnownJobRuns[0].Meta.RunId, util.ExtractJobRunId(existingPod))
	assert.Equal(t, allKnownJobRuns[0].Meta.Queue, util.ExtractQueue(existingPod))
	assert.Equal(t, allKnownJobRuns[0].Meta.JobSet, util.ExtractJobSet(existingPod))
	assert.Equal(t, allKnownJobRuns[0].KubernetesId, string(existingPod.UID))
	assert.Equal(t, allKnownJobRuns[0].Phase, Active)
}

func TestReportRunLeased(t *testing.T) {
	jobRunStateManager, _ := setup(t, []*v1.Pod{})
	jobRunStateManager.ReportRunLeased(defaultRunInfoMeta, &SubmitJob{})

	allKnownJobRuns := jobRunStateManager.GetAll()
	assert.Len(t, allKnownJobRuns, 1)
	assert.Equal(t, allKnownJobRuns[0].Meta, defaultRunInfoMeta)
	assert.Equal(t, allKnownJobRuns[0].KubernetesId, "")
	assert.Equal(t, allKnownJobRuns[0].Phase, Leased)
}

func TestReportFailedSubmission(t *testing.T) {
	jobRunStateManager, _ := setup(t, []*v1.Pod{})
	jobRunStateManager.ReportFailedSubmission(defaultRunInfoMeta)

	allKnownJobRuns := jobRunStateManager.GetAll()
	assert.Len(t, allKnownJobRuns, 1)
	assert.Equal(t, allKnownJobRuns[0].Meta, defaultRunInfoMeta)
	assert.Equal(t, allKnownJobRuns[0].KubernetesId, "")
	assert.Equal(t, allKnownJobRuns[0].Phase, FailedSubmission)
}

func TestOnPodEvent_MovesJobRunStateToActive(t *testing.T) {
	jobRunStateManager, executorContext := setup(t, []*v1.Pod{})

	pod1 := createPod()
	runInfo := &RunMeta{
		RunId:  util.ExtractJobRunId(pod1),
		JobId:  util.ExtractJobId(pod1),
		Queue:  util.ExtractQueue(pod1),
		JobSet: util.ExtractJobSet(pod1),
	}

	// Add leased job run
	jobRunStateManager.ReportRunLeased(runInfo, &SubmitJob{})
	jobRun := jobRunStateManager.Get(runInfo.RunId)
	assert.NotNil(t, jobRun)
	assert.Equal(t, jobRun.Phase, Leased)

	// Simulate pod added to kubernetes
	executorContext.SimulatePodAddEvent(pod1)

	jobRun = jobRunStateManager.Get(runInfo.RunId)
	assert.NotNil(t, jobRun)
	assert.Equal(t, jobRun.Phase, Active)
}

func TestDelete(t *testing.T) {
	jobRunStateManager, _ := setup(t, []*v1.Pod{})

	// Add job run
	jobRunStateManager.ReportRunLeased(defaultRunInfoMeta, &SubmitJob{})
	assert.Len(t, jobRunStateManager.GetAll(), 1)

	jobRunStateManager.Delete(defaultRunInfoMeta.RunId)
	assert.Len(t, jobRunStateManager.GetAll(), 0)
}

func TestGet(t *testing.T) {
	jobRunStateManager, _ := setup(t, []*v1.Pod{})

	// Add job run
	jobRunStateManager.ReportRunLeased(defaultRunInfoMeta, &SubmitJob{})

	jobRun := jobRunStateManager.Get(defaultRunInfoMeta.RunId)
	assert.NotNil(t, jobRun)
	assert.Equal(t, jobRun.Meta, defaultRunInfoMeta)
}

func TestGetAll(t *testing.T) {
	pod1 := createPod()
	pod2 := createPod()
	jobRunStateManager, _ := setup(t, []*v1.Pod{pod1, pod2})

	assert.Len(t, jobRunStateManager.GetAll(), 2)
}

func TestGetByKubernetesId(t *testing.T) {
	pod1 := createPod()
	pod2 := createPod()
	jobRunStateManager, _ := setup(t, []*v1.Pod{pod1, pod2})

	jobRun := jobRunStateManager.GetByKubernetesId(string(pod1.UID))
	assert.NotNil(t, jobRun)
	assert.Equal(t, jobRun.KubernetesId, string(pod1.UID))

	jobRun = jobRunStateManager.GetByKubernetesId(string(pod2.UID))
	assert.NotNil(t, jobRun)
	assert.Equal(t, jobRun.KubernetesId, string(pod2.UID))
}

func setup(t *testing.T, existingPods []*v1.Pod) (*JobRunStateStore, *fakecontext.SyncFakeClusterContext) {
	executorContext := fakecontext.NewSyncFakeClusterContext()
	for _, pod := range existingPods {
		_, err := executorContext.SubmitPod(pod, "test", []string{})
		assert.NoError(t, err)
	}

	jobRunStateManager := NewJobRunStateStore(executorContext)
	return jobRunStateManager, executorContext
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
