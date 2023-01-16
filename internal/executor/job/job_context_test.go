package job

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	podchecksConfig "github.com/armadaproject/armada/internal/executor/configuration/podchecks"
	"github.com/armadaproject/armada/internal/executor/context/fake"
	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/podchecks"
	"github.com/armadaproject/armada/internal/executor/util"
)

func TestHandleDeletion_AddsPodIssue_OnUnexpectedDeletionOfArmadaJobs(t *testing.T) {
	jobContext := NewClusterJobContext(fake.NewSyncFakeClusterContext(), makeMinimalPodChecker(), time.Minute*3, 1)

	armadaPod := makeArmadaPod(v1.PodRunning)
	jobContext.handleDeletedPod(armadaPod)

	activeJobs, err := jobContext.GetJobs()
	assert.NoError(t, err)
	assert.Len(t, activeJobs, 1)
	assert.Equal(t, activeJobs[0].JobId, util.ExtractJobId(armadaPod))
	assert.Equal(t, activeJobs[0].Issue.Type, ExternallyDeleted)
}

func TestHandleDeletion_DoesNothing_OnDeletionOfNonArmadaJobs(t *testing.T) {
	jobContext := NewClusterJobContext(fake.NewSyncFakeClusterContext(), makeMinimalPodChecker(), time.Minute*3, 1)

	activeJobs, err := jobContext.GetJobs()
	assert.NoError(t, err)
	assert.Len(t, activeJobs, 0)

	jobContext.handleDeletedPod(&v1.Pod{})

	activeJobs, err = jobContext.GetJobs()
	assert.NoError(t, err)
	assert.Len(t, activeJobs, 0)
}

func TestHandleDeletion_DoesNothing_OnExpectedDeletionOfArmadaJobs(t *testing.T) {
	jobContext := NewClusterJobContext(fake.NewSyncFakeClusterContext(), makeMinimalPodChecker(), time.Minute*3, 1)

	activeJobs, err := jobContext.GetJobs()
	assert.NoError(t, err)
	assert.Len(t, activeJobs, 0)

	jobContext.handleDeletedPod(makeArmadaPodReadyForDeletion(v1.PodSucceeded))

	activeJobs, err = jobContext.GetJobs()
	assert.NoError(t, err)
	assert.Len(t, activeJobs, 0)
}

func makeArmadaPodReadyForDeletion(phase v1.PodPhase) *v1.Pod {
	pod := makeArmadaPod(phase)
	pod.ObjectMeta.Annotations = map[string]string{
		domain.MarkedForDeletion: time.Now().String(),
		domain.JobDoneAnnotation: time.Now().String(),
		string(phase):            time.Now().String(),
	}
	return pod
}

func makeArmadaPod(phase v1.PodPhase) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				domain.JobId:             "job-id-1",
				domain.MarkedForDeletion: "job-id-1",
			},
		},
		Status: v1.PodStatus{
			Phase: phase,
		},
	}
}

func makeMinimalPodChecker() podchecks.PodChecker {
	var cfg podchecksConfig.Checks
	cfg.Events = []podchecksConfig.EventCheck{}
	cfg.ContainerStatuses = []podchecksConfig.ContainerStatusCheck{}

	checker, err := podchecks.NewPodChecks(cfg)
	if err != nil {
		panic(fmt.Sprintf("Failed to make pod checker: %v", err))
	}

	return checker
}
