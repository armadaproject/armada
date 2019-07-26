package reporter

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestCreatePatchToMarkCurrentStateReported(t *testing.T) {
	podPhase := v1.PodPending
	pod := v1.Pod{
		Status: v1.PodStatus{
			Phase: podPhase,
		},
	}

	result := createPatchToMarkCurrentStateReported(&pod)

	//Check expected annotation is present
	val, ok := result.MetaData.Annotations[string(podPhase)]
	assert.True(t, ok)

	//Confirm full patch structure
	expected := domain.Patch{
		MetaData: metav1.ObjectMeta{
			Annotations: map[string]string{string(podPhase): val},
		},
	}
	assert.Equal(t, result, &expected)
}
