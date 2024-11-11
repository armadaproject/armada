package context

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNewGangSchedulingContext(t *testing.T) {
	jctxs := testNSmallCpuJobSchedulingContext("A", testfixtures.TestDefaultPriorityClass, 2)
	gctx := NewGangSchedulingContext(jctxs)
	assert.Equal(t, jctxs, gctx.JobSchedulingContexts)
	assert.Equal(t, "A", gctx.Queue)
	assert.Equal(t, testfixtures.TestDefaultPriorityClass, gctx.GangInfo.PriorityClassName)
	assert.True(
		t,
		testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(
			map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("8Gi"),
			},
		).Equal(
			gctx.TotalResourceRequests,
		),
	)
}
