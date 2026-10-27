package internaltypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestJobSchedulingInfo_ResourceMutationsRoundTrip(t *testing.T) {
	tests := map[string]struct {
		mutations *RetryResourceMutations
	}{
		"nil mutations stay nil": {mutations: nil},
		"factor survives":        {mutations: &RetryResourceMutations{MemoryFactor: 1.21}},
		"static survives":        {mutations: &RetryResourceMutations{MemoryStatic: "512Mi"}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			info := &JobSchedulingInfo{
				Lifetime:      1,
				PriorityClass: "test",
				SubmitTime:    time.Now().UTC().Truncate(time.Second),
				Priority:      2,
				PodRequirements: &PodRequirements{
					ResourceRequirements: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
					},
				},
				Version:           3,
				ResourceMutations: tc.mutations,
			}

			roundTripped, err := FromSchedulerObjectsJobSchedulingInfo(ToSchedulerObjectsJobSchedulingInfo(info))
			require.NoError(t, err)
			assert.Equal(t, tc.mutations, roundTripped.ResourceMutations)
			assert.Equal(t, info.Version, roundTripped.Version)
		})
	}
}

func TestRetryResourceMutationsToProto(t *testing.T) {
	assert.Nil(t, RetryResourceMutationsToProto(nil))
	proto := RetryResourceMutationsToProto(&RetryResourceMutations{MemoryFactor: 1.1, MemoryStatic: "1Gi"})
	require.NotNil(t, proto)
	assert.Equal(t, &schedulerobjects.RetryResourceMutations{MemoryFactor: 1.1, MemoryStatic: "1Gi"}, proto)
}
