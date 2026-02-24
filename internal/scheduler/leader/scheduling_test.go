package leader

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

type Item struct {
	tolerations []v1.Toleration
}

func (i *Item) getTolerations() []v1.Toleration {
	return slices.Clone(i.tolerations)
}

func TestBench(t *testing.T) {
	tolerations := []v1.Toleration{
		{Key: "a", Value: "true", Effect: v1.TaintEffectNoSchedule},
		{Key: "b", Value: "true", Effect: v1.TaintEffectNoSchedule},
		{Key: "c", Value: "true", Effect: v1.TaintEffectNoSchedule},
		{Key: "d", Value: "true", Effect: v1.TaintEffectNoSchedule},
		{Key: "e", Value: "true", Effect: v1.TaintEffectNoSchedule},
		{Key: "f", Value: "true", Effect: v1.TaintEffectNoSchedule},
		{Key: "l4", Value: "true", Effect: v1.TaintEffectNoSchedule},
	}

	items := make([]*Item, 500000)
	for i := 0; i < 500000; i++ {
		items[i] = &Item{tolerations: tolerations}
	}

	start := time.Now()
	tolerationCount := 0
	for _, item := range items {
		tolerations := item.getTolerations()
		tolerationCount += len(tolerations)
	}
	fmt.Println(fmt.Sprintf("toleration count %d", tolerationCount))
	fmt.Println(fmt.Sprintf("elapsed time %s", time.Since(start)))
}

func TestInstantiate(t *testing.T) {
	jobs := testfixtures.N1Cpu4GiJobs("A", testfixtures.PriorityClass4PreemptibleAway, 500000)

	wellKnownNodeTypeTolerations := map[string][]v1.Toleration{
		"gpu": {
			{Key: "a", Value: "true", Effect: v1.TaintEffectNoSchedule},
			{Key: "b", Value: "true", Effect: v1.TaintEffectNoSchedule},
			{Key: "c", Value: "true", Effect: v1.TaintEffectNoSchedule},
			{Key: "d", Value: "true", Effect: v1.TaintEffectNoSchedule},
			{Key: "e", Value: "true", Effect: v1.TaintEffectNoSchedule},
			{Key: "f", Value: "true", Effect: v1.TaintEffectNoSchedule},
		},
		"l4": {
			{Key: "l4", Value: "true", Effect: v1.TaintEffectNoSchedule},
		},
	}

	awayNodeTypes := []types.AwayNodeType{
		{
			Priority: 10,
			WellKnownNodeTypes: []types.WellKnownNodeTypeConfig{
				{
					Name: "gpu",
				},
				{
					Name: "l4",
					Conditions: []types.AwayNodeTypeCondition{
						{
							Resource: "cpu", Operator: types.AwayNodeTypeConditionOpGreaterThan, Value: map[string]k8sResource.Quantity{"cpu": k8sResource.MustParse("0")},
						},
					},
				},
			},
		},
	}

	start := time.Now()

	tolerationCount := 0
	for _, job := range jobs {
		tolerations := jobdb.ComputeEffectiveAwayNodeTypes(awayNodeTypes, job.AllResourceRequirements(), wellKnownNodeTypeTolerations)
		tolerationCount += len(tolerations)
	}
	fmt.Println(fmt.Sprintf("toleration count %d", tolerationCount))
	fmt.Println(fmt.Sprintf("elapsed time %s", time.Since(start)))
}

func TestMatchesConditions(t *testing.T) {
	cpu0 := k8sResource.MustParse("0")
	cpu2 := k8sResource.MustParse("2")
	cpu4 := k8sResource.MustParse("4")

	tests := map[string]struct {
		conditions   []types.AwayNodeTypeCondition
		jobResources resource.ComputeResources
		expectMatch  bool
	}{
		"empty conditions always matches": {
			conditions:   nil,
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  true,
		},
		"empty conditions with empty resources": {
			conditions:   []types.AwayNodeTypeCondition{},
			jobResources: map[string]k8sResource.Quantity{},
			expectMatch:  true,
		},
		// GreaterThan
		"gt: job value above threshold matches": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpGreaterThan, Value: map[string]k8sResource.Quantity{"cpu": cpu2}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu4},
			expectMatch:  true,
		},
		"gt: job value equal to threshold does not match": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpGreaterThan, Value: map[string]k8sResource.Quantity{"cpu": cpu2}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  false,
		},
		"gt: job value below threshold does not match": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpGreaterThan, Value: map[string]k8sResource.Quantity{"cpu": cpu4}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  false,
		},
		// LessThan
		"lt: job value below threshold matches": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpLessThan, Value: map[string]k8sResource.Quantity{"cpu": cpu4}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  true,
		},
		"lt: job value equal to threshold does not match": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpLessThan, Value: map[string]k8sResource.Quantity{"cpu": cpu2}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  false,
		},
		"lt: job value above threshold does not match": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpLessThan, Value: map[string]k8sResource.Quantity{"cpu": cpu2}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu4},
			expectMatch:  false,
		},
		// Equal
		"eq: job value equal to threshold matches": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpEqual, Value: map[string]k8sResource.Quantity{"cpu": cpu2}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  true,
		},
		"eq: job value above threshold does not match": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpEqual, Value: map[string]k8sResource.Quantity{"cpu": cpu2}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu4},
			expectMatch:  false,
		},
		"eq: job value below threshold does not match": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "cpu", Operator: types.AwayNodeTypeConditionOpEqual, Value: map[string]k8sResource.Quantity{"cpu": cpu4}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  false,
		},
		// Missing resource treated as zero
		"missing resource treated as zero - gt zero does not match": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "nvidia.com/gpu", Operator: types.AwayNodeTypeConditionOpGreaterThan, Value: map[string]k8sResource.Quantity{"cpu": cpu0}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  false,
		},
		"missing resource treated as zero - eq zero matches": {
			conditions:   []types.AwayNodeTypeCondition{{Resource: "nvidia.com/gpu", Operator: types.AwayNodeTypeConditionOpEqual, Value: map[string]k8sResource.Quantity{"cpu": cpu0}}},
			jobResources: map[string]k8sResource.Quantity{"cpu": cpu2},
			expectMatch:  true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			resources := testfixtures.MakeTestResourceListFactory().FromJobResourceListIgnoreUnknown(tc.jobResources)
			result := jobdb.MatchesConditions(tc.conditions, resources)
			assert.Equal(t, tc.expectMatch, result)
		})
	}
}
