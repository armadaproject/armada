package internaltypes

import (
	"fmt"
	"math"

	"github.com/pkg/errors"

	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
)

type ResourceListFactory struct {
	nameToIndex map[string]int
	indexToName []string
	scales      []k8sResource.Scale
}

func MakeResourceListFactory(supportedResourceTypes []configuration.ResourceType) (*ResourceListFactory, error) {
	if len(supportedResourceTypes) == 0 {
		return nil, errors.New("no resource types configured")
	}
	indexToName := make([]string, len(supportedResourceTypes))
	nameToIndex := make(map[string]int, len(supportedResourceTypes))
	scales := make([]k8sResource.Scale, len(supportedResourceTypes))
	for i, t := range supportedResourceTypes {
		if _, exists := nameToIndex[t.Name]; exists {
			return nil, fmt.Errorf("duplicate resource type name %q", t.Name)
		}
		nameToIndex[t.Name] = i
		indexToName[i] = t.Name
		scales[i] = resolutionToScale(t.Resolution)
	}
	return &ResourceListFactory{
		indexToName: indexToName,
		nameToIndex: nameToIndex,
		scales:      scales,
	}, nil
}

// Convert resolution to a k8sResource.Scale
// e.g.
// 1     ->  0
// 0.001 -> -3
// 1000  ->  3
func resolutionToScale(resolution k8sResource.Quantity) k8sResource.Scale {
	if resolution.Sign() < 1 {
		return k8sResource.Milli
	}
	return k8sResource.Scale(math.Floor(math.Log10(resolution.AsApproximateFloat64())))
}

func (factory *ResourceListFactory) MakeAllZero() ResourceList {
	result := make([]int64, len(factory.indexToName))
	return ResourceList{resources: result, factory: factory}
}

// Ignore unknown resources, round down.
func (factory *ResourceListFactory) FromNodeProto(resources map[string]k8sResource.Quantity) ResourceList {
	result := make([]int64, len(factory.indexToName))
	for k, v := range resources {
		index, ok := factory.nameToIndex[k]
		if ok {
			result[index] = QuantityToInt64RoundDown(v, factory.scales[index])
		}
	}
	return ResourceList{resources: result, factory: factory}
}

// Ignore unknown resources, round up.
func (factory *ResourceListFactory) FromJobResourceListIgnoreUnknown(resources map[string]k8sResource.Quantity) ResourceList {
	result := make([]int64, len(factory.indexToName))
	for k, v := range resources {
		index, ok := factory.nameToIndex[k]
		if ok {
			result[index] = QuantityToInt64RoundUp(v, factory.scales[index])
		}
	}
	return ResourceList{resources: result, factory: factory}
}

// Fail on unknown resources, round up.
func (factory *ResourceListFactory) FromJobResourceListFailOnUnknown(resources map[string]k8sResource.Quantity) (ResourceList, error) {
	result := make([]int64, len(factory.indexToName))
	for k, v := range resources {
		index, ok := factory.nameToIndex[k]
		if ok {
			result[index] = QuantityToInt64RoundUp(v, factory.scales[index])
		} else {
			return ResourceList{}, fmt.Errorf("resource type %q is not supported (if you want to use it add to scheduling.supportedResourceTypes in the armada scheduler config)", string(k))
		}
	}
	return ResourceList{resources: result, factory: factory}, nil
}

func (factory *ResourceListFactory) SummaryString() string {
	result := ""
	for i, name := range factory.indexToName {
		if i > 0 {
			result += " "
		}
		scale := factory.scales[i]
		resolution := k8sResource.NewScaledQuantity(1, scale)
		maxValue := k8sResource.NewScaledQuantity(math.MaxInt64, scale)
		result += fmt.Sprintf("%s (scale %v, resolution %v, maxValue %f)", name, scale, resolution, maxValue.AsApproximateFloat64())
	}
	return result
}

func (factory *ResourceListFactory) GetScale(resourceTypeName string) (k8sResource.Scale, error) {
	index, ok := factory.nameToIndex[resourceTypeName]
	if !ok {
		return 0, fmt.Errorf("unknown resource type %q", resourceTypeName)
	}
	return factory.scales[index], nil
}
