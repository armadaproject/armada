package internaltypes

import "fmt"

type ResourceList struct {
	resources []int64
	factory   *ResourceListFactory
}

func (rl *ResourceList) GetByName(name string) (int64, error) {
	index, ok := rl.factory.nameToIndex[name]
	if !ok {
		return 0, fmt.Errorf("resource type %s not found", name)
	}
	return rl.resources[index], nil
}
