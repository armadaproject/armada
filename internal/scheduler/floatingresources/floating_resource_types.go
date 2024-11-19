package floatingresources

import (
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

type FloatingResourceTypes struct {
	floatingResourceLimitsByPool map[string]internaltypes.ResourceList
}

func NewFloatingResourceTypes(config []configuration.FloatingResourceConfig, rlFactory *internaltypes.ResourceListFactory) (*FloatingResourceTypes, error) {
	err := validate(config)
	if err != nil {
		return nil, err
	}

	floatingResourceLimitsByPool := map[string]internaltypes.ResourceList{}
	for _, fr := range config {
		for _, poolConfig := range fr.Pools {
			floatingResourceLimitsByPool[poolConfig.Name] = floatingResourceLimitsByPool[poolConfig.Name].Add(
				rlFactory.FromNodeProto(map[string]resource.Quantity{fr.Name: poolConfig.Quantity}),
			)
		}
	}

	return &FloatingResourceTypes{
		floatingResourceLimitsByPool: floatingResourceLimitsByPool,
	}, nil
}

func validate(config []configuration.FloatingResourceConfig) error {
	floatingResourceNamesSeen := map[string]bool{}
	for _, c := range config {
		if _, exists := floatingResourceNamesSeen[c.Name]; exists {
			return fmt.Errorf("duplicate floating resource %s", c.Name)
		}
		floatingResourceNamesSeen[c.Name] = true
	}

	for _, fr := range config {
		poolNamesSeen := map[string]bool{}
		for _, poolConfig := range fr.Pools {
			if _, exists := poolNamesSeen[poolConfig.Name]; exists {
				return fmt.Errorf("floating resource %s has duplicate pool %s", fr.Name, poolConfig.Name)
			}
			poolNamesSeen[poolConfig.Name] = true
		}
	}
	return nil
}

func (frt *FloatingResourceTypes) WithinLimits(poolName string, allocated internaltypes.ResourceList) (bool, string) {
	available := frt.GetTotalAvailableForPool(poolName)
	if available.AllZero() {
		return false, fmt.Sprintf("floating resources not connfigured for pool %s", poolName)
	}

	resourceName, _, _, exceeds := allocated.OfType(internaltypes.Floating).ExceedsAvailable(available)
	if exceeds {
		return false, fmt.Sprintf("not enough floating resource %s in pool %s", resourceName, poolName)
	}

	return true, ""
}

func (frt *FloatingResourceTypes) AllPools() []string {
	result := maps.Keys(frt.floatingResourceLimitsByPool)
	slices.Sort(result)
	return result
}

func (frt *FloatingResourceTypes) GetTotalAvailableForPool(poolName string) internaltypes.ResourceList {
	limits, ok := frt.floatingResourceLimitsByPool[poolName]
	if !ok {
		return internaltypes.ResourceList{}
	}
	return limits
}

func (frt *FloatingResourceTypes) GetTotalAvailableForPoolAsMap(poolName string) map[string]resource.Quantity {
	limits := frt.GetTotalAvailableForPool(poolName)
	result := map[string]resource.Quantity{}
	for _, res := range limits.GetResources() {
		if res.Type != internaltypes.Floating {
			continue
		}
		result[res.Name] = res.Value
	}
	return result
}

func (frt *FloatingResourceTypes) SummaryString() string {
	if len(frt.floatingResourceLimitsByPool) == 0 {
		return "none"
	}
	poolSummaries := []string{}
	for _, poolName := range frt.AllPools() {
		poolSummaries = append(poolSummaries, fmt.Sprintf("%s: (%s)", poolName, frt.floatingResourceLimitsByPool[poolName]))
	}
	return strings.Join(poolSummaries, " ")
}
