package floatingresources

import (
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/maps"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type FloatingResourceTypes struct {
	zeroFloatingResources schedulerobjects.ResourceList
	pools                 map[string]*floatingResourcePool
}

type floatingResourcePool struct {
	totalResources schedulerobjects.ResourceList
}

func NewFloatingResourceTypes(config []configuration.FloatingResourceConfig) (*FloatingResourceTypes, error) {
	zeroFloatingResources := schedulerobjects.ResourceList{Resources: make(map[string]resource.Quantity, len(config))}
	for _, c := range config {
		if _, exists := zeroFloatingResources.Resources[c.Name]; exists {
			return nil, fmt.Errorf("duplicate floating resource %s", c.Name)
		}
		zeroFloatingResources.Resources[c.Name] = resource.Quantity{}
	}

	pools := map[string]*floatingResourcePool{}
	for _, fr := range config {
		for _, poolConfig := range fr.Pools {
			pool, exists := pools[poolConfig.Name]
			if !exists {
				pool = &floatingResourcePool{
					totalResources: zeroFloatingResources.DeepCopy(),
				}
				pools[poolConfig.Name] = pool
			}
			existing := pool.totalResources.Resources[fr.Name]
			if existing.Cmp(resource.Quantity{}) != 0 {
				return nil, fmt.Errorf("duplicate floating resource %s for pool %s", fr.Name, poolConfig.Name)
			}
			pool.totalResources.Resources[fr.Name] = poolConfig.Quantity.DeepCopy()
		}
	}

	return &FloatingResourceTypes{
		zeroFloatingResources: zeroFloatingResources,
		pools:                 pools,
	}, nil
}

func (frt *FloatingResourceTypes) WithinLimits(poolName string, allocated schedulerobjects.ResourceList) (bool, string) {
	pool, exists := frt.pools[poolName]
	if !exists {
		return false, fmt.Sprintf("floating resources not connfigured for pool %s", poolName)
	}
	rl := pool.totalResources.DeepCopy()
	rl.Sub(allocated)
	for resourceName, quantity := range rl.Resources {
		if !frt.isFloatingResource(resourceName) {
			continue
		}
		if quantity.Cmp(resource.Quantity{}) == -1 {
			return false, fmt.Sprintf("not enough floating resource %s in pool %s", resourceName, poolName)
		}
	}
	return true, ""
}

func (frt *FloatingResourceTypes) AllPools() []string {
	result := maps.Keys(frt.pools)
	slices.Sort(result)
	return result
}

func (frt *FloatingResourceTypes) GetTotalAvailableForPool(poolName string) schedulerobjects.ResourceList {
	pool, exists := frt.pools[poolName]
	if !exists {
		return frt.zeroFloatingResources.DeepCopy()
	}
	return pool.totalResources.DeepCopy()
}

func (frt *FloatingResourceTypes) AddTotalAvailableForPool(poolName string, kubernetesResources schedulerobjects.ResourceList) schedulerobjects.ResourceList {
	floatingResources := frt.GetTotalAvailableForPool(poolName) // Note GetTotalAvailableForPool returns a deep copy
	floatingResources.Add(kubernetesResources)
	return floatingResources
}

func (frt *FloatingResourceTypes) SummaryString() string {
	if len(frt.zeroFloatingResources.Resources) == 0 {
		return "none"
	}
	return strings.Join(maps.Keys(frt.zeroFloatingResources.Resources), " ")
}

func (frt *FloatingResourceTypes) isFloatingResource(resourceName string) bool {
	_, exists := frt.zeroFloatingResources.Resources[resourceName]
	return exists
}
