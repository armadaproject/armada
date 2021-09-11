package configuration

import (
	"fmt"

	"github.com/G-Research/armada/internal/common/util"
)

func ValidateExecutorConfiguration(config ExecutorConfiguration) error {
	toleratedTaints := util.StringListToSet(config.Kubernetes.ToleratedTaints)
	ignoredTaints := util.StringListToSet(config.Kubernetes.IgnoredTaints)
	for key := range toleratedTaints {
		if _, exists := ignoredTaints[key]; exists {
			return fmt.Errorf("cannot have the same value in both toleratedTaints and ignoredTaints. Value found in both lists \"%s\"", key)
		}
	}
	return nil
}
