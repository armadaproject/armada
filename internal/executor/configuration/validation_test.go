package configuration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateExecutorConfiguration_NoTaintConfig(t *testing.T) {
	config := ExecutorConfiguration{
		Kubernetes: KubernetesConfiguration{},
	}

	result := ValidateExecutorConfiguration(config)
	assert.NoError(t, result)
}

func TestValidateExecutorConfiguration_TaintConfigWithDisjointValues(t *testing.T) {
	config := ExecutorConfiguration{
		Kubernetes: KubernetesConfiguration{
			ToleratedTaints: []string{"tolerated"},
			IgnoredTaints:   []string{"ignored"},
		},
	}

	result := ValidateExecutorConfiguration(config)
	assert.NoError(t, result)
}

func TestValidateExecutorConfiguration_TaintConfigWithOverlappingValues(t *testing.T) {
	config := ExecutorConfiguration{
		Kubernetes: KubernetesConfiguration{
			ToleratedTaints: []string{"overlapping"},
			IgnoredTaints:   []string{"overlapping"},
		},
	}

	result := ValidateExecutorConfiguration(config)
	assert.Error(t, result)
}
